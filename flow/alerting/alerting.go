package alerting

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/internal"
	"github.com/PeerDB-io/peerdb/flow/otel_metrics"
	"github.com/PeerDB-io/peerdb/flow/shared"
)

// alerting service, no cool name :(
type Alerter struct {
	shared.CatalogPool
	otelManager *otel_metrics.OtelManager
}

type AlertSenderConfig struct {
	Sender          AlertSender
	AlertForMirrors []string
	Id              int64
}

type AlertKeys struct {
	FlowName string
	PeerName string
	SlotName string
}

// doesn't take care of closing pool, needs to be done externally.
func NewAlerter(_ context.Context, catalogPool shared.CatalogPool, otelManager *otel_metrics.OtelManager) *Alerter {
	if catalogPool.Pool == nil {
		panic("catalog pool is nil for Alerter")
	}
	return &Alerter{
		CatalogPool: catalogPool,
		otelManager: otelManager,
	}
}

func (a *Alerter) registerSendersFromPool(ctx context.Context) ([]AlertSenderConfig, error) {
	rows, err := a.CatalogPool.Query(ctx,
		`SELECT id, service_type, service_config, enc_key_id, alert_for_mirrors
		FROM peerdb_stats.alerting_config`)
	if err != nil {
		return nil, fmt.Errorf("failed to read alerter config from catalog: %w", err)
	}

	keys := internal.PeerDBEncKeys(ctx)
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (AlertSenderConfig, error) {
		var alertSenderConfig AlertSenderConfig
		var serviceType ServiceType
		var serviceConfigEnc []byte
		var encKeyId string
		if err := row.Scan(&alertSenderConfig.Id, &serviceType, &serviceConfigEnc, &encKeyId,
			&alertSenderConfig.AlertForMirrors); err != nil {
			return alertSenderConfig, err
		}

		key, err := keys.Get(encKeyId)
		if err != nil {
			return alertSenderConfig, err
		}
		serviceConfig, err := key.Decrypt(serviceConfigEnc)
		if err != nil {
			return alertSenderConfig, err
		}

		switch serviceType {
		case SLACK:
			var slackServiceConfig slackAlertConfig
			if err := json.Unmarshal(serviceConfig, &slackServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}

			alertSenderConfig.Sender = newSlackAlertSender(&slackServiceConfig)
			return alertSenderConfig, nil
		case EMAIL:
			var replyToAddresses []string
			if replyToEnvString := strings.TrimSpace(
				internal.PeerDBAlertingEmailSenderReplyToAddresses()); replyToEnvString != "" {
				replyToAddresses = strings.Split(replyToEnvString, ",")
			}
			emailServiceConfig := EmailAlertSenderConfig{
				sourceEmail:          internal.PeerDBAlertingEmailSenderSourceEmail(),
				configurationSetName: internal.PeerDBAlertingEmailSenderConfigurationSet(),
				replyToAddresses:     replyToAddresses,
			}
			if emailServiceConfig.sourceEmail == "" {
				return alertSenderConfig, fmt.Errorf("missing sourceEmail for Email alerting service")
			}
			if err := json.Unmarshal(serviceConfig, &emailServiceConfig); err != nil {
				return alertSenderConfig, fmt.Errorf("failed to unmarshal %s service config: %w", serviceType, err)
			}
			var region *string
			if envRegion := internal.PeerDBAlertingEmailSenderRegion(); envRegion != "" {
				region = &envRegion
			}

			alertSender, alertSenderErr := NewEmailAlertSenderWithNewClient(ctx, region, &emailServiceConfig)
			if alertSenderErr != nil {
				return AlertSenderConfig{}, fmt.Errorf("failed to initialize email alerter: %w", alertSenderErr)
			}
			alertSenderConfig.Sender = alertSender

			return alertSenderConfig, nil
		default:
			return alertSenderConfig, fmt.Errorf("unknown service type: %s", serviceType)
		}
	})
}

func (a *Alerter) AlertIfSlotLag(ctx context.Context, alertKeys *AlertKeys, slotInfo *protos.SlotInfo) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] ", internal.PeerDBDeploymentUID())
	}

	defaultSlotLagMBAlertThreshold, err := internal.PeerDBSlotLagMBAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get slot lag alert threshold from catalog", slog.Any("error", err))
		return
	}

	thresholdAlertKey := fmt.Sprintf("%s Slot Lag Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	thresholdAlertMessageTemplate := fmt.Sprintf("%sSlot `%s` on peer `%s` has exceeded threshold size of %%dMB, "+
		`currently at %.2fMB!`, deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.LagInMb)

	badWalStatusAlertKey := fmt.Sprintf("%s Bad WAL Status for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	badWalStatusAlertMessage := fmt.Sprintf("%sSlot `%s` on peer `%s` has bad WAL status: `%s`",
		deploymentUIDPrefix, slotInfo.SlotName, alertKeys.PeerName, slotInfo.WalStatus)

	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) > 0 && !slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
			continue
		}
		threshold := cmp.Or(alertSenderConfig.Sender.getSlotLagMBAlertThreshold(), defaultSlotLagMBAlertThreshold)
		thresholdAlertMessage := fmt.Sprintf(thresholdAlertMessageTemplate, threshold)
		if threshold > 0 && slotInfo.LagInMb > float32(threshold) &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, thresholdAlertKey, thresholdAlertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, thresholdAlertKey, thresholdAlertMessage)
		}

		if (slotInfo.WalStatus == "lost" || slotInfo.WalStatus == "unreserved") &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, badWalStatusAlertKey, badWalStatusAlertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, badWalStatusAlertKey, badWalStatusAlertMessage)
		}
	}
}

func (a *Alerter) AlertIfOpenConnections(ctx context.Context, alertKeys *AlertKeys,
	openConnections *protos.GetOpenConnectionsForUserResult,
) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}

	defaultOpenConnectionsThreshold, err := internal.PeerDBOpenConnectionsAlertThreshold(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get open connections alert threshold from catalog", slog.Any("error", err))
		return
	}

	alertKey := fmt.Sprintf("%s Max Open Connections Threshold Exceeded for Peer %s", deploymentUIDPrefix, alertKeys.PeerName)
	alertMessageTemplate := fmt.Sprintf("%sOpen connections from PeerDB user `%s` on peer `%s`"+
		` has exceeded threshold size of %%d connections, currently at %d connections!`,
		deploymentUIDPrefix, openConnections.UserName, alertKeys.PeerName, openConnections.CurrentOpenConnections)

	for _, alertSenderConfig := range alertSenderConfigs {
		if len(alertSenderConfig.AlertForMirrors) > 0 && !slices.Contains(alertSenderConfig.AlertForMirrors, alertKeys.FlowName) {
			continue
		}
		threshold := cmp.Or(alertSenderConfig.Sender.getOpenConnectionsAlertThreshold(), defaultOpenConnectionsThreshold)
		alertMessage := fmt.Sprintf(alertMessageTemplate, threshold)
		if openConnections.CurrentOpenConnections > int64(threshold) &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, alertKey, alertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, alertKey, alertMessage)
		}
	}
}

func (a *Alerter) AlertIfStalled(ctx context.Context, alertKeys *AlertKeys, applyLag time.Duration, slotStall time.Duration) {
	thresholdMinutes, err := internal.PeerDBIntervalSinceLastNormalizeThresholdMinutes(ctx, nil)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to get interval since last normalize threshold from catalog", slog.Any("error", err))
		return
	}
	if thresholdMinutes == 0 || max(applyLag, slotStall) <= time.Duration(thresholdMinutes)*time.Minute {
		return
	}

	deploymentUIDPrefix := ""
	if internal.PeerDBDeploymentUID() != "" {
		deploymentUIDPrefix = fmt.Sprintf("[%s] - ", internal.PeerDBDeploymentUID())
	}
	a.alertMirror(ctx, alertKeys.FlowName,
		fmt.Sprintf("%s Too long since last data normalize for PeerDB mirror %s", deploymentUIDPrefix, alertKeys.FlowName),
		fmt.Sprintf("%sMirror `%s` is not keeping up: oldest unapplied batch synced %s ago, slot `%s` stalled for %s.",
			deploymentUIDPrefix, alertKeys.FlowName, applyLag.Truncate(time.Second), alertKeys.SlotName, slotStall.Truncate(time.Second)))
}

func (a *Alerter) AlertIfFailed(ctx context.Context, flowName string, status protos.FlowStatus) {
	if status == protos.FlowStatus_STATUS_FAILED {
		a.alertMirror(ctx, flowName, "PeerDB mirror failed: "+flowName,
			fmt.Sprintf("Mirror `%s` is in state FAILED and no longer replicates.", flowName))
	}
}

func (a *Alerter) alertMirror(ctx context.Context, flowName string, alertKey string, alertMessage string) {
	alertSenderConfigs, err := a.registerSendersFromPool(ctx)
	if err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to set alert senders", slog.Any("error", err))
		return
	}
	for _, alertSenderConfig := range alertSenderConfigs {
		if (len(alertSenderConfig.AlertForMirrors) == 0 || slices.Contains(alertSenderConfig.AlertForMirrors, flowName)) &&
			a.checkAndAddAlertToCatalog(ctx, alertSenderConfig.Id, alertKey, alertMessage) {
			a.alertToProvider(ctx, alertSenderConfig, alertKey, alertMessage)
		}
	}
}

func (a *Alerter) alertToProvider(ctx context.Context, alertSenderConfig AlertSenderConfig, alertKey string, alertMessage string) {
	if err := alertSenderConfig.Sender.sendAlert(ctx, alertKey, alertMessage); err != nil {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("error", err))
	}
}

// Only raises an alert if another alert with the same key hasn't been raised
// in the past X minutes, where X is configurable and defaults to 15 minutes
// returns true if alert added to catalog, so proceed with processing alerts to slack
func (a *Alerter) checkAndAddAlertToCatalog(ctx context.Context, alertConfigId int64, alertKey string, alertMessage string) bool {
	logger := internal.LoggerFromCtx(ctx)
	dur, err := internal.PeerDBAlertingGapMinutesAsDuration(ctx, nil)
	if err != nil {
		logger.Warn("failed to get alerting gap duration from catalog", slog.Any("error", err))
		return false
	}
	if dur == 0 {
		logger.Warn("Alerting disabled via environment variable, returning")
		return false
	}

	var createdTimestamp time.Time
	if err := a.CatalogPool.QueryRow(ctx,
		`SELECT created_timestamp FROM peerdb_stats.alerts_v1 WHERE alert_key=$1 AND alert_config_id=$2
		 ORDER BY created_timestamp DESC LIMIT 1`,
		alertKey, alertConfigId,
	).Scan(&createdTimestamp); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		internal.LoggerFromCtx(ctx).Warn("failed to send alert", slog.Any("err", err))
		return false
	}

	if time.Since(createdTimestamp) >= dur {
		if _, err := a.CatalogPool.Exec(ctx,
			"INSERT INTO peerdb_stats.alerts_v1(alert_key,alert_message,alert_config_id) VALUES($1,$2,$3)",
			alertKey, alertMessage, alertConfigId,
		); err != nil {
			internal.LoggerFromCtx(ctx).Warn("failed to insert alert", slog.Any("error", err))
			return false
		}
		return true
	}

	logger.Info(fmt.Sprintf("Skipped sending alerts: last alert was sent at %s, which was <=%s ago", createdTimestamp.String(), dur.String()))
	return false
}

type FlowErrorType string

func (f FlowErrorType) String() string {
	return string(f)
}

const (
	FlowErrorTypeInfo  FlowErrorType = "info"
	FlowErrorTypeWarn  FlowErrorType = "warn"
	FlowErrorTypeError FlowErrorType = "error"
)

// recordFlowErrorInternal pushes the error to the errors table and emits a metric.
func (a *Alerter) recordFlowErrorInternal(
	ctx context.Context,
	flowName string,
	errorType FlowErrorType,
	err error,
) {
	logger := internal.LoggerFromCtx(ctx)
	errClass, errInfo := GetErrorClass(ctx, err)
	errMessage := fmt.Sprintf("%+v", err)

	// 1. Log the error internally
	logFn := logger.Error
	if errorType == FlowErrorTypeWarn {
		logFn = logger.Warn
	}
	logFn(err.Error(),
		slog.Any("errorClass", errClass),
		slog.Any("errorInfo", errInfo),
		slog.Any("errorMessage", errMessage),
		slog.String("flowErrorType", errorType.String()),
	)

	if ctx.Err() != nil {
		logger.Error("Skipping flow error handling: " + ctx.Err().Error())
		return
	}

	// 2. Insert log to flow_errors table
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, errMessage, errorType); err != nil {
		logger.Error("failed to insert flow error", slog.Any("error", err))
	}

	if errorType == FlowErrorTypeError && errClass.ErrorAction() != Ignore {
		mirror := a.mirrorOfFlow(ctx, flowName)
		a.alertMirror(ctx, mirror, fmt.Sprintf("Error in PeerDB mirror %s: %s", mirror, errClass),
			fmt.Sprintf("Mirror `%s` failed: %v", mirror, err))
	}

	// 3. Record error metrics
	errorAttributes := []attribute.KeyValue{
		attribute.Stringer(otel_metrics.ErrorClassKey, errClass),
		attribute.Stringer(otel_metrics.ErrorActionKey, errClass.ErrorAction()),
		attribute.Stringer(otel_metrics.ErrorSourceKey, errInfo.Source),
		attribute.String(otel_metrics.ErrorCodeKey, errInfo.Code),
	}
	if len(errInfo.AdditionalAttributes) != 0 {
		for k, v := range errInfo.AdditionalAttributes {
			errorAttributes = append(errorAttributes, attribute.String(k.String(), v))
		}
	}
	errorAttributeSet := metric.WithAttributeSet(attribute.NewSet(errorAttributes...))
	counter := a.otelManager.Metrics.ErrorsEmittedCounter
	gauge := a.otelManager.Metrics.ErrorEmittedGauge
	if errorType == FlowErrorTypeWarn {
		counter = a.otelManager.Metrics.WarningEmittedCounter
		gauge = a.otelManager.Metrics.WarningsEmittedGauge
	}
	counter.Add(ctx, 1, errorAttributeSet)
	gauge.Record(ctx, 1, errorAttributeSet)
}

func (a *Alerter) mirrorOfFlow(ctx context.Context, flowName string) string {
	var mirror string
	if err := a.CatalogPool.QueryRow(ctx, `SELECT name FROM flows WHERE starts_with($1, 'clone_' || name || '_')
		ORDER BY length(name) DESC LIMIT 1`, flowName).Scan(&mirror); err != nil {
		return flowName
	}
	return mirror
}

func (a *Alerter) LogFlowError(ctx context.Context, flowName string, err error) error {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeError, err)
	return err
}

func (a *Alerter) LogFlowWarning(ctx context.Context, flowName string, err error) {
	a.recordFlowErrorInternal(ctx, flowName, FlowErrorTypeWarn, err)
}

func (a *Alerter) LogFlowInfo(ctx context.Context, flowName string, message string) {
	if err := InsertFlowLog(ctx, a.CatalogPool, flowName, message, FlowErrorTypeInfo); err != nil {
		logger := internal.SlogLoggerFromCtx(ctx)
		logger.WarnContext(ctx, "failed to insert flow info", slog.Any("error", err))
	}
}

// InsertFlowLog Historically flow_errors table only stored errors, hence the name.
// It has since evolved to surface all user-facing logs.
func InsertFlowLog(
	ctx context.Context,
	catalogPool shared.CatalogPool,
	flowName string,
	message string,
	errorType FlowErrorType,
) error {
	logger := internal.SlogLoggerFromCtx(ctx)
	logger.InfoContext(ctx, fmt.Sprintf("inserting user-facing flow log: [%s] %s", errorType.String(), message),
		slog.String("flowName", flowName),
		slog.String("severity", errorType.String()),
		slog.String("message", message),
		slog.String("logAudience", "user"),
	)
	_, err := catalogPool.Exec(
		ctx, "INSERT INTO peerdb_stats.flow_errors(flow_name,error_message,error_type) VALUES($1,$2,$3)",
		flowName, message, errorType.String(),
	)
	return err
}
