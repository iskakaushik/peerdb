package connpostgres

import (
	"log/slog"

	"github.com/jackc/pglogrepl"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
	"github.com/PeerDB-io/peerdb/flow/model"
)

func processTruncateMessage[Items model.Items](
	p *PostgresCDCSource, lsn pglogrepl.LSN, msg *pglogrepl.TruncateMessage,
) model.Record[Items] {
	sent := make(map[uint32]bool, len(msg.RelationIDs))
	for _, relID := range msg.RelationIDs {
		sent[relID] = true
	}
	rec := &model.TruncateRecord[Items]{
		BaseRecord:      p.baseRecord(lsn),
		Cascade:         msg.Option&pglogrepl.TruncateOptionCascade != 0,
		RestartIdentity: msg.Option&pglogrepl.TruncateOptionRestartIdentity != 0,
	}
	done := map[uint32]bool{}
	for _, relID := range msg.RelationIDs {
		parent := p.getParentRelIDIfPartitioned(relID)
		srcName, ok := p.srcTableIDNameMapping[parent]
		if !ok || done[parent] {
			continue
		}
		done[parent] = true
		dstName := p.tableNameMapping[srcName].Name
		if schema := p.tableNameSchemaMapping[dstName]; schema == nil || schema.System != protos.TypeSystem_PG {
			continue
		}
		children, partitioned, allChildren := 0, p.idToRelKindMap[parent] == 'p', true
		for child, of := range p.childToParentRelIDMapping {
			if of == parent {
				children++
				allChildren = allChildren && sent[child]
			}
		}
		if sent[parent] && (partitioned || allChildren) || partitioned && children > 0 && allChildren {
			rec.DestinationTableNames = append(rec.DestinationTableNames, dstName)
		} else {
			p.logger.Warn("skipping TRUNCATE of part of a table hierarchy mirrored into one destination table",
				slog.String("table", srcName))
		}
	}
	if len(rec.DestinationTableNames) == 0 {
		return nil
	}
	return rec
}
