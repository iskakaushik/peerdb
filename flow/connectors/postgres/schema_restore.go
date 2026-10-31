package connpostgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/PeerDB-io/peerdb/flow/generated/protos"
)

type SchemaStep struct {
	Tables   []string
	PreData  bool
	PostData bool
}

func SchemaRestorable(cfg *protos.PostgresConfig) bool {
	return cfg.SshConfig == nil && cfg.AuthType == protos.PostgresAuthType_POSTGRES_PASSWORD
}

func RestoreSchema(ctx context.Context, src, dst *PostgresConnector, step SchemaStep) ([]string, error) {
	dir, err := os.MkdirTemp("", "peerdb-schema-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	archive := filepath.Join(dir, "schema.dump")
	if err := dumpSchema(ctx, src.Config, archive); err != nil {
		return nil, err
	}
	entries, err := readTOC(ctx, archive)
	if err != nil {
		return nil, err
	}

	in := planInput{entries: entries, pre: step.PreData, post: step.PostData}
	if err := inCatalog(ctx, src.conn, func(tx pgx.Tx) error {
		if len(step.Tables) > 0 {
			if in.tables, err = stepTables(ctx, tx, step.Tables); err != nil {
				return err
			}
		}
		in.info, err = loadEntryInfo(ctx, tx, entries)
		return err
	}); err != nil {
		return nil, fmt.Errorf("failed to read source catalog: %w", err)
	}
	if in.exists, err = existingOnDestination(ctx, dst.conn, in); err != nil {
		return nil, err
	}

	plan := planRestore(in)
	if len(plan.restore) > 0 {
		list := filepath.Join(dir, "restore.list")
		var b strings.Builder
		for _, id := range plan.restore {
			b.WriteString(strconv.Itoa(id) + ";\n")
		}
		if err := os.WriteFile(list, []byte(b.String()), 0o600); err != nil {
			return nil, err
		}
		if err := runRestore(ctx, dst.Config, archive, list); err != nil {
			return nil, fmt.Errorf("schema restore failed: %w", err)
		}
	}
	return nil, nil
}

func dumpSchema(ctx context.Context, config *protos.PostgresConfig, archive string) error {
	addr, err := resolvePgAddr(ctx, config)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if out, err := pgCmd(ctx, config, addr, "pg_dump", buildPgDumpArgs(config, addr.host, archive)...).
		CombinedOutput(); err != nil {
		return fmt.Errorf("pg_dump failed: %w\n%s", err, out)
	}
	return nil
}

func readTOC(ctx context.Context, archive string) ([]tocEntry, error) {
	var entries []tocEntry
	for _, section := range []string{"--section=pre-data", "--section=post-data"} {
		out, err := exec.CommandContext(ctx, "pg_restore", "-l", section, archive).Output()
		if err != nil {
			return nil, fmt.Errorf("pg_restore -l failed: %w", err)
		}
		list, err := parseTOCList(bytes.NewReader(out), section == "--section=post-data")
		if err != nil {
			return nil, err
		}
		entries = append(entries, list...)
	}
	cmd := exec.CommandContext(ctx, "pg_restore", "-v", "-f", "-", archive)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	deps, err := parseTOCDeps(stdout)
	_, _ = io.Copy(io.Discard, stdout)
	if werr := cmd.Wait(); err == nil && werr != nil {
		err = fmt.Errorf("pg_restore -v failed: %w", werr)
	}
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].deps = deps[entries[i].id]
	}
	return entries, nil
}

func runRestore(ctx context.Context, config *protos.PostgresConfig, archive, list string) error {
	addr, err := resolvePgAddr(ctx, config)
	if err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	psqlArgs := buildPsqlArgs(config, addr.host)
	restore := exec.CommandContext(ctx, "pg_restore", "--no-owner", "--no-privileges", "-L", list, "-f", "-", archive)
	psql := pgCmd(ctx, config, addr, "psql", psqlArgs...)
	return runPipeline(ctx, restore, psql, "pg_restore", "psql", filterIncompatibleLines)
}
