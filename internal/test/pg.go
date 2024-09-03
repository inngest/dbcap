package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

type StartPGOpts struct {
	Version                   int
	DisableLogicalReplication bool
	DisableCreateRoles        bool
	DisableCreateSlot         bool
}

func StartPG(t *testing.T, ctx context.Context, opts StartPGOpts) (tc.Container, pgx.ConnConfig) {
	t.Helper()
	args := []tc.ContainerCustomizer{
		pgtc.WithDatabase("db"),
		pgtc.WithUsername("postgres"),
		pgtc.WithPassword("password"),
		pgtc.BasicWaitStrategies(),
	}
	if !opts.DisableLogicalReplication {
		args = append(args, tc.CustomizeRequest(tc.GenericContainerRequest{
			ContainerRequest: tc.ContainerRequest{
				Cmd: []string{"-c", "wal_level=logical"},
			},
		}))
	}
	c, err := pgtc.Run(ctx,
		fmt.Sprintf("docker.io/postgres:%d-alpine", opts.Version),
		args...,
	)

	conn, err := pgconn.Connect(ctx, connString(t, c))
	if err != nil {
		require.NoError(t, err)
	}

	if !opts.DisableCreateRoles {
		// Create the replication slot.
		err := prepareRoles(ctx, conn)
		require.NoError(t, err)
	}
	if !opts.DisableCreateSlot {
		// Create the replication slot.
		err := createReplicationSlot(ctx, conn)
		require.NoError(t, err)
	}

	require.NoError(t, err)
	return c, connOpts(t, c)
}

func prepareRoles(ctx context.Context, c *pgconn.PgConn) error {
	stmt := `
		CREATE USER inngest WITH REPLICATION PASSWORD 'password';
		GRANT USAGE ON SCHEMA public TO inngest;
		GRANT SELECT ON ALL TABLES IN SCHEMA public TO inngest;
		ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO inngest;
		CREATE PUBLICATION inngest FOR ALL TABLES;
	`
	res := c.Exec(ctx, stmt)
	if err := res.Close(); err != nil {
		return err
	}
	return nil
}

func createReplicationSlot(ctx context.Context, c *pgconn.PgConn) error {
	stmt := `
		-- pgoutput logical repl plugin
		SELECT pg_create_logical_replication_slot('inngest_cdc', 'pgoutput');
	`
	res := c.Exec(ctx, stmt)
	if err := res.Close(); err != nil {
		return err
	}
	return nil
}

func connString(t *testing.T, c tc.Container) string {
	p, err := c.MappedPort(context.TODO(), "5432")
	require.NoError(t, err)
	port := strings.ReplaceAll(string(p), "/tcp", "")
	return fmt.Sprintf("postgres://postgres:password@localhost:%s/db", port)
}

func connOpts(t *testing.T, c tc.Container) pgx.ConnConfig {
	cfg, err := pgx.ParseConfig(connString(t, c))
	require.NoError(t, err)
	return *cfg
}
