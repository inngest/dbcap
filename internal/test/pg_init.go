package test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/docker/docker/pkg/ioutils"
	"github.com/inngest/dbcap/pkg/replicator/pgreplicator/pgsetup"
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
	// DisableReplicaIdentityFull disables replica identity full, skipping old tuples
	// in updates.
	DisableReplicaIdentityFull bool
}

func init() {
	tc.Logger = log.New(&ioutils.NopWriter{}, "", 0)
}

func StartPG(t *testing.T, ctx context.Context, opts StartPGOpts) (tc.Container, pgx.ConnConfig) {
	t.Helper()
	args := []tc.ContainerCustomizer{
		tc.WithLogger(nil),
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
	require.NoError(t, err)

	conn, err := pgconn.Connect(ctx, connString(t, c))
	if err != nil {
		require.NoError(t, err)
	}

	connCfg, err := pgx.ParseConfig(connString(t, c))
	require.NoError(t, err, "Failed to parse config")

	sr, err := pgsetup.Setup(ctx, pgsetup.SetupOpts{
		AdminConfig:        *connCfg,
		Password:           "password",
		DisableCreateUser:  opts.DisableCreateRoles,
		DisableCreateRoles: opts.DisableCreateRoles,
		DisableCreateSlot:  opts.DisableCreateSlot,
	})
	require.NoError(t, err, "Setup results: %#v", sr.Results())

	err = createTables(ctx, conn)
	require.NoError(t, err)

	if !opts.DisableReplicaIdentityFull {
		stmt := `
		ALTER TABLE accounts REPLICA IDENTITY FULL;
		ALTER TABLE users REPLICA IDENTITY FULL;`
		dbres := conn.Exec(ctx, stmt)
		err := dbres.Close()
		require.NoError(t, err)
	}

	err = conn.Close(ctx)
	require.NoError(t, err)

	// The CDC user always has an `inngest` username.
	pgxConfig := connOpts(t, c)
	pgxConfig.User = "inngest"
	pgxConfig.Config.User = "inngest"

	require.NoError(t, err)
	return c, pgxConfig
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

func createTables(ctx context.Context, c *pgconn.PgConn) error {
	stmt := `
		CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA public;

		ALTER DATABASE postgres SET lock_timeout=5000;

		CREATE TABLE accounts (
		  id uuid DEFAULT public.gen_random_uuid() PRIMARY KEY NOT NULL,
		  name varchar(255),
		  billing_email varchar(255) NOT NULL,

		  concurrency integer DEFAULT 0 NOT NULL,
		  enabled boolean,
		  metadata JSONB,

		  created_at timestamp without time zone NOT NULL default now(),
		  updated_at timestamp without time zone NOT NULL default now()
		);

		CREATE TABLE users (
		  id uuid DEFAULT public.gen_random_uuid() PRIMARY KEY NOT NULL,
		  account_id uuid NOT NULL CONSTRAINT users_account_id REFERENCES accounts ON DELETE CASCADE,
		  email varchar(255) NOT NULL UNIQUE,
		  name varchar(255),

		  metadata jsonb,

		  created_at timestamp without time zone NOT NULL default now(),
		  updated_at timestamp without time zone NOT NULL default now()
		);
	`
	res := c.Exec(ctx, stmt)
	if err := res.Close(); err != nil {
		return err
	}
	return nil
}
