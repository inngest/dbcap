package main

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	ctx := context.Background()
	c, err := pgconn.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	if err := prepareRoles(ctx, c); err != nil {
		panic(err)
	}

	if err := createReplication(ctx, c); err != nil {
		panic(err)
	}

	if err := createTables(ctx, c); err != nil {
		panic(err)
	}
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

func createReplication(ctx context.Context, c *pgconn.PgConn) error {
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

func createTables(ctx context.Context, c *pgconn.PgConn) error {
	stmt := `
		CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA public;

		ALTER DATABASE postgres SET lock_timeout=5000;

		CREATE TABLE accounts (
		  id uuid DEFAULT public.gen_random_uuid() PRIMARY KEY NOT NULL,
		  name varchar(255),
		  billing_email varchar(255) NOT NULL,

		  concurrency integer DEFAULT 0 NOT NULL,
		  event_size_kb integer DEFAULT 512 NOT NULL,
		  max_batch_size integer DEFAULT 0 NOT NULL,

		  created_at timestamp without time zone NOT NULL default now(),
		  updated_at timestamp without time zone NOT NULL default now()
		);
		ALTER TABLE accounts REPLICA IDENTITY FULL;

		CREATE TABLE users (
		  id uuid DEFAULT public.gen_random_uuid() PRIMARY KEY NOT NULL,
		  account_id uuid NOT NULL CONSTRAINT users_account_id REFERENCES accounts ON DELETE CASCADE,
		  email varchar(255) NOT NULL UNIQUE,
		  name varchar(255),

		  metadata jsonb,

		  created_at timestamp without time zone NOT NULL default now(),
		  updated_at timestamp without time zone NOT NULL default now()
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
	`
	res := c.Exec(ctx, stmt)
	if err := res.Close(); err != nil {
		return err
	}
	return nil
}
