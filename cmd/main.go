package main

import (
	"context"
	"os"

	"github.com/inngest/pgcap/pkg/eventwriter"
	"github.com/inngest/pgcap/pkg/replicator"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	// 1: Connect to postgres
	// 2: Start streaming stuff
	//   3: Store LSN, etc, as watermarks
	// 4: Allow full dumps, in the future.
	cstr := os.Getenv("DATABASE_URL")
	if cstr == "" {
		cstr = "postgres://inngest:password@localhost:5432/db?replication=database"
	}

	config, err := pgx.ParseConfig(cstr)
	if err != nil {
		panic(err)
	}

	r, err := replicator.Postgres(ctx, replicator.PostgresOpts{
		Config: *config,
	})
	if err != nil {
		panic(err)
	}

	writer := eventwriter.NewAPIClientWriter(ctx, nil, 10)
	csChan := writer.Listen(ctx)

	if err := r.Pull(ctx, csChan); err != nil {
		panic(err)
	}

	writer.Wait()
}
