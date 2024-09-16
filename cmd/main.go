package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/inngest/dbcap/pkg/eventwriter"
	"github.com/inngest/dbcap/pkg/replicator/pgreplicator"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx := context.Background()

	cstr := os.Getenv("DATABASE_URL")
	if cstr == "" {
		// Example
		cstr = "postgres://inngest:password@localhost:5432/db?replication=database"
	}

	config, err := pgx.ParseConfig(cstr)
	if err != nil {
		panic(err)
	}

	r, err := pgreplicator.New(ctx, pgreplicator.Opts{
		Config: *config,
	})
	if err != nil {
		panic(err)
	}

	writer := eventwriter.NewCallbackWriter(ctx, func(cs *changeset.Changeset) {
		if cs == nil {
			return
		}
		evt := eventwriter.ChangesetToEvent(*cs)
		byt, _ := json.Marshal(evt)
		fmt.Println(string(byt))
	})
	csChan := writer.Listen(ctx, r)

	if err := r.Pull(ctx, csChan); err != nil {
		panic(err)
	}

	writer.Wait()
}
