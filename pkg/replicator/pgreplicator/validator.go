package pgreplicator

import (
	"context"

	"github.com/inngest/dbcap/pkg/replicator"
	"github.com/inngest/dbcap/pkg/replicator/pgreplicator/pgsetup"
	"github.com/jackc/pgx/v5"
)

type InitializerOpts struct {
	// AdminConfig are admin credentials to verify DB config, eg. replication slots, publications,
	// wal mode, etc.
	AdminConfig pgx.ConnConfig

	// Password is the password to use when creating the new replication user.
	Password string
}

func NewInitializer(ctx context.Context, opts InitializerOpts) replicator.SystemInitializer {
	return initializer{opts: opts}
}

type initializer struct {
	opts InitializerOpts
}

// PerformInit perform setup for the replicator.
func (i initializer) PerformInit(ctx context.Context) (replicator.ConnectionResult, error) {
	return pgsetup.Setup(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}

// CheckInit checks setup for the replicator.
func (i initializer) CheckInit(ctx context.Context) (replicator.ConnectionResult, error) {
	return pgsetup.Check(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}
