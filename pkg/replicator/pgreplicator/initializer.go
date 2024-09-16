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

func NewInitializer(ctx context.Context, opts InitializerOpts) replicator.SystemInitializer[pgsetup.TestConnResult] {
	// TODO: Immediatey

	return initializer[pgsetup.TestConnResult]{opts: opts}
}

type initializer[T pgsetup.TestConnResult] struct {
	opts InitializerOpts
}

// PerformInit perform setup for the replicator.
func (i initializer[T]) PerformInit(ctx context.Context) (pgsetup.TestConnResult, error) {
	return pgsetup.Setup(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}

// CheckInit checks setup for the replicator.
func (i initializer[T]) CheckInit(ctx context.Context) (pgsetup.TestConnResult, error) {
	return pgsetup.Check(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}
