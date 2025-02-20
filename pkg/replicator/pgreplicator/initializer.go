package pgreplicator

import (
	"context"

	"github.com/inngest/dbcap/pkg/replicator"
	"github.com/inngest/dbcap/pkg/replicator/pgreplicator/pgsetup"
	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidCredentials         = pgsetup.ErrInvalidCredentials
	ErrCannotCommunicate          = pgsetup.ErrCannotCommunicate
	ErrLogicalReplicationNotSetUp = pgsetup.ErrLogicalReplicationNotSetUp
	ErrReplicationSlotNotFound    = pgsetup.ErrReplicationSlotNotFound
	ErrReplicationAlreadyRunning  = pgsetup.ErrReplicationAlreadyRunning
)

type InitializeResult = pgsetup.TestConnResult

type InitializerOpts struct {
	Teardown bool

	// AdminConfig are admin credentials to verify DB config, eg. replication slots, publications,
	// wal mode, etc.
	AdminConfig pgx.ConnConfig

	// Password is the password to use when creating the new replication user.
	Password string
}

func NewInitializer(ctx context.Context, opts InitializerOpts) (replicator.SystemInitializer[InitializeResult], error) {
	conn, err := pgx.ConnectConfig(ctx, &opts.AdminConfig)
	if err != nil {
		newErr := ErrInvalidCredentials
		newErr.Data = map[string]any{"detail": err.Error()}
		return nil, newErr
	}
	defer conn.Close(ctx)
	if err := conn.Ping(ctx); err != nil {
		newErr := ErrCannotCommunicate
		newErr.Data = map[string]any{"detail": err.Error()}
		return nil, newErr
	}
	return initializer[pgsetup.TestConnResult]{opts: opts}, nil
}

type initializer[T pgsetup.TestConnResult] struct {
	opts InitializerOpts
}

// PerformInit perform setup for the replicator.
func (i initializer[T]) PerformInit(ctx context.Context) (InitializeResult, error) {
	if i.opts.Teardown {
		// Optionally teardown any previous setup.
		err := pgsetup.Teardown(ctx, pgsetup.SetupOpts{
			AdminConfig: i.opts.AdminConfig,
		})
		if err != nil {
			return InitializeResult{}, err
		}
	}

	return pgsetup.Setup(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}

// CheckInit checks setup for the replicator.
func (i initializer[T]) CheckInit(ctx context.Context) (InitializeResult, error) {
	return pgsetup.Check(ctx, pgsetup.SetupOpts{
		AdminConfig: i.opts.AdminConfig,
		Password:    i.opts.Password,
	})
}
