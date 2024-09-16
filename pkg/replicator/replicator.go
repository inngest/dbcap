package replicator

import (
	"context"

	"github.com/inngest/dbcap/pkg/changeset"
)

// Watermarker is a function which saves a given changeset to local storage.  This allows
// us to continue picking up from where the stream left off if services restart.
type WatermarkSaver func(ctx context.Context, watermark changeset.Watermark) error

// WatermarkLoader is a function which loads a watermark for the given database connection.
//
// If this returns nil, we will use the current DB LSN as the starting point, ie.
// using the latest available stream data.
//
// If this returns an error the CDC replicator will fail early.
type WatermarkLoader func(ctx context.Context) (*changeset.Watermark, error)

type Replicator interface {
	changeset.WatermarkCommitter

	// Pull is a blocking method which pulls changes from an external source,
	// sending all found changesets on the given changeset channel.
	//
	// Pull blocks until the context is cancelled, or until Stop() is called.
	//
	// When Pull terminates from a cancelled context or via calling Stop(), the
	// DB connections to the endpoint will automatically be closed in the
	// underlying implementation.
	Pull(context.Context, chan *changeset.Changeset) error

	// Stop stops pulling and shuts down the replicator.  This is an alternative
	// to cancelling the context passed into Pull.
	Stop()
}

type ConnectionResult interface {
	// Steps indicates the sequential steps necessary to set up a replicator.  For example,
	// the Postgres replicator may return {"credentials", "user_created", "replication_slot_created"...}
	// and so on for each step required to connect.
	Steps() []string

	// Results contains a map for each Step string listed in Steps[], representing whether each
	// step has been complete and any error message for each step.
	Results() map[string]ConnectionStepResult
}

type ConnectionStepResult struct {
	Error    error `json:"error"`
	Complete bool  `json:"complete"`
}

type SystemInitializer interface {
	// PerformInit perform setup for the replicator.
	PerformInit(ctx context.Context) (ConnectionResult, error)

	// CheckInit ensures that the setup for the replicator is complete.
	CheckInit(ctx context.Context) (ConnectionResult, error)
}
