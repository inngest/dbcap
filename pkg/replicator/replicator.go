package replicator

import (
	"context"

	"github.com/inngest/pgcap/pkg/changeset"
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
	// Pull is a blocking method which pulls changes from an external source,
	// sending all found changesets on the given changeset channel.
	Pull(context.Context, chan *changeset.Changeset) error

	changeset.WatermarkCommitter
}
