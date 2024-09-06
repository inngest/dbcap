package replicator

import (
	"context"

	"github.com/inngest/pgcap/pkg/changeset"
)

type Replicator interface {
	// Pull is a blocking method which pulls changes from an external source,
	// sending all found changesets on the given changeset channel.
	Pull(context.Context, chan *changeset.Changeset) error

	changeset.WatermarkCommitter
}
