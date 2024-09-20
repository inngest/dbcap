// Package eventwriter creates events from a given replicator, forwarding them to Inngest.
package eventwriter

import (
	"context"

	"github.com/inngest/dbcap/pkg/changeset"
)

const (
	eventPrefix = "pg"
)

type EventWriter interface {
	// Listen returns a channel in which Changesets can be published.  Any published
	// changesets will be broadcast as an event.
	Listen(ctx context.Context, committer changeset.WatermarkCommitter) chan *changeset.Changeset

	// Wait waits for all events to be processed before shutting down.  This must be
	// called after the Listen context has been cancelled.
	Wait()
}
