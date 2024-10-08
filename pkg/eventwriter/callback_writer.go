package eventwriter

import (
	"context"
	"sync"
	"time"

	"github.com/inngest/dbcap/pkg/changeset"
)

// NewCallbackWriter is a simple writer which calls a callback for a given changeset.
//
// This is primarily used for testing.
func NewCallbackWriter(
	ctx context.Context,
	batchSize int,
	batchTimeout time.Duration,
	onChangeset func(cs []*changeset.Changeset) error,
) EventWriter {
	cs := make(chan *changeset.Changeset, batchSize)
	return &cbWriter{
		onChangeset: onChangeset,
		cs:          cs,
		batchSize:   batchSize,
		wg:          sync.WaitGroup{},
	}
}

type cbWriter struct {
	onChangeset func([]*changeset.Changeset) error

	cs           chan *changeset.Changeset
	batchSize    int
	batchTimeout time.Duration

	wg sync.WaitGroup
}

func (a *cbWriter) Listen(ctx context.Context, committer changeset.WatermarkCommitter) chan *changeset.Changeset {
	if a.batchTimeout < 100*time.Millisecond {
		a.batchTimeout = 100 * time.Millisecond
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()

		i := 0
		buf := make([]*changeset.Changeset, a.batchSize)

		// sendCtx is an additional uncancelled CTX which will be cancelled
		// 5 seconds after the
		for {
			timer := time.NewTimer(a.batchTimeout)

			select {
			case <-ctx.Done():
				if i == 0 {
					// Do nothing.
					return
				}

				// Shutting down.  Send the existing batch.
				if err := a.onChangeset(buf); err == nil {
					committer.Commit(buf[i-1].Watermark)
				}
				return
			case <-timer.C:
				// Force sending current batch
				if i == 0 {
					timer.Reset(a.batchTimeout)
					continue
				}

				// We have events after a timeout - send them.
				if err := a.onChangeset(buf); err == nil {
					// Commit the last LSN.
					committer.Commit(buf[i-1].Watermark)
				}

				// reset the buffer
				buf = make([]*changeset.Changeset, a.batchSize)
				i = 0
			case msg := <-a.cs:
				buf[i] = msg
				i++
				// Send this batch after at least 5 seconds
				timer.Reset(a.batchTimeout)
				if i == a.batchSize {
					// send this batch, as we're full.
					if err := a.onChangeset(buf); err == nil {
						committer.Commit(buf[i-1].Watermark)
					}
					// reset the buffer
					buf = make([]*changeset.Changeset, a.batchSize)
					i = 0
					continue
				}
			}
		}
	}()
	return a.cs
}

func (a *cbWriter) Wait() {
	a.wg.Wait()
}
