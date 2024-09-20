package eventwriter

import (
	"context"
	"fmt"
	"time"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/inngest/inngestgo"
)

// ChangesetToEvent returns a map containing event data for the given changeset.
func ChangesetToEvent(cs changeset.Changeset) map[string]any {
	var name string

	if cs.Data.Table == "" {
		name = fmt.Sprintf("%s/%s", eventPrefix, cs.Operation.ToEventVerb())
	} else {
		name = fmt.Sprintf("%s/%s.%s", eventPrefix, cs.Data.Table, cs.Operation.ToEventVerb())
	}

	return map[string]any{
		"name": name,
		"data": cs.Data,
		"ts":   cs.Watermark.ServerTime.UnixMilli(),
	}
}

func NewAPIClientWriter(
	ctx context.Context,
	batchSize int,
	client inngestgo.Client,
) EventWriter {
	return NewCallbackWriter(ctx, batchSize, func(cs []*changeset.Changeset) error {
		return send(client, cs)
	})
}

func send(client inngestgo.Client, batch []*changeset.Changeset) error {
	// Always use a new cancel here so that when we quit polling
	// the HTTP request continues.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	evts := make([]any, len(batch))
	for i, cs := range batch {
		if cs == nil {
			evts = evts[0:i]
			break
		}
		evts[i] = ChangesetToEvent(*cs)
	}

	if len(evts) == 0 {
		return nil
	}

	_, err := client.SendMany(ctx, evts)
	return err
}
