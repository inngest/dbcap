package replicator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/inngest/pgcap/internal/test"
	"github.com/stretchr/testify/require"
)

func TestConnectingWithoutLogicalReplicationFails(t *testing.T) {
	ctx := context.Background()
	versions := []int{12, 13, 14, 15, 16}

	for _, v := range versions {
		c, conn := test.StartPG(t, ctx, test.StartPGOpts{
			Version:                   v,
			DisableLogicalReplication: true,
			DisableCreateSlot:         true,
		})

		opts := PostgresOpts{Config: conn}
		r, err := Postgres(ctx, opts)
		require.NoError(t, err)

		err = r.Pull(ctx, nil)
		require.ErrorIs(t, err, ErrLogicalReplicationNotSetUp)

		c.Stop(ctx, nil)
	}
}

func TestConnectingWithoutReplicationSlotFails(t *testing.T) {
	ctx := context.Background()
	versions := []int{12, 13, 14, 15, 16}

	for _, v := range versions {
		c, conn := test.StartPG(t, ctx, test.StartPGOpts{
			Version:           v,
			DisableCreateSlot: true,
		})

		opts := PostgresOpts{Config: conn}
		r, err := Postgres(ctx, opts)
		require.NoError(t, err)

		err = r.Pull(ctx, nil)
		require.ErrorIs(t, err, ErrReplicationSlotNotFound)

		c.Stop(ctx, nil)
	}
}

func TestMultipleConectionsFail(t *testing.T) {
	versions := []int{12, 13, 14, 15, 16}

	for _, v := range versions {
		ctx, cancel := context.WithCancel(context.Background())
		c, conn := test.StartPG(t, ctx, test.StartPGOpts{
			Version: v,
		})

		// The first time we connect things should succeed.
		opts := PostgresOpts{Config: conn}
		r1, err := Postgres(ctx, opts)
		require.NoError(t, err)

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r1.Pull(ctx, nil)
			require.NoError(t, err)
		}()

		r2, err := Postgres(ctx, opts)
		err = r2.Pull(ctx, nil)
		require.ErrorIs(t, err, ErrReplicationAlreadyRunning)

		cancel()
		timeout := time.Second
		c.Stop(ctx, &timeout)
	}
}
