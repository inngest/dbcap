package pgreplicator

import (
	"context"
	"testing"

	"github.com/inngest/dbcap/internal/test"
	"github.com/stretchr/testify/require"
)

func TestNewInitializer(t *testing.T) {
	t.Parallel()
	versions := []int{10, 11, 12, 13, 14, 15, 16}

	for _, version := range versions {
		v := version // loop capture

		ctx := context.Background()
		c, cfg := test.StartPG(t, ctx, test.StartPGOpts{
			Version:                   v,
			DisableLogicalReplication: true,
			DisableCreateSlot:         true,
		})

		t.Run("It succeeds with the wrong password", func(t *testing.T) {
			_, err := NewInitializer(ctx, InitializerOpts{
				AdminConfig: cfg,
			})
			require.NoError(t, err)
		})

		cfg.Password = "whatever my guy"

		t.Run("It errors with the wrong password", func(t *testing.T) {
			_, err := NewInitializer(ctx, InitializerOpts{
				AdminConfig: cfg,
			})
			require.Error(t, err)
		})

		_ = c.Stop(ctx, nil)
	}
}
