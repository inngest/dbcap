package pgsetup

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	tc "github.com/testcontainers/testcontainers-go"
	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestTeardown(t *testing.T) {
	t.Parallel()
	versions := []int{10, 11, 12, 13, 14, 15, 16}

	for _, version := range versions {
		v := version // loop capture

		t.Run("it works if not set up", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c, cfg := startPG(t, ctx, v)
			err := Teardown(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)
			_ = c.Stop(ctx, nil)
		})

		// set up with just a user.
		t.Run("it is successful without publication or repl slots", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c, cfg := startPG(t, ctx, v)

			_, err := Setup(ctx, SetupOpts{
				AdminConfig:              cfg,
				Password:                 "foo",
				DisableCreateSlot:        true,
				DisableCreatePublication: true,
			})
			require.NoError(t, err)

			err = Teardown(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)
			_ = c.Stop(ctx, nil)
		})

		// set up with just a user.
		t.Run("it is successful without publication", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c, cfg := startPG(t, ctx, v)

			_, err := Setup(ctx, SetupOpts{
				AdminConfig:       cfg,
				Password:          "foo",
				DisableCreateSlot: true,
			})
			require.NoError(t, err)

			err = Teardown(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)
			_ = c.Stop(ctx, nil)
		})

		t.Run("it is successful with full setup", func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			c, cfg := startPG(t, ctx, v)

			_, err := Setup(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)

			err = Teardown(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)

			// Setting up again works.
			_, err = Setup(ctx, SetupOpts{
				AdminConfig: cfg,
				Password:    "foo",
			})
			require.NoError(t, err)

			_ = c.Stop(ctx, nil)
		})
	}
}

func startPG(t *testing.T, ctx context.Context, v int) (tc.Container, pgx.ConnConfig) {
	t.Helper()
	args := []tc.ContainerCustomizer{
		tc.WithLogger(nil),
		pgtc.WithDatabase("db"),
		pgtc.WithUsername("postgres"),
		pgtc.WithPassword("password"),
		pgtc.BasicWaitStrategies(),
		tc.CustomizeRequest(tc.GenericContainerRequest{
			ContainerRequest: tc.ContainerRequest{
				Cmd: []string{"-c", "wal_level=logical"},
			},
		}),
	}

	c, err := pgtc.Run(ctx,
		fmt.Sprintf("docker.io/postgres:%d-alpine", v),
		args...,
	)
	require.NoError(t, err)

	pgxConfig := connOpts(t, c)
	return c, pgxConfig
}

func connOpts(t *testing.T, c tc.Container) pgx.ConnConfig {
	cfg, err := pgx.ParseConfig(connString(t, c))
	require.NoError(t, err)
	return *cfg
}

func connString(t *testing.T, c tc.Container) string {
	p, err := c.MappedPort(context.TODO(), "5432")
	require.NoError(t, err)
	port := strings.ReplaceAll(string(p), "/tcp", "")
	return fmt.Sprintf("postgres://postgres:password@localhost:%s/db", port)
}
