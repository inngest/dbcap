package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const (
	DefaultSeed        = 123
	DefaultAccountUUID = "9b332174-2fc5-5781-8aba-b2500384cc1c"
	DefaultEmail       = "lriai1h2oy1d@example.com"
)

type InsertOpts struct {
	Seed int64

	Max      int
	Batch    int
	Interval time.Duration

	// ConnString, if provided, overrides the
	ConnString *string
}

func DataConn(t *testing.T, cfg pgx.ConnConfig) *pgx.Conn {
	// The data user always has the user 'postgres'
	cfg.User = "postgres"
	c, err := pgx.ConnectConfig(context.Background(), &cfg)
	require.NoError(t, err)
	return c
}

func InsertAccounts(t *testing.T, ctx context.Context, cfg pgx.ConnConfig, opts InsertOpts) {
	t.Helper()

	if opts.Seed == 0 {
		opts.Seed = DefaultSeed
	}

	// The data user always has the user 'postgres'
	cfg.User = "postgres"

	c := DataConn(t, cfg)
	defer c.Close(ctx)

	if opts.Max == 0 {
		opts.Max = 1
	}

	at := time.Unix(1725000000, 0).UTC()

	rand := rand.New(rand.NewSource(opts.Seed))

	for i := 0; i < opts.Max; i++ {
		id := hash(rand.Int63())
		pk := uuid.NewSHA1(uuid.NameSpaceOID, []byte(id))
		// Continue to enqueue accounts and users every second.
		rows, err := c.Query(ctx,
			`INSERT INTO accounts
				(id, name, billing_email, concurrency, enabled, metadata, created_at, updated_at) VALUES
				($1, $2,   $3,            $4,          $5,      $6,       $7,         $8)`,
			pk,
			id,
			id+"@example.com",
			rand.Intn(100),
			true,
			[]byte(`{"ok":true}`), // some rando data
			at,
			at,
		)

		if !errors.Is(err, context.Canceled) {
			require.NoError(t, err)
		}
		rows.Close()
		if opts.Interval > 0 {
			<-time.After(opts.Interval)
		}
	}
}

func hash(in any) string {
	switch v := in.(type) {
	case string:
		ui := xxhash.Sum64String(v)
		return strconv.FormatUint(ui, 36)
	default:
		ui := xxhash.Sum64String(fmt.Sprintf("%v", in))
		return strconv.FormatUint(ui, 36)
	}
}
