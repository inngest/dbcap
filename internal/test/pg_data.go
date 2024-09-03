package test

import (
	"context"
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

type InsertOpts struct {
	Seed int64

	Max      int
	Batch    int
	Interval time.Duration

	// ConnString, if provided, overrides the
	ConnString *string
}

func InsertAccounts(t *testing.T, ctx context.Context, cfg pgx.ConnConfig, opts InsertOpts) {
	t.Helper()

	// The data user always has the user 'postgres
	cfg.User = "postgres"

	c, err := pgx.ConnectConfig(ctx, &cfg)
	require.NoError(t, err)
	defer c.Close(ctx)

	if opts.Max == 0 {
		opts.Max = 1
	}

	at := time.Unix(1725000000, 0)

	rand.Seed(opts.Seed)

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

		require.NoError(t, err)
		rows.Close()
		if opts.Interval > 0 {
			<-time.After(opts.Interval)
		}
	}
}

func InsertAccountsAndUsers(t *testing.T, ctx context.Context, opts InsertOpts) {
	panic("nah")
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
