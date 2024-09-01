package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/jackc/pgx/v5/pgconn"
)

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

func main() {
	ctx := context.Background()
	c, err := pgconn.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, os.Kill)
	defer cancel()

	for {
		if ctx.Err() != nil {
			return
		}

		id := hash(rand.Int63())

		// Continue to enqueue accounts and users every second.
		res := c.ExecParams(ctx,
			`INSERT INTO accounts
				(name, billing_email, concurrency, event_size_kb, max_batch_size) VALUES
				($1,   $2,            $3,          $4,            $5)`,
			[][]byte{
				[]byte(id),
				[]byte(id + "@example.com"),
				[]byte(strconv.Itoa(rand.Intn(100))),
				[]byte(strconv.Itoa(rand.Intn(100))),
				[]byte(strconv.Itoa(rand.Intn(100))),
			},
			nil, nil, nil,
		)
		if _, err := res.Close(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			panic(err)
		}
		fmt.Println("Account inserted")

		// Maybe update the account.
		// Continue to enqueue accounts and users every second.
		res = c.ExecParams(ctx,
			`UPDATE accounts SET concurrency = $1, event_size_kb = $2 WHERE billing_email = $3`,
			[][]byte{
				[]byte(strconv.Itoa(rand.Intn(100))),
				[]byte(strconv.Itoa(rand.Intn(100))),
				[]byte(id + "@example.com"),
			},
			nil, nil, nil,
		)
		if _, err := res.Close(); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			panic(err)
		}
		fmt.Println("Account updated")

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
