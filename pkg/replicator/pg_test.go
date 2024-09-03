package replicator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inngest/pgcap/internal/test"
	"github.com/inngest/pgcap/pkg/changeset"
	"github.com/inngest/pgcap/pkg/eventwriter"
	"github.com/stretchr/testify/require"
)

//
// Simple cases
//

func TestInsert(t *testing.T) {
	t.Parallel()
	versions := []int{12, 13, 14, 15, 16}

	for _, v1 := range versions {
		v := v1 // loop capture
		t.Run(fmt.Sprintf("Insert - Postgres %d", v), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())

			c, conn := test.StartPG(t, ctx, test.StartPGOpts{Version: v})
			opts := PostgresOpts{Config: conn}
			r, err := Postgres(ctx, opts)
			require.NoError(t, err)

			var (
				total   int32
				inserts int32
			)

			cb := eventwriter.NewCallbackWriter(ctx, func(cs *changeset.Changeset) {
				atomic.AddInt32(&total, 1)

				if cs.Operation == changeset.OperationInsert {
					atomic.AddInt32(&inserts, 1)
				}

				switch atomic.LoadInt32(&total) {
				case 1:
					require.EqualValues(t, changeset.OperationBegin, cs.Operation)
				case 2:
					// Insert op
					require.EqualValues(t, changeset.OperationInsert, cs.Operation)
					require.Equal(t, "accounts", cs.Data.Table, "expected account name to be inserted")
					require.Equal(
						t,
						changeset.UpdateTuples{
							"billing_email": {
								Encoding: "t",
								Data:     test.DefaultEmail,
							},
							"concurrency": {
								Encoding: "i",
								Data:     100,
							},
							"created_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
							"enabled": {
								Encoding: "t",
								Data:     "t",
							},
							"id": {
								Encoding: "t",
								Data:     "6db2bd8a-2a2f-52d3-aa79-abb4015d6dbd",
							},
							"metadata": {
								Encoding: "t",
								Data:     "{\"ok\": true}",
							},
							"name": {
								Encoding: "t",
								Data:     "lriai1h2oy1d",
							},
							"updated_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
						},
						cs.Data.New,
					)
				case 3:
					require.EqualValues(t, changeset.OperationCommit, cs.Operation)
				}
			})
			csChan := cb.Listen(ctx, r)

			go func() {
				err := r.Pull(ctx, csChan)
				require.NoError(t, err)
			}()

			test.InsertAccounts(t, ctx, conn, test.InsertOpts{
				Max:      50,
				Interval: 1 * time.Millisecond,
			})

			<-time.After(1 * time.Second)
			require.EqualValues(t, 150, total)
			require.EqualValues(t, 50, inserts)

			cancel()

			c.Stop(ctx, nil)
		})
	}
}

func TestUpdateMany_ReplicaIdentityFull(t *testing.T) {
	t.Parallel()
	versions := []int{12, 13, 14, 15, 16}

	for _, v1 := range versions {
		v := v1 // loop capture
		t.Run(fmt.Sprintf("Insert - Postgres %d", v), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())

			c, connCfg := test.StartPG(t, ctx, test.StartPGOpts{Version: v})
			opts := PostgresOpts{Config: connCfg}

			//
			// Insert accounts before starting replication watching.  This lets us
			// ensure we're only testing against updates
			//
			test.InsertAccounts(t, ctx, connCfg, test.InsertOpts{
				Max:      50,
				Interval: 1 * time.Millisecond,
			})

			r, err := Postgres(ctx, opts)
			require.NoError(t, err)

			var (
				total   int32
				updates int32
			)

			cb := eventwriter.NewCallbackWriter(ctx, func(cs *changeset.Changeset) {
				atomic.AddInt32(&total, 1)

				if cs.Operation == changeset.OperationUpdate {
					atomic.AddInt32(&updates, 1)
				}

				switch atomic.LoadInt32(&total) {
				case 1:
					require.EqualValues(t, changeset.OperationBegin, cs.Operation)
				case 2:
					// Insert op
					require.EqualValues(t, changeset.OperationUpdate, cs.Operation)
					require.Equal(t, "accounts", cs.Data.Table, "expected account name to be inserted")
					require.Equal(
						t,
						changeset.UpdateTuples{
							"billing_email": {
								Encoding: "t",
								Data:     test.DefaultEmail,
							},
							"concurrency": {
								Encoding: "i",
								Data:     49,
							},
							"created_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
							"enabled": {
								Encoding: "t",
								Data:     "t",
							},
							"id": {
								Encoding: "t",
								Data:     "6db2bd8a-2a2f-52d3-aa79-abb4015d6dbd",
							},
							"metadata": {
								Encoding: "t",
								Data:     "{\"ok\": true}",
							},
							"name": {
								Encoding: "t",
								Data:     "lriai1h2oy1d",
							},
							"updated_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
						},
						cs.Data.Old,
						"Old data in update isn't correct",
					)
					require.Equal(
						t,
						changeset.UpdateTuples{
							"billing_email": {
								Encoding: "t",
								Data:     "test@example.com",
							},
							"concurrency": {
								Encoding: "i",
								Data:     49,
							},
							"created_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
							"enabled": {
								Encoding: "t",
								Data:     "t",
							},
							"id": {
								Encoding: "t",
								Data:     "6db2bd8a-2a2f-52d3-aa79-abb4015d6dbd",
							},
							"metadata": {
								Encoding: "t",
								Data:     "{\"ok\": true}",
							},
							"name": {
								Encoding: "t",
								Data:     "lriai1h2oy1d",
							},
							"updated_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
						},
						cs.Data.New,
						"New data in update isn't correct",
					)
				case 52:
					require.EqualValues(t, changeset.OperationCommit, cs.Operation)
				}
			})
			csChan := cb.Listen(ctx, r)

			go func() {
				err := r.Pull(ctx, csChan)
				require.NoError(t, err)
			}()

			_, err = test.DataConn(t, connCfg).Exec(ctx, "UPDATE accounts SET billing_email = 'test@example.com'")
			require.NoError(t, err)

			<-time.After(1 * time.Second)

			require.EqualValues(t, 50, updates)

			cancel()

			c.Stop(ctx, nil)
		})
	}
}

func TestUpdateMany_DisableReplicaIdentityFull(t *testing.T) {
	t.Parallel()
	versions := []int{12, 13, 14, 15, 16}

	for _, v1 := range versions {
		v := v1 // loop capture
		t.Run(fmt.Sprintf("Insert - Postgres %d", v), func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())

			c, connCfg := test.StartPG(t, ctx, test.StartPGOpts{
				Version:                    v,
				DisableReplicaIdentityFull: true,
			})
			opts := PostgresOpts{Config: connCfg}

			//
			// Insert accounts before starting replication watching.  This lets us
			// ensure we're only testing against updates
			//
			test.InsertAccounts(t, ctx, connCfg, test.InsertOpts{
				Max:      50,
				Interval: 1 * time.Millisecond,
			})

			r, err := Postgres(ctx, opts)
			require.NoError(t, err)

			var (
				total   int32
				updates int32
			)

			cb := eventwriter.NewCallbackWriter(ctx, func(cs *changeset.Changeset) {
				atomic.AddInt32(&total, 1)

				if cs.Operation == changeset.OperationUpdate {
					atomic.AddInt32(&updates, 1)
				}

				switch atomic.LoadInt32(&total) {
				case 1:
					require.EqualValues(t, changeset.OperationBegin, cs.Operation)
				case 2:
					require.EqualValues(t, changeset.OperationUpdate, cs.Operation)
					require.Equal(t, "accounts", cs.Data.Table, "expected account name to be inserted")
					// No old data as replica identity isn't set.
					require.Equal(
						t,
						changeset.UpdateTuples(nil),
						cs.Data.Old,
						"Old data in update isn't correct",
					)
					require.Equal(
						t,
						changeset.UpdateTuples{
							"billing_email": {
								Encoding: "t",
								Data:     "test@example.com",
							},
							"concurrency": {
								Encoding: "i",
								Data:     49,
							},
							"created_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
							"enabled": {
								Encoding: "t",
								Data:     "t",
							},
							"id": {
								Encoding: "t",
								Data:     "6db2bd8a-2a2f-52d3-aa79-abb4015d6dbd",
							},
							"metadata": {
								Encoding: "t",
								Data:     "{\"ok\": true}",
							},
							"name": {
								Encoding: "t",
								Data:     "lriai1h2oy1d",
							},
							"updated_at": {
								Encoding: "t",
								Data:     "2024-08-30 07:40:00",
							},
						},
						cs.Data.New,
						"New data in update isn't correct",
					)
				case 52:
					require.EqualValues(t, changeset.OperationCommit, cs.Operation)
				}
			})
			csChan := cb.Listen(ctx, r)

			go func() {
				err := r.Pull(ctx, csChan)
				require.NoError(t, err)
			}()

			_, err = test.DataConn(t, connCfg).Exec(ctx, "UPDATE accounts SET billing_email = 'test@example.com'")
			require.NoError(t, err)

			<-time.After(1 * time.Second)

			require.EqualValues(t, 50, updates)

			cancel()

			c.Stop(ctx, nil)
		})
	}
}

//
// Failure cases
//

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

		<-time.After(50 * time.Millisecond)

		r2, err := Postgres(ctx, opts)
		err = r2.Pull(ctx, nil)
		require.ErrorIs(t, err, ErrReplicationAlreadyRunning)

		cancel()

		<-time.After(time.Second)

		timeout := time.Second
		c.Stop(ctx, &timeout)
	}
}
