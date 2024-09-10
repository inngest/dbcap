package replicator

import (
	"testing"
	"time"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/stretchr/testify/require"
)

func TestTxnUnwrapper(t *testing.T) {
	t.Run("it unwraps single DML changeset series", func(t *testing.T) {

		cc := make(chan *changeset.Changeset, 10)
		u := txnUnwrapper{cc: cc}

		commitTime := time.Now()

		// This should be non-blocking.
		u.Process(&changeset.Changeset{
			Operation: changeset.OperationBegin,
			Data: changeset.Data{
				TxnLSN:        1,
				TxnCommitTime: &commitTime,
			},
		})
		require.NotNil(t, u.begin)
		require.Nil(t, u.dml)
		require.Equal(t, 0, len(cc), "There should be no events in the channel")

		// Send the insert.
		insert := &changeset.Changeset{
			Operation: changeset.OperationInsert,
			Data: changeset.Data{
				New: changeset.UpdateTuples{
					"id": changeset.ColumnUpdate{
						Encoding: "t",
						Data:     "1",
					},
				},
			},
		}
		u.Process(insert)
		require.NotNil(t, u.dml)
		require.Equal(t, 0, len(cc), "There should be no events in the channel")
		require.EqualValues(t, 1, u.sequence)

		// And the commit should send
		u.Process(&changeset.Changeset{
			Operation: changeset.OperationCommit,
			Data: changeset.Data{
				TxnLSN:        1,
				TxnCommitTime: &commitTime,
			},
		})

		require.Nil(t, u.begin)
		require.Nil(t, u.dml)
		require.EqualValues(t, 0, u.sequence)
		require.Equal(t, 1, len(cc), "There should be one insert event in the channel")
		evt := <-cc
		require.Equal(t, insert, evt)
	})

	t.Run("it doesn't unwrap multi-statement txns", func(t *testing.T) {

		cc := make(chan *changeset.Changeset, 10)
		u := txnUnwrapper{cc: cc}

		commitTime := time.Now()

		dmls := []*changeset.Changeset{
			{
				Operation: changeset.OperationBegin,
				Data: changeset.Data{
					TxnLSN:        1,
					TxnCommitTime: &commitTime,
				},
			},
			{
				Operation: changeset.OperationInsert,
				Data: changeset.Data{
					New: changeset.UpdateTuples{
						"id": changeset.ColumnUpdate{
							Encoding: "t",
							Data:     "1",
						},
					},
				},
			},
			{
				Operation: changeset.OperationInsert,
				Data: changeset.Data{
					New: changeset.UpdateTuples{
						"id": changeset.ColumnUpdate{
							Encoding: "t",
							Data:     "2",
						},
					},
				},
			},
			{
				Operation: changeset.OperationCommit,
				Data: changeset.Data{
					TxnLSN:        1,
					TxnCommitTime: &commitTime,
				},
			},
		}
		begin, a, b, commit := dmls[0], dmls[1], dmls[2], dmls[3]

		// This should be non-blocking.
		u.Process(begin)
		require.NotNil(t, u.begin)
		require.Nil(t, u.dml)
		require.Equal(t, 0, len(cc), "There should be no events in the channel")

		// Send the insert.
		u.Process(a)
		u.Process(b)
		require.Equal(t, u.dml, a)
		require.EqualValues(t, 2, u.sequence)
		require.Equal(t, 3, len(cc), "There should be events in the channel: begin, a, b.")

		// And the commit should send
		u.Process(commit)
		require.Equal(t, 4, len(cc))

		// Commit resets the unwrapper
		require.Nil(t, u.begin)
		require.Nil(t, u.dml)
		require.EqualValues(t, 0, u.sequence)

		close(cc)

		n := 0
		for evt := range cc {
			require.Equal(t, dmls[n], evt)
			n++
		}
	})
}
