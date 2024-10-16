package pgreplicator

import (
	"sync/atomic"

	"github.com/inngest/dbcap/pkg/changeset"
)

// txnUnwrapper unwraps single transaction BEGIN/DML/COMMIT events into a single Changeset.
//
// Postgres batches every individual insert, update, etc. within a BEGIN/COMMIT message.
// This is great for replication.  However, for Inngest events, we don't want superflous begin
// or commit messages as events.
//
// The txn unwrapper acts as a buffer for the begin and first DML message.  Once received, we
// check the next chagneset;  if the changeset is a COMMIT we discard the BEGIN and only serve
// the DML.
type txnUnwrapper struct {
	cc chan *changeset.Changeset

	sequence int32

	begin *changeset.Changeset
	dml   *changeset.Changeset
}

func (t *txnUnwrapper) Process(cs *changeset.Changeset) {
	if cs == nil {
		return
	}

	switch cs.Operation {
	case changeset.OperationHeartbeat:
		// The unwrapper should never receive heartbeats as the replicator should
		// handle them and short circuit.  However, always transmit them immediately
		// for safety in code in case someone changes something in the future.
		t.cc <- cs
		return
	case changeset.OperationBegin:
		t.begin = cs
	case changeset.OperationCommit:
		if atomic.LoadInt32(&t.sequence) == 1 {
			// Only broadcast the DML
			t.cc <- t.dml
		} else {
			// Broadcast the end of the TXN
			t.cc <- cs
		}
		t.Reset()
	default:
		next := atomic.AddInt32(&t.sequence, 1)
		switch next {
		case 1:
			// Always add the commit time to the operation.  DML updates don't have this.
			if cs.Data.TxnCommitTime == nil {
				cs.Data.TxnCommitTime = t.begin.Data.TxnCommitTime
				cs.Data.TxnLSN = t.begin.Data.TxnLSN
			}

			// This is the first DML statement.  Cache and potentially unwrap
			t.dml = cs
		case 2:
			// Always add the commit time to the operation.  DML updates don't have this.
			if cs.Data.TxnCommitTime == nil {
				cs.Data.TxnCommitTime = t.begin.Data.TxnCommitTime
				cs.Data.TxnLSN = t.begin.Data.TxnLSN
			}

			// This is the second statement in a multi-statement DML.
			// Broadcast the begin, the first DML statement, and all new operations.
			t.cc <- t.begin
			t.cc <- t.dml
			t.cc <- cs
		default:
			// Broadcast the next DML statement in the TXN
			t.cc <- cs
		}
	}
}

func (t *txnUnwrapper) Reset() {
	t.begin = nil
	t.dml = nil
	atomic.StoreInt32(&t.sequence, 0)
}
