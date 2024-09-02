package changeset

import (
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
)

type Operation string

const (
	OperationBegin    = "BEGIN"
	OperationCommit   = "COMMIT"
	OperationInsert   = "INSERT"
	OperationUpdate   = "UPDATE"
	OperationDelete   = "DELETE"
	OperationTruncate = "TRUNCATE"
)

func (o Operation) ToEventVerb() string {
	switch o {
	case OperationBegin:
		return "tx-began"
	case OperationCommit:
		return "tx-committed"
	case OperationInsert:
		return "inserted"
	case OperationUpdate:
		return "updated"
	case OperationDelete:
		return "deleted"
	case OperationTruncate:
		return "truncated"
	default:
		return strings.ToLower(string(o))
	}
}

type Changeset struct {
	// Watermark represents the internal watermark for this changeset op.
	Watermark Watermark `json:"watermark"`

	// Operation represents the operation type for this event.
	Operation Operation `json:"operation"`

	// Data represents the actual data for the operation
	Data Data `json:"data"`
}

type Watermark struct {
	LSN        pglogrepl.LSN
	ServerTime time.Time
}

type Data struct {
	// TransactionLSN represents the last LSN of a transaction.
	// No
	TxnLSN        uint32    `json:"txn_id,omitempty"`
	TxnCommitTime time.Time `json:"txn_commit_time,omitempty"`

	Table string       `json:"table,omitempty"`
	Old   UpdateTuples `json:"old,omitempty"`
	New   UpdateTuples `json:"new,omitempty"`

	// TruncatedTables represents the table names truncated in a Truncate operation
	TruncatedTables []string `json:"truncated_tables,omitempty"`
}

type UpdateTuples map[string]ColumnUpdate

type ColumnUpdate struct {
	// Encoding represents the encoding of the data in Data.  This may be one of:
	//
	// - "n", representing null data.
	// - "u", representing the unchagned TOAST data within postgres
	// - "t", representing text-encoded data
	// - "b", representing binary data.
	// - "i", representing an integer
	// - "f", representing a float
	Encoding string `json:"encoding"`
	// Data is the value of the column.  If this is binary data, this data will be
	// base64 encoded.
	Data any `json:"data"`
}
