package decoder

import (
	"fmt"
	"log/slog"
	"strconv"

	"github.com/inngest/pgcap/pkg/changeset"
	"github.com/inngest/pgcap/pkg/consts/pgconsts"
	"github.com/inngest/pgcap/pkg/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func NewV1LogicalDecoder(s *schema.PGXSchemaLoader) Decoder {
	return v1LogicalDecoder{
		schema:    s,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

type v1LogicalDecoder struct {
	log *slog.Logger

	schema    *schema.PGXSchemaLoader
	relations map[uint32]*pglogrepl.RelationMessage
}

func (v1LogicalDecoder) ReplicationPluginArgs() []string {
	// https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
	//
	// "Proto_version '2'" with "streaming 'true' streams transactions as they're progressing.
	// This is ideal for keeping the WAL buffer on disk low, but prevents Inngest working
	// properly:  we send events as the stream is received, and with streaming we instead need
	// to buffer events and send them when the transaction commits.
	//
	// Version 1 only sends DML entries when the transaction commits, ensuring that any event
	// generated by Inngest is for a committed transaction.
	return []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", pgconsts.PublicationName),
		"messages 'true'",
	}
}

// Decode returns a boolean indicating
func (v v1LogicalDecoder) Decode(in []byte, cs *changeset.Changeset) (bool, error) {
	msgType := pglogrepl.MessageType(in[0])
	switch msgType {
	case pglogrepl.MessageTypeRelation:
		// MessageTypeRelation describes the OIDs for any relation before DML messages are sent.  From the docs:
		//
		//   >  Before the first DML message for a given relation OID, a Relation message will be sent, describing
		//   >  the schema of that relation
		//
		// We store the relation data in-memory within the decoder such that the decoder can read the table names,
		// column names, etc.
		m, err := pglogrepl.Parse(in)
		rm, ok := m.(*pglogrepl.RelationMessage)
		if err != nil || !ok {
			return false, fmt.Errorf("error reading relation message: %w", err)
		}
		v.relations[rm.RelationID] = rm
		return false, nil
	case pglogrepl.MessageTypeBegin, pglogrepl.MessageTypeCommit:
		// These can use the pglogrepl types, as they're good enough.
		m, err := pglogrepl.Parse(in)
		if err != nil {
			return false, fmt.Errorf("error parsing transaction begin/commit: %w", err)
		}
		err = v.mutateChangeset(m, cs)
		return true, err
	case pglogrepl.MessageTypeInsert, pglogrepl.MessageTypeDelete, pglogrepl.MessageTypeUpdate, pglogrepl.MessageTypeTruncate:
		m, err := pglogrepl.Parse(in)
		if err != nil {
			return false, fmt.Errorf("error parsing message: %w", err)
		}
		err = v.mutateChangeset(m, cs)
		return true, err

	default:
		// Unsupported message - log and carry on.
		v.log.Debug("unsupported message type in v1 decoder", "msg_type", msgType.String())
	}

	return false, nil
}

func (v v1LogicalDecoder) mutateChangeset(in pglogrepl.Message, cs *changeset.Changeset) error {
	// XXX: When seeing a begin, annotate the transaction ID, LSN,
	// and commit time to all messages between the begin and commit.

	switch msg := in.(type) {
	case *pglogrepl.BeginMessage:
		cs.Operation = changeset.OperationBegin
		cs.Data.TxnLSN = uint32(msg.FinalLSN)
		cs.Data.TxnCommitTime = msg.CommitTime
		return nil

	case *pglogrepl.CommitMessage:
		cs.Operation = changeset.OperationCommit
		cs.Data.TxnLSN = uint32(msg.CommitLSN)
		cs.Data.TxnCommitTime = msg.CommitTime
		return nil
	case *pglogrepl.TruncateMessage:
		cs.Operation = changeset.OperationTruncate
		cs.Data.TruncatedTables = []string{}
		for _, id := range msg.RelationIDs {
			rel, _ := v.relations[id]
			if rel == nil {
				return fmt.Errorf("couldn't find relation for truncate in relation OID: %d", id)
			}
			cs.Data.TruncatedTables = append(cs.Data.TruncatedTables, rel.RelationName)
		}
		return nil
	case *pglogrepl.InsertMessage:
		cs.Operation = changeset.OperationInsert

		rel, err := v.findRelationByOID(msg.RelationID)
		if err != nil {
			return err
		}

		cs.Data.Table = rel.RelationName

		if msg.Tuple != nil {
			var err error
			cs.Data.New, err = v.parseTuple(msg.Tuple, rel)
			if err != nil {
				v.log.Error("error parsing new tuple",
					"operation", "insert",
					"relation_id", msg.RelationID,
					"error", err,
				)
			}
		} else {
			v.log.Error("insert didn't include new tuple data",
				"operation", "insert",
				"relation_id", msg.RelationID,
				"error", err,
			)
		}

	case *pglogrepl.UpdateMessage:
		var err error
		cs.Operation = changeset.OperationUpdate

		rel, err := v.findRelationByOID(msg.RelationID)
		if err != nil {
			return err
		}

		cs.Data.Table = rel.RelationName

		if msg.OldTuple != nil {
			// Table has REPLICA IDENTITY FULL set;  we can add in old
			// data pre-update.
			cs.Data.Old, err = v.parseTuple(msg.OldTuple, rel)
			if err != nil {
				v.log.Error("error parsing old tuple",
					"operation", "insert",
					"relation_id", msg.RelationID,
					"error", err,
				)
			}
		}
		if msg.NewTuple != nil {
			cs.Data.New, err = v.parseTuple(msg.OldTuple, rel)
			if err != nil {
				v.log.Error("error parsing new tuple",
					"operation", "insert",
					"relation_id", msg.RelationID,
					"error", err,
				)
			}
		} else {
			v.log.Error("update had no new data", "relation_id", msg.RelationID)
		}

	case *pglogrepl.DeleteMessage:
		var err error
		cs.Operation = changeset.OperationDelete

		rel, err := v.findRelationByOID(msg.RelationID)
		if err != nil {
			return err
		}

		cs.Data.Table = rel.RelationName
		if msg.OldTuple != nil {
			// Table has REPLICA IDENTITY FULL set;  we can add in old
			// data pre-update.
			cs.Data.Old, err = v.parseTuple(msg.OldTuple, rel)
			if err != nil {
				v.log.Error("delete had no old tuple data", "relation_id", msg.RelationID)
			}
		}
	}

	return nil
}

func (v v1LogicalDecoder) findRelationByOID(oid uint32) (*pglogrepl.RelationMessage, error) {
	// PG always sends a RelationMessage before DMLs, so we can grab table information
	// here and this should _always_ be present.
	rel, _ := v.relations[oid]
	if rel == nil {
		v.log.Warn("no relation found for oid", "oid", oid)
		return nil, fmt.Errorf("couldn't find relation in decoder: %d", oid)
	}
	return rel, nil
}

func (v1LogicalDecoder) parseTuple(tpl *pglogrepl.TupleData, rm *pglogrepl.RelationMessage) (changeset.UpdateTuples, error) {
	if rm == nil {
		return nil, fmt.Errorf("cannot parse tuple with nil relation message")
	}

	res := changeset.UpdateTuples{}

	for n, colData := range tpl.Columns {
		if n >= len(rm.Columns) {
			return nil, fmt.Errorf(
				"received too many columns in tuple data: OID '%d': relation '%s'",
				rm.RelationID,
				rm.RelationName,
			)
		}

		// Create the raw update, using the defaults.
		update := changeset.ColumnUpdate{
			Encoding: string(colData.DataType),
			Data:     colData.Data,
		}

		col := rm.Columns[n]

		// Depending on the column type, we pre-parse specific values.
		switch col.DataType {
		case pgtype.Float4OID, pgtype.Float8OID:
			if fl, err := strconv.ParseFloat(string(colData.Data), 64); err == nil {
				update.Data = fl
				update.Encoding = "f"
			}
		case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID:
			if i, err := strconv.Atoi(string(colData.Data)); err == nil {
				update.Data = i
				update.Encoding = "i"
			}
		}

		// If this is text data, use the raw text encoding.
		if update.Encoding == "t" {
			update.Data = string(colData.Data)
		}

		res[col.Name] = update
	}

	return res, nil
}
