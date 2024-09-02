package replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/inngest/pgcap/pkg/changeset"
	"github.com/inngest/pgcap/pkg/consts/pgconsts"
	"github.com/inngest/pgcap/pkg/decoder"
	"github.com/inngest/pgcap/pkg/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ReadTimeout = time.Second * 5
)

type Replicator interface {
	Pull(context.Context) error
}

type PostgresOpts struct {
	Config pgx.ConnConfig
}

func Postgres(ctx context.Context, opts PostgresOpts) (Replicator, error) {
	cfg, _ := pgconn.ParseConfig(opts.Config.ConnString())
	// Ensure that we add "replication": "database" as a to the replication
	// configuration
	replConfig := cfg.Copy()
	replConfig.RuntimeParams["replication"] = "database"
	// And for schema inspection, ensure this is never set.
	schemaConfig := opts.Config.Copy()
	delete(schemaConfig.RuntimeParams, "replication")

	// Connect using pgconn for replication.  This is a prerequisite, as
	// replication uses different client connection parameters to enable specific
	// postgres functionality.
	conn, err := pgconn.ConnectConfig(ctx, replConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres host for replication: %w", err)
	}

	pgxc, err := pgx.ConnectConfig(ctx, schemaConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres host for schemas: %w", err)
	}

	sl := schema.NewPGXSchemaLoader(pgxc)
	// Refresh all schemas to begin with
	if err := sl.Refresh(); err != nil {
		return nil, err
	}

	return &pg{
		conn:    conn,
		decoder: decoder.NewV1LogicalDecoder(sl),
	}, nil
}

type pg struct {
	decoder decoder.Decoder

	conn *pgconn.PgConn

	// nextReportTime records the time in which we must next report the current
	// LSN to the pg server, advancing the replication slot.
	nextReportTime time.Time
	lsn            uint64
}

func (p *pg) Pull(ctx context.Context) error {

	// TODO:
	// - Is this the first time that we've ever connected to this DB?
	//   - If so, upsert metadata our side
	//
	// 1. Fetch system info, including current LSN
	// 2. Subscribe.
	// 3. Pull events and transform WAL

	identify, err := pglogrepl.IdentifySystem(ctx, p.conn)
	if err != nil {
		return fmt.Errorf("error identifying postgres: %w", err)
	}

	// TODO: If PG <= 14, throw an error.
	err = pglogrepl.StartReplication(
		ctx,
		p.conn,
		pgconsts.SlotName,
		identify.XLogPos,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: p.decoder.ReplicationPluginArgs(),
		},
	)

	if err != nil {
		// TODO: Failure modes - what if the LSN is too far behind?
		return fmt.Errorf("error starting logical replication: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		changes, err := p.fetch(ctx)
		if err != nil {
			fmt.Println("error: ", err)
			return err
		}
		if changes == nil {
			continue
		}

		byt, _ := json.MarshalIndent(changes, "", "  ")
		fmt.Println(string(byt))

		// TODO: Create Inngest event for each change.
	}
}

func (p *pg) fetch(ctx context.Context) (*changeset.Changeset, error) {
	var err error

	defer func() {
		if time.Now().After(p.nextReportTime) {
			if err = p.report(ctx, p.nextReportTime.IsZero()); err != nil {
				// TODO: Log plz
			}
			p.nextReportTime = time.Now().Add(5 * time.Second)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), ReadTimeout)
	rawMsg, err := p.conn.ReceiveMessage(ctx)
	cancel()

	if err != nil {
		if pgconn.Timeout(err) {
			p.forceNextReport()
			// We return nil as we want to keep iterating.
			return nil, nil
		}
		return nil, err
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("received pg wal error: %#v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return nil, fmt.Errorf("unknown message type: %T", rawMsg)
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return nil, fmt.Errorf("error parsing replication keepalive: %w", err)
		}
		if pkm.ReplyRequested {
			p.forceNextReport()
		}

		// TODO: Store LSN and watermark here locally to keep updated w/ progress.

		return nil, nil
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return nil, fmt.Errorf("error parsing replication txn data: %w", err)
		}

		cs := changeset.Changeset{
			Watermark: changeset.Watermark{
				// NOTE: It's expected that WALStart and ServerWALEnd
				// are the same.
				LSN:        xld.WALStart,
				ServerTime: xld.ServerTime,
			},
		}

		// xld.WALData may be reused, so copy the slice ASAP.
		ok, err = p.decoder.Decode(copySlice(xld.WALData), &cs)
		if err != nil {
			return nil, fmt.Errorf("error decoding xlog data: %w", err)
		}
		if !ok {
			return nil, nil
		}
		return &cs, nil
	}

	return nil, nil
}

func (p *pg) forceNextReport() {
	// Updating the next report time to a zero time always reports the LSN,
	// as time.Now() is always after the empty time.
	p.nextReportTime = time.Time{}
}

// report reports the current replication slot's LSN progress to the server.  We can optionally
// force the server to reply with an ack by setting forceReply to true.  This is used when we
// receive timeout errors from PG;  it acts as a ping.
func (p *pg) report(ctx context.Context, forceReply bool) error {
	lsn := p.LSN()
	if lsn == 0 {
		return nil
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx,
		p.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			ReplyRequested:   forceReply,
		},
	)
}

func (p *pg) LSN() (lsn pglogrepl.LSN) {
	return pglogrepl.LSN(atomic.LoadUint64(&p.lsn))
}

// copySlice is a util for copying a slice.
func copySlice(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
