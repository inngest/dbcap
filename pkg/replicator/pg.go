package replicator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	SlotName        = "inngest_cdc"
	PublicationName = "inngest"
)

var (
	ReadTimeout = time.Second * 5
)

type Replicator interface {
	Pull(context.Context) error
}

type PostgresOpts struct {
	Config pgconn.Config
}

func Postgres(ctx context.Context, opts PostgresOpts) (Replicator, error) {
	conn, err := pgconn.ConnectConfig(ctx, &opts.Config)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres host: %w", err)
	}

	return &pg{
		conn:    conn,
		decoder: v1LogicalDecoder{},
	}, nil
}

type pg struct {
	decoder Decoder

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
	//

	identify, err := pglogrepl.IdentifySystem(ctx, p.conn)
	if err != nil {
		return fmt.Errorf("error identifying postgres: %w", err)
	}

	fmt.Printf("%#v\n", identify)

	// TODO: If PG <= 14, don't use these args.
	// TODO: pgcapture doesn't use proto_version 2 - it uses proto 1

	err = pglogrepl.StartReplication(
		ctx,
		p.conn,
		SlotName,
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

		// TODO: Changes
		_ = changes
	}
}

func (p *pg) fetch(ctx context.Context) (Change, error) {
	var (
		change Change
		err    error
	)

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
			return change, nil
		}
		return change, err
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return change, fmt.Errorf("received pg wal error: %#v", errMsg)
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		return change, fmt.Errorf("unknown message type: %T", rawMsg)
	}

	switch msg.Data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		// Keepalives are part of the process with logical replication.
		// We need to XYZ...
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
		if err != nil {
			return change, fmt.Errorf("error parsing replication keepalive: %w", err)
		}

		if pkm.ReplyRequested {
			p.forceNextReport()
		}

		return Change{
			Watermark: Watermark{
				LSN:        pkm.ServerWALEnd,
				ServerTime: pkm.ServerTime,
			},
		}, nil

	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
		if err != nil {
			return change, fmt.Errorf("error parsing replication txn data: %w", err)
		}

		// xld.WALData may be reused, so copy the slice ASAP.
		m, err := p.decoder.Decode(copySlice(xld.WALData))
		if err != nil {
			return change, err
		}
		if m == nil {
			return change, nil
		}

		fmt.Printf("XLOG: %s\n", m)
	}

	return change, nil
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
