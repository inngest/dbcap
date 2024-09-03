package replicator

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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

	ErrLogicalReplicationNotSetUp = fmt.Errorf("ERR_PG_001: Your database does not have logical replication configured.  You must set the WAL level to 'logical' to stream events.")

	ErrReplicationSlotNotFound = fmt.Errorf("ERR_PG_002: The replication slot 'inngest_cdc' doesn't exist in your database.  Please create the logical replication slot to stream events.")

	ErrReplicationAlreadyRunning = fmt.Errorf("ERR_PG_901: Replication is already streaming events")
)

type WatermarkCommitter interface {
	// Commit commits the current watermark across the backing datastores - remote
	// and local.  Note that the remote may be committed at specific intervals,
	// so no guarantee of an immediate commit is provided.
	Commit(changeset.Watermark)
}

type Replicator interface {
	// Pull is a blocking method which pulls changes from an external source,
	// sending all found changesets on the given changeset channel.
	Pull(context.Context, chan *changeset.Changeset) error

	WatermarkCommitter
}

// PostgresWatermarker is a function which saves a given postgres changeset to storage.  This allows
// us to continue picking up from where the stream left off if services restart.
type PostgresWatermarkSaver func(ctx context.Context, watermark changeset.Watermark) error

// PostgresWatermarkLoader is a function which loads a postgres watermark for the given database connection.
//
// If this returns nil, we will use the current DB LSN as the starting point, ie. using the latest available
// stream data.
//
// If this returns an error the CDC replicator will fail early.
type PostgresWatermarkLoader func(ctx context.Context) (*changeset.Watermark, error)

type PostgresOpts struct {
	Config pgx.ConnConfig

	WatermarkSaver  PostgresWatermarkSaver
	WatermarkLoader PostgresWatermarkLoader
}

// Postgres returns a new postgres replicator for a single postgres database.
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
	// opts stores the initialization opts, including watermark functs
	opts PostgresOpts

	// conn is the WAL connection
	conn *pgconn.PgConn
	// decoder decodes the binary WAL log
	decoder decoder.Decoder
	// nextReportTime records the time in which we must next report the current
	// LSN to the pg server, advancing the replication slot.
	nextReportTime time.Time
	// lsn is the current LSN
	lsn uint64
	// lsnTime is the server time for the LSN, stored as a uint64 nanosecond epoch.
	lsnTime int64

	log *slog.Logger
}

// Commit commits the current watermark into the postgres replicator.  The postgres replicator
// will transmit the committed LSN to the remote server at the next interval (or on shutdown),
// and will save the committed watermark to local state via the PostgresWatermarkSaver function
// provided during instantiation.
func (p *pg) Commit(wm changeset.Watermark) {
	atomic.StoreUint64(&p.lsn, uint64(wm.LSN))
	atomic.StoreInt64(&p.lsnTime, wm.ServerTime.UnixNano())
}

func (p *pg) Connect(ctx context.Context, lsn pglogrepl.LSN) error {
	identify, err := pglogrepl.IdentifySystem(ctx, p.conn)
	if err != nil {
		return fmt.Errorf("error identifying postgres: %w", err)
	}

	// By default, start at the current LSN, ie. the latest point in the stream.
	startLSN := identify.XLogPos
	if lsn > 0 {
		startLSN = lsn
	}

	err = pglogrepl.StartReplication(
		ctx,
		p.conn,
		pgconsts.SlotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: p.decoder.ReplicationPluginArgs(),
		},
	)
	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "logical decoding requires wal_level") {
			return ErrLogicalReplicationNotSetUp
		}
		if strings.Contains(msg, fmt.Sprintf(`replication slot "%s" does not exist`, pgconsts.SlotName)) {
			return ErrReplicationSlotNotFound
		}
		if strings.Contains(msg, fmt.Sprintf(`replication slot "%s" is active`, pgconsts.SlotName)) {
			return ErrReplicationAlreadyRunning
		}
		return fmt.Errorf("error starting logical replication: %w", err)
	}
	return nil
}

func (p *pg) Pull(ctx context.Context, cc chan *changeset.Changeset) error {
	// By default, start at the current LSN, ie. the latest point in the stream.
	var startLSN pglogrepl.LSN
	if p.opts.WatermarkLoader != nil {
		watermark, err := p.opts.WatermarkLoader(ctx)
		if err != nil {
			return fmt.Errorf("error loading watermark: %w", err)
		}
		startLSN = watermark.LSN
	}

	if err := p.Connect(ctx, pglogrepl.LSN(startLSN)); err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		changes, err := p.fetch(ctx)
		if err != nil {
			return err
		}
		if changes == nil {
			continue
		}

		cc <- changes
	}
}

func (p *pg) fetch(ctx context.Context) (*changeset.Changeset, error) {
	var err error

	defer func() {
		// Note that this reports the committed LSN called via Commit().  If the
		// caller to the postgres replicator never calls Commit() to let us know
		// that the changeset.Changeset has been fully processed, the DB will never
		// receive new updates and the WAL log will grow indefinitely.
		if time.Now().After(p.nextReportTime) {
			if err = p.report(ctx, p.nextReportTime.IsZero()); err != nil {
				p.log.Error("error reporting lsn progress", "error", err)
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

func (p *pg) committedWatermark() (wm changeset.Watermark) {
	lsn, nano := atomic.LoadUint64(&p.lsn), atomic.LoadInt64(&p.lsnTime)
	return changeset.Watermark{
		LSN:        pglogrepl.LSN(lsn),
		ServerTime: time.Unix(0, nano),
	}
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
	err := pglogrepl.SendStandbyStatusUpdate(ctx,
		p.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: lsn,
			ReplyRequested:   forceReply,
		},
	)
	if err != nil {
		return fmt.Errorf("error sending pg status update: %w", err)
	}
	if p.opts.WatermarkSaver != nil {
		// Also commit this watermark to local state.
		return p.opts.WatermarkSaver(ctx, p.committedWatermark())
	}
	return nil
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
