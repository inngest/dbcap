package pgreplicator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/inngest/dbcap/pkg/changeset"
	"github.com/inngest/dbcap/pkg/consts/pgconsts"
	"github.com/inngest/dbcap/pkg/decoder"
	"github.com/inngest/dbcap/pkg/replicator"
	"github.com/inngest/dbcap/pkg/schema"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	ReadTimeout          = time.Second * 5
	CommitInterval       = time.Second * 5
	DefaultHeartbeatTime = time.Minute
)

// PostgresReplicator is a Replicator with added postgres functionality.
type PostgresReplicator interface {
	replicator.Replicator

	// ReplicationSlot returns the replication slot data or an error.
	//
	// If ReplicationSlot does not return an error, it's safe to assume that the
	// postgres database is correctly configured.
	ReplicationSlot(ctx context.Context) (ReplicationSlot, error)

	// ServerLSN reports the server LSN
	ServerLSN(ctx context.Context) (pglogrepl.LSN, error)

	// Close closes all DB conns
	Close(ctx context.Context) error
}

type Opts struct {
	Config pgx.ConnConfig
	// WatermarkSaver saves the current watermark to local storage.  This should be paired with a
	// WatermarkLoader to load offsets when the replicator restarts.
	WatermarkSaver replicator.WatermarkSaver
	// WatermarkLoader, if specified, loads watermarks for the given connection to start replication
	// from a given offset.  If this isn't specified, replication will start from the latest point in
	// the Postgres server's WAL.
	WatermarkLoader replicator.WatermarkLoader
	// Log, if specified, is the stdlib logger used to log debug and warning messages during
	// replication.
	Log *slog.Logger
}

// New returns a new postgres replicator for a single postgres database.
func New(ctx context.Context, opts Opts) (PostgresReplicator, error) {
	if opts.Log == nil {
		opts.Log = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})).With("host", opts.Config.Host)
	}

	cfg := opts.Config

	// Ensure that we add "replication": "database" as a to the replication
	// configuration
	replConfig := cfg.Copy()
	replConfig.RuntimeParams["replication"] = "database"
	// And for schema inspection, ensure this is never set.

	// Connect using pgconn for replication.  This is a prerequisite, as
	// replication uses different client connection parameters to enable specific
	// postgres functionality.
	replConn, err := pgx.ConnectConfig(ctx, replConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to postgres host for replication: %w", err)
	}

	p := &pg{
		opts:          opts,
		conn:          replConn,
		queryLock:     &sync.Mutex{},
		log:           opts.Log,
		heartbeatTime: DefaultHeartbeatTime,
	}

	if err := p.createQueryConn(ctx); err != nil {
		return nil, err
	}

	// Query for current postgres version.
	row := p.queryConn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int / 10000;")
	if err := row.Scan(&p.version); err != nil {
		opts.Log.Warn("error querying for postgres version", "error", err)
	}

	sl := schema.NewPGXSchemaLoader(p.queryConn)
	// Refresh all schemas to begin with
	if err := sl.Refresh(); err != nil {
		return nil, err
	}

	p.decoder = decoder.NewV1LogicalDecoder(sl, opts.Log, p.version >= pgconsts.MessagesVersion)

	return p, nil
}

type pg struct {
	// opts stores the initialization opts, including watermark functs
	opts Opts
	// conn is the WAL streaming connection.  Once replication starts, this
	// conn cannot be used for any queries.
	conn *pgx.Conn

	// queryCon is a conn for querying data.
	queryConn *pgx.Conn

	// queryLock is used to lock pgx.Conn, as it's a single connection which cannot be used
	// in parallel.
	queryLock *sync.Mutex

	// decoder decodes the binary WAL log
	decoder decoder.Decoder
	// nextReportTime records the time in which we must next report the current
	// LSN to the pg server, advancing the replication slot.
	nextReportTime time.Time
	// lsn is the current LSN
	lsn uint64
	// lsnTime is the server time for the LSN, stored as a uint64 nanosecond epoch.
	lsnTime int64
	// log is a stdlib logger for reporting debug and warn logs.
	log *slog.Logger

	version       int
	heartbeatTime time.Duration

	stopped int32
}

func (p *pg) createQueryConn(ctx context.Context) error {
	p.queryLock.Lock()
	defer p.queryLock.Unlock()

	if p.queryConn != nil && !p.queryConn.IsClosed() {
		_ = p.queryConn.Close(ctx)
	}

	// And for schema inspection, ensure this is never set.
	schemaConfig := p.opts.Config.Copy()
	delete(schemaConfig.RuntimeParams, "replication")

	// Connect using pgconn for replication.  This is a prerequisite, as
	pgxc, err := pgx.ConnectConfig(ctx, schemaConfig)
	if err != nil {
		return fmt.Errorf("error connecting to postgres host for schemas: %w", err)
	}
	p.queryConn = pgxc
	return nil

}

func (p *pg) Stop() {
	atomic.StoreInt32(&p.stopped, 1)
	_ = p.Close(context.Background())
}

func (p *pg) Close(ctx context.Context) error {
	atomic.StoreInt32(&p.stopped, 1)
	_ = p.conn.Close(ctx)
	_ = p.queryConn.Close(ctx)
	return nil
}

func (p *pg) ReplicationSlot(ctx context.Context) (ReplicationSlot, error) {

	mode, err := p.walMode(ctx)
	if err != nil {
		return ReplicationSlot{}, err
	}

	if mode != "logical" {
		return ReplicationSlot{}, ErrLogicalReplicationNotSetUp
	}

	// Lock when querying repl slot data.
	p.queryLock.Lock()
	defer p.queryLock.Unlock()

	return ReplicationSlotData(ctx, p.queryConn)
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
	// By default, start at the current LSN, ie. the latest point in the stream.
	startLSN, err := p.ServerLSN(ctx)
	if err != nil {
		return fmt.Errorf("error fetching server LSN: %w", err)
	}

	// And if we've got an LSN provided, use that.
	if lsn > 0 {
		startLSN = lsn
	}

	err = pglogrepl.StartReplication(
		ctx,
		p.conn.PgConn(),
		pgconsts.SlotName,
		startLSN,
		pglogrepl.StartReplicationOptions{
			Mode:       pglogrepl.LogicalReplication,
			PluginArgs: p.decoder.ReplicationPluginArgs(),
		},
	)
	if err != nil {
		_ = p.Close(ctx)
		if converted, newErr := standardizeErr(err); converted {
			return newErr
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
		if watermark != nil && watermark.LSN > 0 {
			startLSN = watermark.LSN
		}
	}

	if err := p.Connect(ctx, pglogrepl.LSN(startLSN)); err != nil {
		return err
	}

	p.log.Debug("connected to replication slot")

	// Postgres batches every individual insert, update, etc. within a BEGIN/COMMIT message.
	// This is great for replication.  However, for Inngest events, we don't want superflous begin
	// or commit messages as events.
	//
	// The txn unwrapper acts as a buffer for the begin and first DML message.  Once received, we
	// check the next chagneset;  if the changeset is a COMMIT we discard the BEGIN and only serve
	// the DML.
	unwrapper := &txnUnwrapper{cc: cc}

	go func() {
		if p.version < pgconsts.MessagesVersion {
			// doesn't support wal messages;  ignore.
			p.log.Debug("heartbeat not supported", "pg_version", p.version)
			return
		}

		// Send a hearbeat immediately.
		if err := p.heartbeat(ctx); err != nil {
			p.log.Warn("unable to emit immediate heartbeat", "error", err)
		}

		t := time.NewTicker(p.heartbeatTime)
		doneCheck := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-doneCheck.C:
				// Check for the stopped signal internally every second. This lets us log
				// the stopped message relatively close to the stop signal occurring.
				if atomic.LoadInt32(&p.stopped) == 1 {
					p.log.Debug("stopping heartbeat", "ctx_err", ctx.Err(), "stopped", atomic.LoadInt32(&p.stopped))
					return
				}
				continue
			case <-t.C:
				if p.queryConn.IsClosed() {
					if err := p.createQueryConn(ctx); err != nil {
						p.log.Error("error reconnecting for heartbeat", "error", err)
						return
					}
				}

				// Send a hearbeat every minute
				err := p.heartbeat(ctx)
				if err != nil {
					p.log.Warn("unable to emit heartbeat", "error", err)
					continue
				}

				p.log.Debug("sent heartbeat", "error", err)
			}
		}
	}()

	for {
		if ctx.Err() != nil || atomic.LoadInt32(&p.stopped) == 1 || p.conn.IsClosed() {
			// Always call Close automatically.
			p.log.Debug("stopping cdc connection", "conn_closed", p.conn.IsClosed())
			p.Close(ctx)
			return nil
		}

		changes, err := p.fetch(ctx)
		if err != nil {
			p.log.Warn("error pulling messages", "error", err)
			return err
		}
		if changes == nil {
			p.log.Debug("no messages pulled")
			continue
		}

		if changes.Operation == changeset.OperationHeartbeat {
			p.log.Debug("heartbeat pulled")
			p.Commit(changes.Watermark)
			if err := p.forceNextReport(ctx); err != nil {
				p.log.Warn("unable to report lsn on heartbeat", "error", err, "host", p.opts.Config.Host)
			}
			continue
		}

		p.log.Debug("message pulled", "op", changes.Operation)

		unwrapper.Process(changes)
	}
}

func (p *pg) heartbeat(ctx context.Context) error {
	// Send a hearbeat every minute
	p.queryLock.Lock()
	_, err := p.queryConn.Exec(ctx, "SELECT pg_logical_emit_message(false, 'heartbeat', now()::varchar);")
	p.queryLock.Unlock()
	return err

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
			p.nextReportTime = time.Now().Add(CommitInterval)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), ReadTimeout)
	rawMsg, err := p.conn.PgConn().ReceiveMessage(ctx)
	cancel()

	if err != nil {
		if pgconn.Timeout(err) {
			if err := p.forceNextReport(ctx); err != nil {
				p.log.Warn("unable to report lsn on timeout", "error", err, "host", p.opts.Config.Host)
			}
			// We return nil as we want to keep iterating.
			return nil, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Neon returns unexpected EOFs without new messages.  Handle
			// this gracefully.
			return nil, nil
		}
		return nil, err
	}

	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		return nil, fmt.Errorf("received pg wal error: %#v", errMsg)
	}

	if _, ok := rawMsg.(*pgproto3.CommandComplete); ok {
		return nil, nil
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
			if err := p.forceNextReport(ctx); err != nil {
				p.log.Warn("unable to report lsn on request", "error", err, "host", p.opts.Config.Host)
			}
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
				PostgresWatermark: changeset.PostgresWatermark{
					LSN:        xld.WALStart,
					ServerTime: xld.ServerTime,
				},
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

func (p *pg) ServerLSN(ctx context.Context) (pglogrepl.LSN, error) {
	identify, err := pglogrepl.IdentifySystem(ctx, p.conn.PgConn())
	if err != nil {
		if converted, err := standardizeErr(err); converted {
			return pglogrepl.LSN(0), err
		}
		return pglogrepl.LSN(0), fmt.Errorf("error identifying postgres: %w", err)
	}

	// By default, start at the current LSN, ie. the latest point in the stream.
	return identify.XLogPos, nil
}

func (p *pg) committedWatermark() (wm changeset.Watermark) {
	lsn, nano := atomic.LoadUint64(&p.lsn), atomic.LoadInt64(&p.lsnTime)
	return changeset.Watermark{
		PostgresWatermark: changeset.PostgresWatermark{
			LSN:        pglogrepl.LSN(lsn),
			ServerTime: time.Unix(0, nano),
		},
	}
}

func (p *pg) forceNextReport(ctx context.Context) error {
	// Updating the next report time to a zero time always reports the LSN,
	// as time.Now() is always after the empty time.
	p.nextReportTime = time.Time{}
	return p.report(ctx, true)
}

// report reports the current replication slot's LSN progress to the server.  We can optionally
// force the server to reply with an ack by setting forceReply to true.  This is used when we
// receive timeout errors from PG;  it acts as a ping.
func (p *pg) report(ctx context.Context, forceReply bool) error {
	lsn := p.LSN()
	if lsn == 0 {
		return nil
	}

	p.log.Debug("reporting lsn to source", "lsn", p.LSN().String())

	err := pglogrepl.SendStandbyStatusUpdate(ctx,
		p.conn.PgConn(),
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

func (p *pg) walMode(ctx context.Context) (string, error) {
	p.queryLock.Lock()
	defer p.queryLock.Unlock()

	var mode string
	row := p.queryConn.QueryRow(ctx, "SHOW wal_level")
	err := row.Scan(&mode)
	return mode, err
}

// copySlice is a util for copying a slice.
func copySlice(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

type ReplicationSlot struct {
	Active            bool
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
}

func ReplicationSlotData(ctx context.Context, conn *pgx.Conn) (ReplicationSlot, error) {
	ret := ReplicationSlot{}
	row := conn.QueryRow(
		ctx,
		fmt.Sprintf(`SELECT
                        active, restart_lsn, confirmed_flush_lsn
                        FROM pg_replication_slots WHERE slot_name = '%s';`,
			pgconsts.SlotName,
		),
	)
	err := row.Scan(&ret.Active, &ret.RestartLSN, &ret.ConfirmedFlushLSN)
	// pgx has its own ErrNoRows :(
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
		return ret, ErrReplicationSlotNotFound
	}
	return ret, err
}

func standardizeErr(err error) (bool, error) {
	msg := err.Error()
	if strings.Contains(msg, "logical decoding requires wal_level") {
		return true, ErrLogicalReplicationNotSetUp
	}
	if strings.Contains(msg, fmt.Sprintf(`replication slot "%s" does not exist`, pgconsts.SlotName)) {
		return true, ErrReplicationSlotNotFound
	}
	if strings.Contains(msg, fmt.Sprintf(`replication slot "%s" is active`, pgconsts.SlotName)) {
		return true, ErrReplicationAlreadyRunning
	}
	return false, err
}

func IsConnClosedErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "conn closed")
}
