package pgsetup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/inngest/dbcap/pkg/consts/pgconsts"
	"github.com/inngest/dbcap/pkg/replicator"
	"github.com/jackc/pgx/v5"
)

var (
	ErrLogicalReplicationNotSetUp = fmt.Errorf("ERR_PG_001: Your database does not have logical replication configured.  You must set the WAL level to 'logical' to stream events.")
	ErrReplicationSlotNotFound    = fmt.Errorf("ERR_PG_002: The replication slot 'inngest_cdc' doesn't exist in your database.  Please create the logical replication slot to stream events.")
	ErrReplicationAlreadyRunning  = fmt.Errorf("ERR_PG_901: Replication is already streaming events")
)

type TestConnResult struct {
	LogicalReplication replicator.ConnectionStepResult
	UserCreated        replicator.ConnectionStepResult
	RolesGranted       replicator.ConnectionStepResult
	SlotCreated        replicator.ConnectionStepResult
	PublicationCreated replicator.ConnectionStepResult
}

func (c TestConnResult) Steps() []string {
	return []string{
		"logical_replication_enabled",
		"user_created",
		"roles_granted",
		"replication_slot_created",
		"publication_created",
	}
}

func (c TestConnResult) Results() map[string]replicator.ConnectionStepResult {
	return map[string]replicator.ConnectionStepResult{
		"logical_replication_enabled": c.LogicalReplication,
		"user_created":                c.UserCreated,
		"roles_granted":               c.RolesGranted,
		"replication_slot_created":    c.SlotCreated,
		"publication_created":         c.PublicationCreated,
	}
}

type SetupOpts struct {
	AdminConfig pgx.ConnConfig
	// Password represents the password for the replication user.
	Password string

	DisableCreateUser        bool
	DisableCreateRoles       bool
	DisableCreateSlot        bool
	DisableCreatePublication bool
}

func Setup(ctx context.Context, opts SetupOpts) (TestConnResult, error) {
	conn, err := pgx.ConnectConfig(ctx, &opts.AdminConfig)
	if err != nil {
		return TestConnResult{}, err
	}

	setup := setup{
		opts: opts,
		c:    conn,
	}
	return setup.Setup(ctx)
}

func Check(ctx context.Context, opts SetupOpts) (TestConnResult, error) {
	conn, err := pgx.ConnectConfig(ctx, &opts.AdminConfig)
	if err != nil {
		return TestConnResult{}, err
	}

	setup := setup{
		opts: opts,
		c:    conn,
	}
	return setup.Check(ctx)
}

type setup struct {
	opts SetupOpts
	c    *pgx.Conn

	res TestConnResult
}

func (s *setup) Check(ctx context.Context) (TestConnResult, error) {
	chain := []func(ctx context.Context) error{
		s.checkWAL,
		s.checkUser,
		s.checkRoles,
		s.checkReplicationSlot,
		s.checkPublication,
	}
	for _, f := range chain {
		if err := f(ctx); err != nil {
			// Short circuit and return the connection result and first error.
			return s.res, err
		}
	}
	return s.res, nil
}

func (s *setup) Setup(ctx context.Context) (TestConnResult, error) {
	chain := []func(ctx context.Context) error{}

	if !s.opts.DisableCreateUser {
		chain = append(chain, s.createUser)
	}
	if !s.opts.DisableCreateRoles {
		chain = append(chain, s.createRoles)
	}
	if !s.opts.DisableCreateSlot {
		chain = append(chain, s.createReplicationSlot)
	}
	if !s.opts.DisableCreatePublication {
		chain = append(chain, s.createPublication)
	}
	for _, f := range chain {
		if err := f(ctx); err != nil {
			// Short circuit and return the connection result and first error.
			return s.res, err
		}
	}
	return s.res, nil
}

func (s *setup) checkWAL(ctx context.Context) error {
	var mode string
	row := s.c.QueryRow(ctx, "SHOW wal_level")
	err := row.Scan(&mode)
	if err != nil {
		s.res.LogicalReplication.Error = fmt.Errorf("Error checking WAL mode: %w", err)
		return s.res.LogicalReplication.Error
	}
	if mode != "logical" {
		s.res.LogicalReplication.Error = ErrLogicalReplicationNotSetUp
		return s.res.LogicalReplication.Error
	}
	s.res.LogicalReplication.Complete = true
	return nil
}

// checkUser checks if the UserCreated step is complete.
func (s *setup) checkUser(ctx context.Context) error {
	row := s.c.QueryRow(ctx,
		"SELECT 1 FROM pg_roles WHERE rolname = $1",
		pgconsts.Username,
	)
	var i int
	err := row.Scan(&i)

	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
		// Add the error to the TestConnResult.
		s.res.UserCreated.Error = fmt.Errorf("User '%s' does not exist", pgconsts.Username)
		return s.res.UserCreated.Error
	}

	s.res.UserCreated.Complete = true
	return nil
}

func (s *setup) createUser(ctx context.Context) error {
	if err := s.checkUser(ctx); err == nil {
		// The user already exists;  don't need to add.
		return nil
	}

	stmt := fmt.Sprintf(`
		CREATE USER %s WITH REPLICATION PASSWORD '%s';
	`, pgconsts.Username, s.opts.Password)
	_, err := s.c.Exec(ctx, stmt)
	if err != nil {
		s.res.UserCreated.Error = fmt.Errorf("Error creating user '%s': %w", pgconsts.Username, err)
		return s.res.UserCreated.Error
	}
	return nil
}

// checkRoles checks if the Inngest user has necessary roles
func (s *setup) checkRoles(ctx context.Context) error {
	// Check roles is a stub implementation and will always execute.
	return nil
}

func (s *setup) createRoles(ctx context.Context) error {
	stmt := fmt.Sprintf(`
		GRANT USAGE ON SCHEMA public TO %s;
		GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;
		ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s;
	`, pgconsts.Username, pgconsts.Username, pgconsts.Username)
	_, err := s.c.Exec(ctx, stmt)
	if err != nil {
		s.res.RolesGranted.Error = fmt.Errorf("Error granting roles for user '%s': %w", pgconsts.Username, err)
		return s.res.RolesGranted.Error
	}
	s.res.RolesGranted.Complete = true
	return nil
}

func (s *setup) checkReplicationSlot(ctx context.Context) error {
	row := s.c.QueryRow(ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
		pgconsts.SlotName,
	)
	var i int
	err := row.Scan(&i)

	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
		s.res.SlotCreated.Error = ErrReplicationSlotNotFound
		return s.res.SlotCreated.Error
	}

	s.res.SlotCreated.Complete = true
	return nil
}

func (s *setup) createReplicationSlot(ctx context.Context) error {
	if err := s.checkReplicationSlot(ctx); err == nil {
		return nil
	}

	stmt := `
		-- pgoutput logical repl plugin
		SELECT pg_create_logical_replication_slot('inngest_cdc', 'pgoutput');
	`
	_, err := s.c.Exec(ctx, stmt)
	if err != nil {
		s.res.SlotCreated.Error = fmt.Errorf("Error creating replication slot '%s': %w", pgconsts.SlotName, err)
		return s.res.SlotCreated.Error
	}
	s.res.SlotCreated.Complete = true
	return nil
}

func (s *setup) checkPublication(ctx context.Context) error {
	row := s.c.QueryRow(ctx,
		"SELECT 1 FROM pg_publication WHERE pubname = $1",
		pgconsts.PublicationName,
	)
	var i int
	err := row.Scan(&i)

	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
		s.res.PublicationCreated.Error = fmt.Errorf("The publication '%s' doesn't exist in your database", pgconsts.PublicationName)
		return s.res.PublicationCreated.Error
	}

	s.res.PublicationCreated.Complete = true
	return nil
}

func (s *setup) createPublication(ctx context.Context) error {
	if err := s.checkPublication(ctx); err == nil {
		return nil
	}

	stmt := fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES;`, pgconsts.PublicationName)
	_, err := s.c.Exec(ctx, stmt)
	if err != nil {
		s.res.PublicationCreated.Error = fmt.Errorf("Error creating publication '%s': %w", pgconsts.PublicationName, err)
		return s.res.PublicationCreated.Error
	}
	s.res.PublicationCreated.Complete = true
	return nil
}
