package worker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

type portPool struct {
	db        *sql.DB
	firstPort int
	lastPort  int
	mu        sync.Mutex
}

func newPortPool(configuredRange string) (*portPool, error) {
	parts := strings.Split(configuredRange, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid host-port range %q; expected start-end", configuredRange)
	}
	first, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, fmt.Errorf("invalid first host port: %w", err)
	}
	last, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, fmt.Errorf("invalid last host port: %w", err)
	}
	if first < 1 || last > 65535 || first > last {
		return nil, fmt.Errorf("invalid host-port range %q", configuredRange)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory for host-port database: %w", err)
	}
	dir := filepath.Join(home, ".orclet")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create orclet state directory: %w", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "runtime.db"))
	if err != nil {
		return nil, fmt.Errorf("open host-port database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS host_ports (
		replica_id TEXT PRIMARY KEY,
		port INTEGER NOT NULL UNIQUE
	)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize host-port database: %w", err)
	}
	return &portPool{db: db, firstPort: first, lastPort: last}, nil
}

func (p *portPool) close() error {
	return p.db.Close()
}

func (p *portPool) acquire(ctx context.Context, replicaID string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var current int
	err = tx.QueryRowContext(ctx, "SELECT port FROM host_ports WHERE replica_id = ?", replicaID).Scan(&current)
	if err == nil {
		return current, tx.Commit()
	}
	if err != sql.ErrNoRows {
		return 0, err
	}
	for port := p.firstPort; port <= p.lastPort; port++ {
		result, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO host_ports (replica_id, port) VALUES (?, ?)", replicaID, port)
		if err != nil {
			return 0, err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		if inserted == 1 {
			return port, tx.Commit()
		}
	}
	return 0, fmt.Errorf("host-port pool %d-%d is exhausted", p.firstPort, p.lastPort)
}

func (p *portPool) reserve(ctx context.Context, replicaID string, port int) error {
	if port == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.db.ExecContext(ctx, `INSERT INTO host_ports (replica_id, port) VALUES (?, ?)
		ON CONFLICT(replica_id) DO UPDATE SET port = excluded.port`, replicaID, port)
	if err != nil {
		return fmt.Errorf("reserve host port %d for replica %q: %w", port, replicaID, err)
	}
	return nil
}

func (p *portPool) release(ctx context.Context, replicaID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, err := p.db.ExecContext(ctx, "DELETE FROM host_ports WHERE replica_id = ?", replicaID)
	return err
}

func (p *portPool) releaseUndesired(ctx context.Context, desired map[string]struct{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows, err := p.db.QueryContext(ctx, "SELECT replica_id FROM host_ports")
	if err != nil {
		return err
	}
	var orphaned []string
	for rows.Next() {
		var replicaID string
		if err := rows.Scan(&replicaID); err != nil {
			_ = rows.Close()
			return err
		}
		if _, wanted := desired[replicaID]; !wanted {
			orphaned = append(orphaned, replicaID)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, replicaID := range orphaned {
		if _, err := p.db.ExecContext(ctx, "DELETE FROM host_ports WHERE replica_id = ?", replicaID); err != nil {
			return fmt.Errorf("release stale host port for replica %q: %w", replicaID, err)
		}
	}
	return nil
}
