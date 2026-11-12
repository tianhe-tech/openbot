package memstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// ErrArchiveLeased reports that another process holds the archive lease.
var ErrArchiveLeased = errors.New("memstore: archive is leased by another gateway instance")

// LeaseOptions tunes the instance lease. The zero value is usable.
type LeaseOptions struct {
	// TTL is how long a lease survives without a heartbeat. Zero uses 90
	// seconds. It must be comfortably longer than Heartbeat so a slow pass
	// does not lose the lease, and short enough that a crashed holder does not
	// block a restart for long.
	TTL time.Duration
	// Heartbeat is how often the holder renews. Zero uses TTL/3.
	Heartbeat time.Duration
	// Holder identifies this process in the lease row. Zero uses
	// "<hostname>:<pid>", which is enough for an operator to tell which
	// process holds the archive.
	Holder string
}

func (options LeaseOptions) resolved() LeaseOptions {
	if options.TTL <= 0 {
		options.TTL = 90 * time.Second
	}
	if options.Heartbeat <= 0 {
		options.Heartbeat = options.TTL / 3
	}
	if options.Heartbeat <= 0 {
		options.Heartbeat = time.Second
	}
	if options.Holder == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "unknown"
		}
		options.Holder = fmt.Sprintf("%s:%d", host, os.Getpid())
	}
	return options
}

// Lease is a held instance lease. Release must be called on shutdown; the TTL
// is the fallback for a crash, not the normal path.
type Lease struct {
	writer      *ArchiveWriter
	holder      string
	ttl         time.Duration
	stop        chan struct{}
	done        chan struct{}
	acquired    int64
	releaseOnce sync.Once
	releaseErr  error
}

// AcquireLease takes the archive's instance lease, or returns ErrArchiveLeased
// if a live holder exists.
//
// The check and the claim are one statement. A read followed by a write would
// let two processes both observe an expired lease and both claim it, which is
// the exact race the lease exists to prevent.
func (writer *ArchiveWriter) AcquireLease(ctx context.Context, options LeaseOptions) (*Lease, error) {
	options = options.resolved()
	now := time.Now().UTC().UnixMilli()
	expires := now + options.TTL.Milliseconds()
	// The WHERE clause on the DO UPDATE is what makes this atomic: when a live
	// holder exists the update is skipped, so no row is returned and the
	// caller learns it lost. When the lease is expired or absent the update
	// runs and RETURNING hands back the row.
	var holder string
	err := writer.store.db.QueryRowContext(ctx, `INSERT INTO mem_v2_instance_lease
		(id, holder, acquired_at, heartbeat_at, expires_at) VALUES (1, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			holder = excluded.holder,
			acquired_at = excluded.acquired_at,
			heartbeat_at = excluded.heartbeat_at,
			expires_at = excluded.expires_at
		WHERE mem_v2_instance_lease.expires_at <= ?
		RETURNING holder`, options.Holder, now, now, expires, now).Scan(&holder)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrArchiveLeased
	}
	if err != nil {
		return nil, fmt.Errorf("memstore: acquire instance lease: %w", err)
	}
	lease := &Lease{
		writer:   writer,
		holder:   options.Holder,
		ttl:      options.TTL,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		acquired: now,
	}
	writer.mu.Lock()
	writer.lease = lease
	writer.mu.Unlock()
	go lease.heartbeat(options.Heartbeat)
	return lease, nil
}

// Holder reports the identity recorded in the lease row.
func (lease *Lease) Holder() string { return lease.holder }

func (lease *Lease) heartbeat(interval time.Duration) {
	defer close(lease.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-lease.stop:
			return
		case <-ticker.C:
			// A failed heartbeat is not fatal on its own: the lease is still
			// valid until expires_at, and the next tick may succeed. Losing
			// the row entirely means another process took over, which the
			// next successful renewal will not fix, so the heartbeat stops
			// rather than fighting for it.
			if !lease.renew() {
				return
			}
		}
	}
}

// renew extends the lease. It returns false when the row is no longer ours,
// which means another process claimed it after our lease expired.
func (lease *Lease) renew() bool {
	now := time.Now().UTC().UnixMilli()
	ctx, cancel := context.WithTimeout(context.Background(), lease.ttl/3)
	defer cancel()
	result, err := lease.writer.store.db.ExecContext(ctx, `UPDATE mem_v2_instance_lease
		SET heartbeat_at = ?, expires_at = ? WHERE id = 1 AND holder = ? AND acquired_at = ? AND expires_at > ?`,
		now, now+lease.ttl.Milliseconds(), lease.holder, lease.acquired, now)
	if err != nil {
		return true // transient; keep trying until the lease actually expires
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return true
	}
	return affected > 0
}

// Release drops the lease and stops the heartbeat. It is safe to call twice.
func (lease *Lease) Release() error {
	lease.releaseOnce.Do(func() {
		close(lease.stop)
		<-lease.done
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := lease.writer.store.db.ExecContext(ctx, `DELETE FROM mem_v2_instance_lease WHERE id = 1 AND holder = ? AND acquired_at = ?`, lease.holder, lease.acquired)
		if err != nil {
			lease.releaseErr = fmt.Errorf("memstore: release instance lease: %w", err)
		}
	})
	return lease.releaseErr
}

// LeaseHolder reports which process currently holds the lease, or an empty
// string when the archive is unleased. It exists so a startup failure can name
// the holder instead of only saying "leased".
func (writer *ArchiveWriter) LeaseHolder(ctx context.Context) (string, error) {
	var holder string
	var expires int64
	err := writer.store.db.QueryRowContext(ctx,
		`SELECT holder, expires_at FROM mem_v2_instance_lease WHERE id = 1`).Scan(&holder, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("memstore: read instance lease: %w", err)
	}
	if expires <= time.Now().UTC().UnixMilli() {
		return "", nil
	}
	return holder, nil
}
