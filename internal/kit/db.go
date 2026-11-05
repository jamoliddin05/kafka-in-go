package kit

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectDB opens a pgx pool and waits until Postgres answers, so a role
// started together with the database doesn't crash-loop on boot.
func (a *App) ConnectDB(ctx context.Context) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, a.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("pgx pool: %w", err)
	}
	for {
		err = pool.Ping(ctx)
		if err == nil {
			return pool, nil
		}
		a.Log.Warn("postgres not ready, retrying", "err", err)
		select {
		case <-ctx.Done():
			pool.Close()
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
