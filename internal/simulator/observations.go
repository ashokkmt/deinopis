package simulator

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	_ "modernc.org/sqlite"
)

func recordVerification(ctx context.Context, witnessDB, runID string, result DemoVerification) (*time.Time, int, error) {
	db, err := sql.Open("sqlite", witnessDB)
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, `PRAGMA synchronous=FULL`); err != nil {
		return nil, 0, err
	}
	if _, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS simulator_observations (
		id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
		checked_at_ns INTEGER NOT NULL, result_json BLOB NOT NULL);
		CREATE TABLE IF NOT EXISTS simulator_first_adverse (
		run_id TEXT PRIMARY KEY, observed_at_ns INTEGER NOT NULL, result_json BLOB NOT NULL);`); err != nil {
		return nil, 0, err
	}
	body, err := json.Marshal(result)
	if err != nil {
		return nil, 0, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO simulator_observations(run_id,checked_at_ns,result_json) VALUES(?,?,?)`, runID, result.CheckedAt.UnixNano(), body); err != nil {
		return nil, 0, err
	}
	if result.Content != "INTACT" || result.Coverage != "complete" || result.Witness != "first-write originals" || result.Anchor != "local registry readback" {
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO simulator_first_adverse(run_id,observed_at_ns,result_json) VALUES(?,?,?)`, runID, result.CheckedAt.UnixNano(), body); err != nil {
			return nil, 0, err
		}
	}
	var firstNS int64
	if err = tx.QueryRowContext(ctx, `SELECT observed_at_ns FROM simulator_first_adverse WHERE run_id=?`, runID).Scan(&firstNS); err != nil && err != sql.ErrNoRows {
		return nil, 0, err
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM simulator_observations WHERE run_id=?`, runID).Scan(&count); err != nil {
		return nil, 0, err
	}
	if err = tx.Commit(); err != nil {
		return nil, 0, err
	}
	if firstNS == 0 {
		return nil, count, nil
	}
	first := time.Unix(0, firstNS).UTC()
	return &first, count, nil
}
