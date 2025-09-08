package server

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func InitSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `create table if not exists auth_credentials (
		user_id uuid primary key,
		user_name text,
		password_hash text not null,
		created_at timestamptz not null default now(),
		updated_at timestamptz not null default now()
	);`)
	if err != nil {
		return err
	}
	
	_, _ = pool.Exec(ctx, `alter table auth_credentials add column if not exists user_name text`)
	
	_, _ = pool.Exec(ctx, `alter table auth_credentials drop constraint if exists auth_credentials_user_name_key`)
	
	_, _ = pool.Exec(ctx, `drop index if exists idx_auth_credentials_user_name`)
	return nil
}
