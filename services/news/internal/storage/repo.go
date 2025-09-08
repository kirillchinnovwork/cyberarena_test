package storage

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type News struct {
	ID               uuid.UUID
	Title            string
	ShortDescription string
	CoverURL         string
	Content          string
	IsPublished      bool
	PublishedAt      *time.Time
	UpdatedAt        time.Time
}

type Attachment struct {
	ID          uuid.UUID
	URL         string
	ContentType string
	Size        int64
	ObjectKey   string
}

type Repo struct {
	pool *pgxpool.Pool
}

func NewRepo(pool *pgxpool.Pool) *Repo { return &Repo{pool: pool} }

func (r *Repo) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS news (
			id UUID PRIMARY KEY,
			title TEXT NOT NULL,
			short_description TEXT NOT NULL,
			cover_url TEXT NOT NULL,
			content TEXT NOT NULL,
			is_published BOOLEAN NOT NULL DEFAULT FALSE,
			published_at TIMESTAMPTZ NULL,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_news_published ON news(is_published)`,
		`CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at)`,
		`CREATE TABLE IF NOT EXISTS attachments (
			id UUID PRIMARY KEY,
			url TEXT NOT NULL,
			content_type TEXT NOT NULL,
			size BIGINT NOT NULL,
			object_key TEXT NOT NULL
		)`,
	}
	for _, s := range stmts {
		if _, err := r.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) CreateNews(ctx context.Context, n *News) error {
	if n.ID == uuid.Nil {
		return errors.New("news id is nil")
	}
	q := `INSERT INTO news (id, title, short_description, cover_url, content, is_published, published_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
	_, err := r.pool.Exec(ctx, q, n.ID, n.Title, n.ShortDescription, n.CoverURL, n.Content, n.IsPublished, n.PublishedAt, n.UpdatedAt)
	return err
}

func (r *Repo) UpdateNews(ctx context.Context, n *News) error {
	q := `UPDATE news SET title=$2, short_description=$3, cover_url=$4, content=$5, is_published=$6, published_at=$7, updated_at=$8 WHERE id=$1`
	ct, err := r.pool.Exec(ctx, q, n.ID, n.Title, n.ShortDescription, n.CoverURL, n.Content, n.IsPublished, n.PublishedAt, n.UpdatedAt)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) DeleteNews(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `DELETE FROM news WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) GetNews(ctx context.Context, id uuid.UUID) (*News, error) {
	row := r.pool.QueryRow(ctx, `SELECT id, title, short_description, cover_url, content, is_published, published_at, updated_at FROM news WHERE id=$1`, id)
	n := &News{}
	var publishedAt *time.Time
	if err := row.Scan(&n.ID, &n.Title, &n.ShortDescription, &n.CoverURL, &n.Content, &n.IsPublished, &publishedAt, &n.UpdatedAt); err != nil {
		return nil, err
	}
	n.PublishedAt = publishedAt
	return n, nil
}

func (r *Repo) GetPublishedNews(ctx context.Context, id uuid.UUID) (*News, error) {
	row := r.pool.QueryRow(ctx, `SELECT id, title, short_description, cover_url, content, is_published, published_at, updated_at FROM news WHERE id=$1 AND is_published=TRUE`, id)
	n := &News{}
	var publishedAt *time.Time
	if err := row.Scan(&n.ID, &n.Title, &n.ShortDescription, &n.CoverURL, &n.Content, &n.IsPublished, &publishedAt, &n.UpdatedAt); err != nil {
		return nil, err
	}
	n.PublishedAt = publishedAt
	return n, nil
}

func (r *Repo) ListNews(ctx context.Context, page, pageSize int, publishedOnly bool) ([]*News, int32, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize
	where := ""
	if publishedOnly {
		where = "WHERE is_published=TRUE"
	}
	var total int32
	countSQL := "SELECT COUNT(*) FROM news " + where
	if err := r.pool.QueryRow(ctx, countSQL).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := r.pool.Query(ctx, "SELECT id, title, short_description, cover_url, content, is_published, published_at, updated_at FROM news "+where+" ORDER BY published_at DESC NULLS LAST, updated_at DESC LIMIT $1 OFFSET $2", pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var list []*News
	for rows.Next() {
		n := &News{}
		var publishedAt *time.Time
		if err := rows.Scan(&n.ID, &n.Title, &n.ShortDescription, &n.CoverURL, &n.Content, &n.IsPublished, &publishedAt, &n.UpdatedAt); err != nil {
			return nil, 0, err
		}
		n.PublishedAt = publishedAt
		list = append(list, n)
	}
	if rows.Err() != nil {
		return nil, 0, rows.Err()
	}
	return list, total, nil
}

func (r *Repo) SetPublishState(ctx context.Context, id uuid.UUID, publish bool, at time.Time) (*News, error) {
	var publishedAt *time.Time
	if publish {
		publishedAt = &at
	} else {
		publishedAt = nil
	}
	q := `UPDATE news SET is_published=$2, published_at=$3, updated_at=$4 WHERE id=$1 RETURNING id, title, short_description, cover_url, content, is_published, published_at, updated_at`
	row := r.pool.QueryRow(ctx, q, id, publish, publishedAt, at)
	n := &News{}
	var pa *time.Time
	if err := row.Scan(&n.ID, &n.Title, &n.ShortDescription, &n.CoverURL, &n.Content, &n.IsPublished, &pa, &n.UpdatedAt); err != nil {
		return nil, err
	}
	n.PublishedAt = pa
	return n, nil
}

func (r *Repo) CreateAttachment(ctx context.Context, a *Attachment) error {
	if a.ID == uuid.Nil {
		return errors.New("attachment id is nil")
	}
	_, err := r.pool.Exec(ctx, `INSERT INTO attachments (id, url, content_type, size, object_key) VALUES ($1,$2,$3,$4,$5)`, a.ID, a.URL, a.ContentType, a.Size, a.ObjectKey)
	return err
}

func (r *Repo) DeleteAttachment(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `DELETE FROM attachments WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) GetAttachment(ctx context.Context, id uuid.UUID) (*Attachment, error) {
	row := r.pool.QueryRow(ctx, `SELECT id, url, content_type, size, object_key FROM attachments WHERE id=$1`, id)
	a := &Attachment{}
	if err := row.Scan(&a.ID, &a.URL, &a.ContentType, &a.Size, &a.ObjectKey); err != nil {
		return nil, err
	}
	return a, nil
}

func (r *Repo) GetAttachments(ctx context.Context, ids []uuid.UUID) ([]*Attachment, error) {
	if len(ids) == 0 {
		return []*Attachment{}, nil
	}
	
	rows, err := r.pool.Query(ctx, `SELECT id, url, content_type, size, object_key FROM attachments WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*Attachment
	for rows.Next() {
		a := &Attachment{}
		if err := rows.Scan(&a.ID, &a.URL, &a.ContentType, &a.Size, &a.ObjectKey); err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return list, nil
}
