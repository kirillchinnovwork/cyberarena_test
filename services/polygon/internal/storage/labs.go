package storage

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Lab struct {
	ID          uuid.UUID
	PolygonID   uuid.UUID
	Title       string
	Description string
	StartedAt   *time.Time
	TTLSeconds  int64
	GroupID     *uuid.UUID
	StepCount   int32
	CreatedAt   time.Time
}

type LabStep struct {
	ID           uuid.UUID
	LabID        uuid.UUID
	Title        string
	Description  string
	InitialItems json.RawMessage
	HasAnswer    bool
	Answer       json.RawMessage
	OrderIndex   int32
}

func (r *Repo) MigrateLabs(ctx context.Context) error {
	stmts := []string{
		`create table if not exists labs(
			id uuid primary key,
			polygon_id uuid not null references polygons(id) on delete cascade,
			title text not null,
			description text not null default '',
			started_at timestamptz,
			ttl_seconds bigint not null default 0,
			group_id uuid,
			step_count int not null default 0,
			created_at timestamptz not null default now()
		);`,
		`create index if not exists idx_labs_polygon on labs(polygon_id);`,
		`create index if not exists idx_labs_group on labs(group_id);`,
		`create table if not exists lab_steps(
			id uuid primary key,
			lab_id uuid not null references labs(id) on delete cascade,
			title text not null,
			description text not null default '',
			initial_items jsonb not null default '{}',
			has_answer boolean not null default false,
			answer jsonb not null default '{}',
			order_index int not null default 0
		);`,
		`create index if not exists idx_lab_steps_lab on lab_steps(lab_id);`,
	}
	for _, s := range stmts {
		if _, err := r.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) CreateLab(ctx context.Context, lab *Lab) error {
	_, err := r.pool.Exec(ctx, `insert into labs(id, polygon_id, title, description, started_at, ttl_seconds, group_id, step_count, created_at)
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		lab.ID, lab.PolygonID, lab.Title, lab.Description, lab.StartedAt, lab.TTLSeconds, lab.GroupID, lab.StepCount, lab.CreatedAt)
	return err
}

func (r *Repo) GetLab(ctx context.Context, id uuid.UUID) (*Lab, error) {
	row := r.pool.QueryRow(ctx, `select id, polygon_id, title, description, started_at, ttl_seconds, group_id, step_count, created_at
		from labs where id = $1`, id)
	return scanLab(row)
}

func (r *Repo) GetLabByPolygon(ctx context.Context, polygonID uuid.UUID) (*Lab, error) {
	row := r.pool.QueryRow(ctx, `select id, polygon_id, title, description, started_at, ttl_seconds, group_id, step_count, created_at
		from labs where polygon_id = $1 order by created_at desc limit 1`, polygonID)
	return scanLab(row)
}

func (r *Repo) ListLabs(ctx context.Context, polygonID *uuid.UUID) ([]Lab, error) {
	var rows pgx.Rows
	var err error
	if polygonID != nil {
		rows, err = r.pool.Query(ctx, `select id, polygon_id, title, description, started_at, ttl_seconds, group_id, step_count, created_at
			from labs where polygon_id = $1 order by created_at desc`, *polygonID)
	} else {
		rows, err = r.pool.Query(ctx, `select id, polygon_id, title, description, started_at, ttl_seconds, group_id, step_count, created_at
			from labs order by created_at desc`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var labs []Lab
	for rows.Next() {
		var lab Lab
		if err := rows.Scan(&lab.ID, &lab.PolygonID, &lab.Title, &lab.Description, &lab.StartedAt, &lab.TTLSeconds, &lab.GroupID, &lab.StepCount, &lab.CreatedAt); err != nil {
			return nil, err
		}
		labs = append(labs, lab)
	}
	return labs, rows.Err()
}

func (r *Repo) UpdateLab(ctx context.Context, id uuid.UUID, title, description *string, startedAt *time.Time, ttlSeconds *int64, groupID *uuid.UUID) error {
	sets := []string{}
	args := []any{}
	idx := 1

	if title != nil {
		sets = append(sets, "title=$"+strconv.Itoa(idx))
		args = append(args, *title)
		idx++
	}
	if description != nil {
		sets = append(sets, "description=$"+strconv.Itoa(idx))
		args = append(args, *description)
		idx++
	}
	if startedAt != nil {
		sets = append(sets, "started_at=$"+strconv.Itoa(idx))
		args = append(args, *startedAt)
		idx++
	}
	if ttlSeconds != nil {
		sets = append(sets, "ttl_seconds=$"+strconv.Itoa(idx))
		args = append(args, *ttlSeconds)
		idx++
	}
	if groupID != nil {
		sets = append(sets, "group_id=$"+strconv.Itoa(idx))
		args = append(args, *groupID)
		idx++
	}

	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	q := "update labs set " + strings.Join(sets, ",") + " where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) DeleteLab(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from labs where id = $1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) UpdateLabStepCount(ctx context.Context, labID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `update labs set step_count = (select count(*) from lab_steps where lab_id = $1) where id = $1`, labID)
	return err
}

func (r *Repo) CreateLabStep(ctx context.Context, step *LabStep) error {
	_, err := r.pool.Exec(ctx, `insert into lab_steps(id, lab_id, title, description, initial_items, has_answer, answer, order_index)
		values ($1, $2, $3, $4, $5, $6, $7, $8)`,
		step.ID, step.LabID, step.Title, step.Description, step.InitialItems, step.HasAnswer, step.Answer, step.OrderIndex)
	if err != nil {
		return err
	}
	return r.UpdateLabStepCount(ctx, step.LabID)
}

func (r *Repo) GetLabStep(ctx context.Context, id uuid.UUID) (*LabStep, error) {
	row := r.pool.QueryRow(ctx, `select id, lab_id, title, description, initial_items, has_answer, answer, order_index
		from lab_steps where id = $1`, id)
	return scanLabStep(row)
}

func (r *Repo) ListLabSteps(ctx context.Context, labID uuid.UUID) ([]LabStep, error) {
	rows, err := r.pool.Query(ctx, `select id, lab_id, title, description, initial_items, has_answer, answer, order_index
		from lab_steps where lab_id = $1 order by order_index`, labID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []LabStep
	for rows.Next() {
		var step LabStep
		if err := rows.Scan(&step.ID, &step.LabID, &step.Title, &step.Description, &step.InitialItems, &step.HasAnswer, &step.Answer, &step.OrderIndex); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func (r *Repo) ListLabStepsPublic(ctx context.Context, labID uuid.UUID) ([]LabStep, error) {
	rows, err := r.pool.Query(ctx, `select id, lab_id, title, description, initial_items, has_answer, order_index
		from lab_steps where lab_id = $1 order by order_index`, labID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []LabStep
	for rows.Next() {
		var step LabStep
		if err := rows.Scan(&step.ID, &step.LabID, &step.Title, &step.Description, &step.InitialItems, &step.HasAnswer, &step.OrderIndex); err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	return steps, rows.Err()
}

func (r *Repo) UpdateLabStep(ctx context.Context, id uuid.UUID, title, description *string, initialItems, answer *json.RawMessage, hasAnswer *bool, orderIndex *int32) error {
	sets := []string{}
	args := []any{}
	idx := 1

	if title != nil {
		sets = append(sets, "title=$"+strconv.Itoa(idx))
		args = append(args, *title)
		idx++
	}
	if description != nil {
		sets = append(sets, "description=$"+strconv.Itoa(idx))
		args = append(args, *description)
		idx++
	}
	if initialItems != nil {
		sets = append(sets, "initial_items=$"+strconv.Itoa(idx))
		args = append(args, *initialItems)
		idx++
	}
	if hasAnswer != nil {
		sets = append(sets, "has_answer=$"+strconv.Itoa(idx))
		args = append(args, *hasAnswer)
		idx++
	}
	if answer != nil {
		sets = append(sets, "answer=$"+strconv.Itoa(idx))
		args = append(args, *answer)
		idx++
	}
	if orderIndex != nil {
		sets = append(sets, "order_index=$"+strconv.Itoa(idx))
		args = append(args, *orderIndex)
		idx++
	}

	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	q := "update lab_steps set " + strings.Join(sets, ",") + " where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) DeleteLabStep(ctx context.Context, id uuid.UUID) error {
	var labID uuid.UUID
	err := r.pool.QueryRow(ctx, `select lab_id from lab_steps where id = $1`, id).Scan(&labID)
	if err != nil {
		return err
	}

	ct, err := r.pool.Exec(ctx, `delete from lab_steps where id = $1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}

	return r.UpdateLabStepCount(ctx, labID)
}

func scanLab(row pgx.Row) (*Lab, error) {
	var lab Lab
	if err := row.Scan(&lab.ID, &lab.PolygonID, &lab.Title, &lab.Description, &lab.StartedAt, &lab.TTLSeconds, &lab.GroupID, &lab.StepCount, &lab.CreatedAt); err != nil {
		return nil, err
	}
	return &lab, nil
}

func scanLabStep(row pgx.Row) (*LabStep, error) {
	var step LabStep
	if err := row.Scan(&step.ID, &step.LabID, &step.Title, &step.Description, &step.InitialItems, &step.HasAnswer, &step.Answer, &step.OrderIndex); err != nil {
		return nil, err
	}
	return &step, nil
}
