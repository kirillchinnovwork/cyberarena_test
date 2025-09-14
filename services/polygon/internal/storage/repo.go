package storage

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repo struct{ pool *pgxpool.Pool }

var (
	ErrUserAlreadyInTeam = errors.New("user already in a team")
)

func NewRepo(p *pgxpool.Pool) *Repo { return &Repo{pool: p} }

func (r *Repo) Migrate(ctx context.Context) error {
	stmts := []string{
		`create table if not exists teams(
			id uuid primary key,
			name text not null,
			type smallint not null,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		);`,
		`create table if not exists team_users(
			team_id uuid not null references teams(id) on delete cascade,
			user_id uuid not null,
			primary key(team_id,user_id)
		);`,
		`create unique index if not exists team_users_user_unique on team_users(user_id);`,
		`create table if not exists polygons(
			id uuid primary key,
			name text not null,
			description text not null,
			cover_url text,
			cover_key text,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		);`,
		`create table if not exists incidents(
			id uuid primary key,
			polygon_id uuid not null references polygons(id) on delete cascade,
			name text not null,
			description text not null,
			base_prize bigint not null default 0,
			blue_share_percent int not null default 0,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		);`,
		`create table if not exists reports(
			id uuid primary key,
			incident_id uuid not null references incidents(id) on delete cascade,
			team_id uuid not null references teams(id) on delete cascade,
			status smallint not null,
			rejection_reason text,
			time int not null default 0,
			created_at timestamptz not null default now(),
			updated_at timestamptz not null default now()
		);`,
		`create table if not exists report_steps(
			id uuid primary key,
			report_id uuid not null references reports(id) on delete cascade,
			number int not null,
			name text,
			time int,
			description text,
			target text,
			source text,
			result text
		);`,
		`create table if not exists report_attachments(
			id uuid primary key,
			report_id uuid not null references reports(id) on delete cascade,
			url text not null,
			object_key text not null,
			content_type text not null,
			size bigint not null,
			created_at timestamptz not null default now()
		);`,
		`create table if not exists initial_items(
			id uuid primary key,
			name text not null,
			description text not null,
			files_urls text[] not null default '{}',
			user_id uuid null -- если null, элемент виден всем
			,created_at timestamptz not null default now()
			,updated_at timestamptz not null default now()
		);`,
		`alter table initial_items add column if not exists user_id uuid null;`,
		`alter table initial_items add column if not exists created_at timestamptz not null default now();`,
		`alter table initial_items add column if not exists updated_at timestamptz not null default now();`,
		// новые/эволюционные изменения
		`alter table teams add column if not exists polygon_id uuid null references polygons(id) on delete set null;`,
	}
	for _, s := range stmts {
		if _, err := r.pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repo) CreateTeam(ctx context.Context, id uuid.UUID, name string, t int32) error {
	_, err := r.pool.Exec(ctx, `insert into teams(id,name,type) values ($1,$2,$3)`, id, name, t)
	return err
}
func (r *Repo) UpdateTeam(ctx context.Context, id uuid.UUID, name string, t *int32) error {
	sets := []string{}
	args := []any{}
	idx := 1
	if name != "" {
		sets = append(sets, "name=$"+strconv.Itoa(idx))
		args = append(args, name)
		idx++
	}
	if t != nil {
		sets = append(sets, "type=$"+strconv.Itoa(idx))
		args = append(args, *t)
		idx++
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	q := "update teams set " + strings.Join(sets, ",") + ", updated_at=now() where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) DeleteTeam(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from teams where id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) AddUserToTeam(ctx context.Context, teamID, userID uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `insert into team_users(team_id,user_id) values ($1,$2)`, teamID, userID)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return ErrUserAlreadyInTeam
		}
		return err
	}
	return nil
}
func (r *Repo) RemoveUserFromTeam(ctx context.Context, teamID, userID uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from team_users where team_id=$1 and user_id=$2`, teamID, userID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (r *Repo) GetTeam(ctx context.Context, id uuid.UUID) (*Team, error) {
	row := r.pool.QueryRow(ctx, `select id, name, type from teams where id=$1`, id)
	var t Team
	if err := row.Scan(&t.ID, &t.Name, &t.Type); err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *Repo) ListTeams(ctx context.Context) ([]Team, error) {
	rows, err := r.pool.Query(ctx, `select id, name, type from teams order by created_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Team
	for rows.Next() {
		var t Team
		if err := rows.Scan(&t.ID, &t.Name, &t.Type); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, rows.Err()
}

type TeamWithUsers struct {
	Team
	UserIDs []uuid.UUID
}

func (r *Repo) ListTeamsWithUsers(ctx context.Context) ([]TeamWithUsers, error) {
	rows, err := r.pool.Query(ctx, `select t.id, t.name, t.type, coalesce(array_agg(tu.user_id) filter (where tu.user_id is not null), '{}')
		from teams t left join team_users tu on tu.team_id=t.id
		group by t.id, t.name, t.type
		order by t.created_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []TeamWithUsers
	for rows.Next() {
		var t TeamWithUsers
		var arr []uuid.UUID
		if err := rows.Scan(&t.ID, &t.Name, &t.Type, &arr); err != nil {
			return nil, err
		}
		t.UserIDs = arr
		res = append(res, t)
	}
	return res, rows.Err()
}

func (r *Repo) CreatePolygon(ctx context.Context, id uuid.UUID, name, description, coverURL, coverKey string) error {
	_, err := r.pool.Exec(ctx, `insert into polygons(id,name,description,cover_url,cover_key) values ($1,$2,$3,$4,$5)`, id, name, description, coverURL, coverKey)
	return err
}
func (r *Repo) UpdatePolygon(ctx context.Context, id uuid.UUID, name, description, coverURL, coverKey *string) error {
	sets := []string{}
	args := []any{}
	idx := 1
	if name != nil && *name != "" {
		sets = append(sets, "name=$"+strconv.Itoa(idx))
		args = append(args, *name)
		idx++
	}
	if description != nil && *description != "" {
		sets = append(sets, "description=$"+strconv.Itoa(idx))
		args = append(args, *description)
		idx++
	}
	if coverURL != nil {
		sets = append(sets, "cover_url=$"+strconv.Itoa(idx))
		args = append(args, *coverURL)
		idx++
	}
	if coverKey != nil {
		sets = append(sets, "cover_key=$"+strconv.Itoa(idx))
		args = append(args, *coverKey)
		idx++
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	q := "update polygons set " + strings.Join(sets, ",") + ", updated_at=now() where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// SetTeamPolygon устанавливает polygon_id для команды (можно передать uuid.Nil чтобы отвязать)
func (r *Repo) SetTeamPolygon(ctx context.Context, teamID, polygonID uuid.UUID) error {
	var pid *uuid.UUID
	if polygonID != uuid.Nil {
		pid = &polygonID
	}
	_, err := r.pool.Exec(ctx, `update teams set polygon_id=$2, updated_at=now() where id=$1`, teamID, pid)
	return err
}
func (r *Repo) DeletePolygon(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from polygons where id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) GetPolygon(ctx context.Context, id uuid.UUID) (*Polygon, error) {
	row := r.pool.QueryRow(ctx, `select id, name, description, coalesce(cover_url,''), coalesce(cover_key,'') from polygons where id=$1`, id)
	var p Polygon
	if err := row.Scan(&p.ID, &p.Name, &p.Description, &p.CoverURL, &p.CoverKey); err != nil {
		return nil, err
	}
	return &p, nil
}

// FindBlueTeamByPolygon возвращает синюю команду, привязанную к полигону (teams.polygon_id=id AND type=BLUE)
func (r *Repo) FindBlueTeamByPolygon(ctx context.Context, polygonID uuid.UUID) (*Team, error) {
	row := r.pool.QueryRow(ctx, `select id, name, type from teams where polygon_id=$1 and type=1 limit 1`, polygonID)
	var t Team
	if err := row.Scan(&t.ID, &t.Name, &t.Type); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

func (r *Repo) CreateIncident(ctx context.Context, id, polygonID uuid.UUID, name, description string) error {
	_, err := r.pool.Exec(ctx, `insert into incidents(id,polygon_id,name,description) values ($1,$2,$3,$4)`, id, polygonID, name, description)
	return err
}
func (r *Repo) UpdateIncident(ctx context.Context, id uuid.UUID, name, description *string) error {
	sets := []string{}
	args := []any{}
	idx := 1
	if name != nil && *name != "" {
		sets = append(sets, "name=$"+strconv.Itoa(idx))
		args = append(args, *name)
		idx++
	}
	if description != nil && *description != "" {
		sets = append(sets, "description=$"+strconv.Itoa(idx))
		args = append(args, *description)
		idx++
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	q := "update incidents set " + strings.Join(sets, ",") + ", updated_at=now() where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) DeleteIncident(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from incidents where id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) GetIncident(ctx context.Context, id uuid.UUID) (*Incident, error) {
	row := r.pool.QueryRow(ctx, `select id, name, description from incidents where id=$1`, id)
	var in Incident
	if err := row.Scan(&in.ID, &in.Name, &in.Description); err != nil {
		return nil, err
	}
	return &in, nil
}

func (r *Repo) InsertReport(ctx context.Context, id, incidentID, teamID uuid.UUID, status int32, reportTime int32) error {
	_, err := r.pool.Exec(ctx, `insert into reports(id,incident_id,team_id,status,time) values ($1,$2,$3,$4,$5)`, id, incidentID, teamID, status, reportTime)
	return err
}
func (r *Repo) InsertReportSteps(ctx context.Context, reportID uuid.UUID, steps []ReportStep) error {
	batch := &pgx.Batch{}
	for _, s := range steps {
		batch.Queue(`insert into report_steps(id,report_id,number,name,time,description,target,source,result) values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.ID, reportID, s.Number, s.Name, s.Time, s.Description, s.Target, s.Source, s.Result)
	}
	br := r.pool.SendBatch(ctx, batch)
	return br.Close()
}
func (r *Repo) GetReport(ctx context.Context, id uuid.UUID) (*Report, error) {
	row := r.pool.QueryRow(ctx, `select id, incident_id, team_id, status, coalesce(rejection_reason,''), time, created_at, updated_at from reports where id=$1`, id)
	var rp Report
	if err := row.Scan(&rp.ID, &rp.IncidentID, &rp.TeamID, &rp.Status, &rp.RejectionReason, &rp.Time, &rp.CreatedAt, &rp.UpdatedAt); err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `select id, number, coalesce(name,''), coalesce(time,0), coalesce(description,''), coalesce(target,''), coalesce(source,''), coalesce(result,'') from report_steps where report_id=$1 order by number`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var s ReportStep
		if err := rows.Scan(&s.ID, &s.Number, &s.Name, &s.Time, &s.Description, &s.Target, &s.Source, &s.Result); err != nil {
			return nil, err
		}
		rp.Steps = append(rp.Steps, s)
	}
	return &rp, rows.Err()
}

func (r *Repo) ListTeamReports(ctx context.Context, teamID uuid.UUID) ([]Report, error) {
	rows, err := r.pool.Query(ctx, `select id, incident_id, team_id, status, coalesce(rejection_reason,''), time, created_at, updated_at from reports where team_id=$1 order by created_at desc`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Report
	for rows.Next() {
		var rp Report
		if err := rows.Scan(&rp.ID, &rp.IncidentID, &rp.TeamID, &rp.Status, &rp.RejectionReason, &rp.Time, &rp.CreatedAt, &rp.UpdatedAt); err != nil {
			return nil, err
		}
		stRows, err := r.pool.Query(ctx, `select id, number, coalesce(name,''), coalesce(time,0), coalesce(description,''), coalesce(target,''), coalesce(source,''), coalesce(result,'') from report_steps where report_id=$1 order by number`, rp.ID)
		if err != nil {
			return nil, err
		}
		for stRows.Next() {
			var s ReportStep
			if err := stRows.Scan(&s.ID, &s.Number, &s.Name, &s.Time, &s.Description, &s.Target, &s.Source, &s.Result); err != nil {
				stRows.Close()
				return nil, err
			}
			rp.Steps = append(rp.Steps, s)
		}
		stRows.Close()
		if err := stRows.Err(); err != nil {
			return nil, err
		}
		res = append(res, rp)
	}
	return res, rows.Err()
}

func (r *Repo) GetTeamIncidentReport(ctx context.Context, incidentID, teamID uuid.UUID) (*Report, error) {
	row := r.pool.QueryRow(ctx, `select id from reports where incident_id=$1 and team_id=$2 order by created_at desc limit 1`, incidentID, teamID)
	var rid uuid.UUID
	if err := row.Scan(&rid); err != nil {
		return nil, err
	}
	return r.GetReport(ctx, rid)
}
func (r *Repo) UpdateReportStatus(ctx context.Context, id uuid.UUID, status int32, reason *string) error {
	var err error
	if reason != nil {
		_, err = r.pool.Exec(ctx, `update reports set status=$2, rejection_reason=$3, updated_at=now() where id=$1`, id, status, *reason)
	} else {
		_, err = r.pool.Exec(ctx, `update reports set status=$2, updated_at=now(), rejection_reason=null where id=$1`, id, status)
	}
	return err
}
func (r *Repo) ReplaceReportSteps(ctx context.Context, reportID uuid.UUID, steps []ReportStep) error {
	_, err := r.pool.Exec(ctx, `delete from report_steps where report_id=$1`, reportID)
	if err != nil {
		return err
	}
	return r.InsertReportSteps(ctx, reportID, steps)
}
func (r *Repo) ReportExistsForTeam(ctx context.Context, incidentID, teamID uuid.UUID) (bool, uuid.UUID, error) {
	row := r.pool.QueryRow(ctx, `select id from reports where incident_id=$1 and team_id=$2 order by created_at desc limit 1`, incidentID, teamID)
	var id uuid.UUID
	err := row.Scan(&id)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, uuid.UUID{}, nil
		}
		return false, uuid.UUID{}, err
	}
	return true, id, nil
}
func (r *Repo) UpdateReportForEdit(ctx context.Context, id uuid.UUID, status int32) error {
	_, err := r.pool.Exec(ctx, `update reports set status=$2, rejection_reason=null, updated_at=now() where id=$1`, id, status)
	return err
}
func (r *Repo) ListReportAttachments(ctx context.Context, reportID uuid.UUID) ([]Attachment, error) {
	rows, err := r.pool.Query(ctx, `select id, report_id, url, object_key, content_type, size from report_attachments where report_id=$1`, reportID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Attachment
	for rows.Next() {
		var a Attachment
		if err := rows.Scan(&a.ID, &a.ReportID, &a.URL, &a.ObjectKey, &a.ContentType, &a.Size); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, rows.Err()
}

func (r *Repo) InsertAttachment(ctx context.Context, id, reportID uuid.UUID, url, objectKey, contentType string, size int64) error {
	_, err := r.pool.Exec(ctx, `insert into report_attachments(id,report_id,url,object_key,content_type,size) values ($1,$2,$3,$4,$5,$6)`, id, reportID, url, objectKey, contentType, size)
	return err
}
func (r *Repo) GetAttachment(ctx context.Context, id uuid.UUID) (*Attachment, error) {
	row := r.pool.QueryRow(ctx, `select id, report_id, url, object_key, content_type, size from report_attachments where id=$1`, id)
	var a Attachment
	if err := row.Scan(&a.ID, &a.ReportID, &a.URL, &a.ObjectKey, &a.ContentType, &a.Size); err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *Repo) ListPolygonsWithIncidents(ctx context.Context) ([]PolygonWithIncidents, error) {
	rows, err := r.pool.Query(ctx, `select p.id, p.name, p.description, coalesce(p.cover_url,'') from polygons p order by p.created_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	polys := []PolygonWithIncidents{}
	for rows.Next() {
		var p PolygonWithIncidents
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.CoverURL); err != nil {
			return nil, err
		}
		polys = append(polys, p)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	for i := range polys {
		ir, err := r.pool.Query(ctx, `select id, name, description from incidents where polygon_id=$1 order by created_at`, polys[i].ID)
		if err != nil {
			return nil, err
		}
		for ir.Next() {
			var in Incident
			if err := ir.Scan(&in.ID, &in.Name, &in.Description); err != nil {
				ir.Close()
				return nil, err
			}
			polys[i].Incidents = append(polys[i].Incidents, in)
		}
		ir.Close()
	}
	return polys, nil
}
func (r *Repo) ListIncidents(ctx context.Context, polygonID uuid.UUID) ([]Incident, error) {
	rows, err := r.pool.Query(ctx, `select id, name, description from incidents where polygon_id=$1 order by created_at`, polygonID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res := []Incident{}
	for rows.Next() {
		var in Incident
		if err := rows.Scan(&in.ID, &in.Name, &in.Description); err != nil {
			return nil, err
		}
		res = append(res, in)
	}
	return res, rows.Err()
}

func (r *Repo) ListInitialItems(ctx context.Context, userID *uuid.UUID) ([]InitialItem, error) {
	var rows pgx.Rows
	var err error
	if userID != nil {
		rows, err = r.pool.Query(ctx, `select id, name, description, files_urls, user_id from initial_items where user_id is null or user_id=$1 order by name`, *userID)
	} else {
		rows, err = r.pool.Query(ctx, `select id, name, description, files_urls, user_id from initial_items where user_id is null order by name`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []InitialItem
	for rows.Next() {
		var it InitialItem
		if err := rows.Scan(&it.ID, &it.Name, &it.Description, &it.Files, &it.UserID); err != nil {
			return nil, err
		}
		res = append(res, it)
	}
	return res, rows.Err()
}

type Report struct {
	ID              uuid.UUID
	IncidentID      uuid.UUID
	TeamID          uuid.UUID
	Status          int32
	RejectionReason string
	Time            int32
	Steps           []ReportStep
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type ReportStep struct {
	ID          uuid.UUID
	Number      int32
	Name        string
	Time        int32
	Description string
	Target      string
	Source      string
	Result      string
}

type Attachment struct {
	ID          uuid.UUID
	ReportID    uuid.UUID
	URL         string
	ObjectKey   string
	ContentType string
	Size        int64
}

type PolygonWithIncidents struct {
	ID          uuid.UUID
	Name        string
	Description string
	CoverURL    string
	Incidents   []Incident
}

type Incident struct {
	ID          uuid.UUID
	Name        string
	Description string
}

type InitialItem struct {
	ID          uuid.UUID
	Name        string
	Description string
	Files       []string
	UserID      *uuid.UUID
}

type Polygon struct {
	ID                                    uuid.UUID
	Name, Description, CoverURL, CoverKey string
}

type Team struct {
	ID   uuid.UUID
	Name string
	Type int32
}

type LatestReportStatus struct {
	IncidentID uuid.UUID
	TeamID     uuid.UUID
	Status     int32
	TeamType   int32
	CreatedAt  time.Time
}

func (r *Repo) GetLatestReportStatusForTeam(ctx context.Context, incidentID, teamID uuid.UUID) (int32, error) {
	row := r.pool.QueryRow(ctx, `select status from reports where incident_id=$1 and team_id=$2 order by created_at desc limit 1`, incidentID, teamID)
	var st int32
	if err := row.Scan(&st); err != nil {
		return 0, err
	}
	return st, nil
}

// GetLatestReportForTeam возвращает статус и причину отклонения последнего отчёта команды по инциденту.
// Если отчётов нет — возвращает pgx.ErrNoRows.
func (r *Repo) GetLatestReportForTeam(ctx context.Context, incidentID, teamID uuid.UUID) (int32, *string, error) {
	row := r.pool.QueryRow(ctx, `select status, rejection_reason from reports where incident_id=$1 and team_id=$2 order by created_at desc limit 1`, incidentID, teamID)
	var st int32
	var reason *string
	if err := row.Scan(&st, &reason); err != nil {
		return 0, nil, err
	}
	return st, reason, nil
}

func (r *Repo) GetLatestReportStatusesByType(ctx context.Context, incidentIDs []uuid.UUID, teamType int32) ([]LatestReportStatus, error) {
	if len(incidentIDs) == 0 {
		return nil, nil
	}
	params := make([]any, 0, len(incidentIDs)+1)
	placeholders := make([]string, 0, len(incidentIDs))
	for i, id := range incidentIDs {
		params = append(params, id)
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
	}
	params = append(params, teamType)
	q := `select distinct on (r.incident_id, r.team_id) r.incident_id, r.team_id, r.status, t.type, r.created_at
		  from reports r join teams t on t.id = r.team_id
		  where r.incident_id in (` + strings.Join(placeholders, ",") + `) and t.type = $` + strconv.Itoa(len(incidentIDs)+1) + `
		  order by r.incident_id, r.team_id, r.created_at desc`
	rows, err := r.pool.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []LatestReportStatus
	for rows.Next() {
		var lr LatestReportStatus
		if err := rows.Scan(&lr.IncidentID, &lr.TeamID, &lr.Status, &lr.TeamType, &lr.CreatedAt); err != nil {
			return nil, err
		}
		res = append(res, lr)
	}
	return res, rows.Err()
}

func (r *Repo) GetAcceptedReportTeamIDs(ctx context.Context, incidentIDs []uuid.UUID, teamType int32) (map[uuid.UUID][]uuid.UUID, error) {
	res := make(map[uuid.UUID][]uuid.UUID)
	if len(incidentIDs) == 0 {
		return res, nil
	}
	params := make([]any, 0, len(incidentIDs)+2)
	placeholders := make([]string, 0, len(incidentIDs))
	for i, id := range incidentIDs {
		params = append(params, id)
		placeholders = append(placeholders, "$"+strconv.Itoa(i+1))
	}

	params = append(params, int32(2))
	params = append(params, teamType)
	q := `select distinct r.incident_id, r.team_id
		  from reports r join teams t on t.id = r.team_id
		  where r.incident_id in (` + strings.Join(placeholders, ",") + `) and r.status = $` + strconv.Itoa(len(incidentIDs)+1) + ` and t.type = $` + strconv.Itoa(len(incidentIDs)+2)
	rows, err := r.pool.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var incID, teamID uuid.UUID
		if err := rows.Scan(&incID, &teamID); err != nil {
			return nil, err
		}
		res[incID] = append(res[incID], teamID)
	}
	return res, rows.Err()
}

func SumStepTime(steps []ReportStep) int32 {
	var total int32
	for _, s := range steps {
		if s.Time > 0 {
			total += s.Time
		}
	}
	return total
}

func (r *Repo) ListUserTeams(ctx context.Context, userID uuid.UUID) ([]Team, error) {
	rows, err := r.pool.Query(ctx, `select t.id, t.name, t.type from team_users tu join teams t on t.id=tu.team_id where tu.user_id=$1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []Team
	for rows.Next() {
		var t Team
		if err := rows.Scan(&t.ID, &t.Name, &t.Type); err != nil {
			return nil, err
		}
		res = append(res, t)
	}
	return res, rows.Err()
}
func (r *Repo) GetUserTeam(ctx context.Context, userID uuid.UUID) (*Team, error) {
	row := r.pool.QueryRow(ctx, `select t.id, t.name, t.type from team_users tu join teams t on t.id=tu.team_id where tu.user_id=$1 limit 1`, userID)
	var t Team
	if err := row.Scan(&t.ID, &t.Name, &t.Type); err != nil {
		return nil, err
	}
	return &t, nil
}

// GetTeamPolygonID возвращает polygon_id, привязанный к команде (для синих команд)
func (r *Repo) GetTeamPolygonID(ctx context.Context, teamID uuid.UUID) (uuid.UUID, error) {
	row := r.pool.QueryRow(ctx, `select coalesce(polygon_id, '00000000-0000-0000-0000-000000000000') from teams where id=$1`, teamID)
	var pid uuid.UUID
	if err := row.Scan(&pid); err != nil {
		return uuid.Nil, err
	}
	if pid == uuid.Nil {
		return uuid.Nil, nil
	}
	return pid, nil
}

// AcceptedRedReportSummary — краткое представление принятого red отчёта для формирования IncidentBlueView.
type AcceptedRedReportSummary struct {
	ReportID            uuid.UUID
	IncidentID          uuid.UUID
	IncidentName        string
	IncidentDescription string
	TeamID              uuid.UUID
	Time                int32
}

// ListAcceptedRedReports возвращает принятые отчёты красных команд по набору инцидентов.
func (r *Repo) ListAcceptedRedReports(ctx context.Context, incidentIDs []uuid.UUID) ([]AcceptedRedReportSummary, error) {
	if len(incidentIDs) == 0 {
		return nil, nil
	}
	params := make([]any, 0, len(incidentIDs)+2)
	ph := make([]string, 0, len(incidentIDs))
	for i, id := range incidentIDs {
		params = append(params, id)
		ph = append(ph, "$"+strconv.Itoa(i+1))
	}
	// status=2 (ACCEPTED), type=RED(0)
	params = append(params, int32(2))
	params = append(params, int32(0))
	q := `select r.id, r.incident_id, i.name, i.description, r.team_id, r.time
		  from reports r
		  join incidents i on i.id=r.incident_id
		  join teams t on t.id=r.team_id
		  where r.incident_id in (` + strings.Join(ph, ",") + `) and r.status=$` + strconv.Itoa(len(incidentIDs)+1) + ` and t.type=$` + strconv.Itoa(len(incidentIDs)+2) + `
		  order by r.created_at` // можно оптимизировать под нужный порядок
	rows, err := r.pool.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []AcceptedRedReportSummary
	for rows.Next() {
		var a AcceptedRedReportSummary
		if err := rows.Scan(&a.ReportID, &a.IncidentID, &a.IncidentName, &a.IncidentDescription, &a.TeamID, &a.Time); err != nil {
			return nil, err
		}
		res = append(res, a)
	}
	return res, rows.Err()
}

func (r *Repo) CreateInitialItem(ctx context.Context, id uuid.UUID, name, description string, files []string, userID *uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `insert into initial_items(id,name,description,files_urls,user_id) values($1,$2,$3,$4,$5)`, id, name, description, files, userID)
	return err
}
func (r *Repo) UpdateInitialItem(ctx context.Context, id uuid.UUID, name, description *string, files *[]string, userIDSet bool, userID *uuid.UUID) error {
	sets := []string{}
	args := []any{}
	idx := 1
	if name != nil {
		sets = append(sets, "name=$"+strconv.Itoa(idx))
		args = append(args, *name)
		idx++
	}
	if description != nil {
		sets = append(sets, "description=$"+strconv.Itoa(idx))
		args = append(args, *description)
		idx++
	}
	if files != nil {
		sets = append(sets, "files_urls=$"+strconv.Itoa(idx))
		args = append(args, *files)
		idx++
	}
	if userIDSet {
		sets = append(sets, "user_id=$"+strconv.Itoa(idx))
		args = append(args, userID)
		idx++
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, id)
	q := "update initial_items set " + strings.Join(sets, ",") + ", updated_at=now() where id=$" + strconv.Itoa(idx)
	ct, err := r.pool.Exec(ctx, q, args...)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) DeleteInitialItem(ctx context.Context, id uuid.UUID) error {
	ct, err := r.pool.Exec(ctx, `delete from initial_items where id=$1`, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
func (r *Repo) GetInitialItem(ctx context.Context, id uuid.UUID) (*InitialItem, error) {
	row := r.pool.QueryRow(ctx, `select id,name,description,files_urls,user_id from initial_items where id=$1`, id)
	var it InitialItem
	if err := row.Scan(&it.ID, &it.Name, &it.Description, &it.Files, &it.UserID); err != nil {
		return nil, err
	}
	return &it, nil
}
