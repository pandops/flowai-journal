package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
	_ "time/tzdata" // operator zones resolve without the host's zoneinfo
)

// DashboardQuery is a validated-shape dashboard selection. Empty strings
// mean "omitted"; TimeZones carries every supplied time_zone value so a
// duplicate can be rejected.
type DashboardQuery struct {
	Period    string
	Week      string
	Month     string
	TimeZones []string
	HasZone   bool
}

// Dashboard is the ui.dashboard reply body.
type Dashboard struct {
	Period      string            `json:"period"`
	CalendarKey string            `json:"calendar_key"`
	TimeZone    string            `json:"time_zone"`
	Counts      StateCounts       `json:"counts"`
	Buckets     []DashboardBucket `json:"buckets"`
}

// DashboardBucket is one civil day of the selected calendar.
type DashboardBucket struct {
	At         *string     `json:"at"`
	Key        string      `json:"key"`
	Label      string      `json:"label"`
	ShortLabel string      `json:"shortLabel"`
	Total      int         `json:"total"`
	Counts     StateCounts `json:"counts"`
	IsFuture   bool        `json:"isFuture"`
}

// StateCounts are the seven visible task states.
type StateCounts struct {
	Queued    int `json:"queued"`
	Pending   int `json:"pending"`
	Created   int `json:"created"`
	Running   int `json:"running"`
	Finished  int `json:"finished"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
}

func (c *StateCounts) add(state string) bool {
	switch state {
	case "queued":
		c.Queued++
	case "pending":
		c.Pending++
	case "created":
		c.Created++
	case "running":
		c.Running++
	case "finished":
		c.Finished++
	case "failed":
		c.Failed++
	case "cancelled":
		c.Cancelled++
	default:
		return false
	}
	return true
}

// TaskEvents returns the ordered event history of one task of the team.
func (e *Engine) TaskEvents(ctx context.Context, teamID, taskID, afterID int64, limit int) ([]Event, error) {
	ok, err := e.store.TaskExists(ctx, teamID, taskID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return e.store.ListEvents(ctx, EventQuery{TeamID: teamID, TaskID: &taskID, AfterID: afterID, Limit: limit})
}

// TaskLogs returns accepted log chunks after afterOffset.
func (e *Engine) TaskLogs(ctx context.Context, teamID, taskID, afterOffset int64, limit int) ([]LogEntry, error) {
	ok, err := e.store.TaskExists(ctx, teamID, taskID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return e.store.ListLogs(ctx, teamID, taskID, afterOffset, limit)
}

// ControlEvents returns the events of one control of a task.
func (e *Engine) ControlEvents(ctx context.Context, teamID, taskID, controlID, afterID int64, limit int) ([]Event, error) {
	ok, err := e.store.ControlExists(ctx, teamID, taskID, controlID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return e.store.ListEvents(ctx, EventQuery{TeamID: teamID, TaskID: &taskID, ControlID: &controlID, AfterID: afterID, Limit: limit})
}

// ExecutorEvents returns the event history of one team Executor.
func (e *Engine) ExecutorEvents(ctx context.Context, teamID, executorID, afterID int64, limit int) ([]Event, error) {
	rec, ok, err := e.store.ExecutorByID(ctx, executorID)
	if err != nil {
		return nil, err
	}
	if !ok || !rec.Registered || rec.TeamID != teamID {
		return nil, ErrNotFound
	}
	return e.store.ListEvents(ctx, EventQuery{TeamID: teamID, ExecutorID: &executorID, ExecutorHistory: true, AfterID: afterID, Limit: limit})
}

// ExecutorStates returns the team's Executor list with served states.
func (e *Engine) ExecutorStates(ctx context.Context, teamID int64) ([]ExecutorRecord, error) {
	return e.store.ListExecutors(ctx, teamID)
}

// Frames returns the team's frames with frame_id > afterFrameID.
func (e *Engine) Frames(ctx context.Context, teamID, afterFrameID int64, taskID *int64, limit int) ([]Frame, error) {
	return e.store.ListFrames(ctx, teamID, afterFrameID, taskID, limit)
}

// MaxFrameID returns the team's latest frame_id (0 when none).
func (e *Engine) MaxFrameID(ctx context.Context, teamID int64) (int64, error) {
	return e.store.MaxFrameID(ctx, teamID)
}

var zoneName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*$`)

// ResolveZone validates an IANA zone name (UTC when omitted).
func ResolveZone(q DashboardQuery) (*time.Location, string, error) {
	if !q.HasZone {
		return time.UTC, "UTC", nil
	}
	if len(q.TimeZones) != 1 {
		return nil, "", ErrInvalidTimeZone
	}
	name := q.TimeZones[0]
	if !zoneName.MatchString(name) || name == "Local" || len(name) > 64 {
		return nil, "", ErrInvalidTimeZone
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, "", ErrInvalidTimeZone
	}
	return loc, name, nil
}

type window struct {
	loc    *time.Location
	period string
	key    string
	start  time.Time // civil first day (UTC date carrier)
	end    time.Time // civil day after the last
}

func isoWeekStart(year, week int) time.Time {
	jan4 := time.Date(year, time.January, 4, 0, 0, 0, 0, time.UTC)
	return jan4.AddDate(0, 0, -(int(jan4.Weekday())+6)%7+(week-1)*7)
}

func selectWindow(q DashboardQuery, loc *time.Location, now time.Time) (window, error) {
	local := now.In(loc)
	today := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
	switch q.Period {
	case "week":
		if q.Month != "" {
			return window{}, ErrInvalidCalendar
		}
		cy, cw := today.ISOWeek()
		key := q.Week
		if key == "" {
			key = fmt.Sprintf("%04d-W%02d", cy, cw)
		}
		var year, week int
		if _, err := fmt.Sscanf(key, "%04d-W%02d", &year, &week); err != nil || fmt.Sprintf("%04d-W%02d", year, week) != key {
			return window{}, ErrInvalidCalendar
		}
		start := isoWeekStart(year, week)
		if y, w := start.ISOWeek(); y != year || w != week || start.After(isoWeekStart(cy, cw)) {
			return window{}, ErrInvalidCalendar
		}
		return window{loc: loc, period: "week", key: key, start: start, end: start.AddDate(0, 0, 7)}, nil
	case "month":
		if q.Week != "" {
			return window{}, ErrInvalidCalendar
		}
		current := time.Date(today.Year(), today.Month(), 1, 0, 0, 0, 0, time.UTC)
		key := q.Month
		if key == "" {
			key = current.Format("2006-01")
		}
		start, err := time.ParseInLocation("2006-01", key, time.UTC)
		if err != nil || start.Format("2006-01") != key || start.After(current) {
			return window{}, ErrInvalidCalendar
		}
		return window{loc: loc, period: "month", key: key, start: start, end: start.AddDate(0, 1, 0)}, nil
	}
	return window{}, ErrInvalidPeriod
}

// civilDateBounds intersects the civil date with each zone-offset interval,
// so skipped dates have no instants and repeated hours keep both sides.
func civilDateBounds(civil time.Time, loc *time.Location) (time.Time, time.Time, bool) {
	next := civil.AddDate(0, 0, 1)
	from, to := civil.Add(-48*time.Hour), next.Add(48*time.Hour)
	var first, last time.Time
	found := false
	for at := from; at.Before(to); {
		local := at.In(loc)
		_, offset := local.Zone()
		_, end := local.ZoneBounds()
		if end.IsZero() || end.After(to) {
			end = to
		}
		start := civil.Add(-time.Duration(offset) * time.Second)
		stop := next.Add(-time.Duration(offset) * time.Second)
		if start.Before(at) {
			start = at
		}
		if stop.After(end) {
			stop = end
		}
		if start.Before(stop) {
			if !found || start.Before(first) {
				first = start
			}
			if !found || stop.After(last) {
				last = stop
			}
			found = true
		}
		at = end
	}
	return first.UTC(), last.UTC(), found
}

// Dashboard returns the selected calendar of the team. Zone and selection
// are validated before any read.
func (e *Engine) Dashboard(ctx context.Context, teamID int64, q DashboardQuery) (Dashboard, error) {
	loc, zone, err := ResolveZone(q)
	if err != nil {
		return Dashboard{}, err
	}
	now := e.opts.Now()
	w, err := selectWindow(q, loc, now)
	if err != nil {
		return Dashboard{}, err
	}
	local := now.In(loc)
	todayKey := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC).Format(time.DateOnly)
	weekdays := [...]string{"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
	d := Dashboard{Period: w.period, CalendarKey: w.key, TimeZone: zone, Buckets: []DashboardBucket{}}
	index := map[string]int{}
	var from, to time.Time
	found := false
	for day := w.start; day.Before(w.end); day = day.AddDate(0, 0, 1) {
		key := day.Format(time.DateOnly)
		b := DashboardBucket{Key: key, IsFuture: key > todayKey}
		if w.period == "week" {
			b.Label, b.ShortLabel = weekdays[len(d.Buckets)], weekdays[len(d.Buckets)]
		} else {
			b.Label, b.ShortLabel = day.Format("Jan 2"), fmt.Sprint(day.Day())
		}
		if start, end, ok := civilDateBounds(day, loc); ok {
			at := FormatTime(start)
			b.At = &at
			if !found || start.Before(from) {
				from = start
			}
			if !found || end.After(to) {
				to = end
			}
			found = true
		}
		index[key] = len(d.Buckets)
		d.Buckets = append(d.Buckets, b)
	}
	if !found {
		return d, nil
	}
	tasks, err := e.store.ListTasks(ctx, teamID, from, to)
	if err != nil {
		return Dashboard{}, err
	}
	for _, t := range tasks {
		if t.FirstQueuedAt == nil {
			continue
		}
		i, ok := index[t.FirstQueuedAt.In(loc).Format(time.DateOnly)]
		if !ok || d.Buckets[i].At == nil {
			continue
		}
		if d.Buckets[i].Counts.add(t.LatestKind) {
			d.Buckets[i].Total++
			d.Counts.add(t.LatestKind)
		}
	}
	return d, nil
}

// IsSelectionError reports whether err is a dashboard selection error.
func IsSelectionError(err error) bool {
	return errors.Is(err, ErrInvalidPeriod) || errors.Is(err, ErrInvalidCalendar) || errors.Is(err, ErrInvalidTimeZone)
}
