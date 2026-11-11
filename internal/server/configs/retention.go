package configs

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Window is how long one table of one application is kept: Search in
// Elasticsearch, Archive in the Parquet archive. The archive is the long,
// cheap copy; the index the short one people search.
type Window struct {
	Search  Duration `yaml:"search"`
	Archive Duration `yaml:"archive"`
}

// Policy is the retention of one application, per table.
type Policy struct {
	Logs   Window `yaml:"logs"`
	Events Window `yaml:"events"`
}

// Retention says how long the logs and events of each application are kept.
// An application that is not listed gets Default, and so does every field an
// application leaves out. Urgent is how long the index keeps the copies of
// urgent events, which also arrive with the events table.
type Retention struct {
	Default Policy            `yaml:"default"`
	Urgent  Duration          `yaml:"urgent"`
	Apps    map[string]Policy `yaml:"apps"`
}

// DefaultRetention is used without a retention file.
func DefaultRetention() *Retention {
	return &Retention{
		Default: Policy{
			Logs:   Window{Search: Days(7), Archive: Days(30)},
			Events: Window{Search: Days(30), Archive: Days(90)},
		},
		Urgent: Days(7),
	}
}

// LoadRetention reads a retention file. What it leaves out of the default
// comes from DefaultRetention.
func LoadRetention(path string) (*Retention, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := &Retention{}
	if err := yaml.Unmarshal(data, r); err != nil {
		return nil, fmt.Errorf("retention file %s: %w", path, err)
	}
	def := DefaultRetention()
	r.Default.Logs = r.Default.Logs.or(def.Default.Logs)
	r.Default.Events = r.Default.Events.or(def.Default.Events)
	if r.Urgent == 0 {
		r.Urgent = def.Urgent
	}
	for app, p := range r.Apps {
		if app == "" {
			return nil, fmt.Errorf("retention file %s: an application without a name", path)
		}
		if p.Logs.Search < 0 || p.Logs.Archive < 0 || p.Events.Search < 0 || p.Events.Archive < 0 {
			return nil, fmt.Errorf("retention file %s: a negative period for %q", path, app)
		}
	}
	return r, nil
}

func (w Window) or(def Window) Window {
	if w.Search <= 0 {
		w.Search = def.Search
	}
	if w.Archive <= 0 {
		w.Archive = def.Archive
	}
	return w
}

// For returns the window of an application's table ("logs" or "events").
func (r *Retention) For(app, table string) Window {
	p, def := r.Apps[app], r.Default
	if table == "events" {
		return p.Events.or(def.Events)
	}
	return p.Logs.or(def.Logs)
}

// Longest is the longest any archive keeps anything: how long a received file
// has to be remembered for a second upload of it to be recognised.
func (r *Retention) Longest() time.Duration {
	longest := max(r.Default.Logs.Archive, r.Default.Events.Archive)
	for app := range r.Apps {
		longest = max(longest, r.For(app, "logs").Archive, r.For(app, "events").Archive)
	}
	return time.Duration(longest)
}

// Duration is a time.Duration that also takes days: "30d", "12h", "90m".
type Duration time.Duration

// Days is n days.
func Days(n int) Duration { return Duration(time.Duration(n) * 24 * time.Hour) }

// D is the duration as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalText(text []byte) error {
	v, err := ParseDuration(string(text))
	*d = Duration(v)
	return err
}

func (d Duration) String() string { return FormatDuration(time.Duration(d)) }

// ParseDuration parses "7d" as seven days, and anything else as
// time.ParseDuration does.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, errors.New("negative duration " + s)
	}
	return d, nil
}

// FormatDuration writes whole days as "7d" and the rest as Go does.
func FormatDuration(d time.Duration) string {
	if d > 0 && d%(24*time.Hour) == 0 {
		return strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	}
	return d.String()
}
