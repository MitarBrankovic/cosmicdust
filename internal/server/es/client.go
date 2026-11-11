// Package es is the little of the Elasticsearch REST API the service uses:
// bulk indexing, index templates, data streams and their lifecycle. A client
// of its own keeps the dependency small, and every request is one the
// service's tests can answer with a fake server.
package es

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to one Elasticsearch cluster.
type Client struct {
	base               string
	http               *http.Client
	username, password string
	apiKey             string
}

// Options are the client's credentials and timeout.
type Options struct {
	Username, Password string        // basic authentication
	APIKey             string        // an API key instead, base64 "id:key"
	Timeout            time.Duration // per request, default 60 s
}

// New returns a client for addr, e.g. "http://elasticsearch:9200".
func New(addr string, o Options) (*Client, error) {
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("es: bad address %q", addr)
	}
	if o.Timeout <= 0 {
		o.Timeout = time.Minute
	}
	return &Client{base: strings.TrimRight(addr, "/"), http: &http.Client{Timeout: o.Timeout},
		username: o.Username, password: o.Password, apiKey: o.APIKey}, nil
}

// Error is an error answer of Elasticsearch.
type Error struct {
	Status int
	Type   string
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("es: %d %s: %s", e.Status, e.Type, e.Reason)
}

// IsType reports whether err is an Elasticsearch error of the given type.
func IsType(err error, typ string) bool {
	var e *Error
	return errors.As(err, &e) && e.Type == typ
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string, out any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
	switch {
	case c.apiKey != "":
		req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	case c.username != "":
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("es: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		e := &Error{Status: resp.StatusCode, Reason: strings.TrimSpace(string(data))}
		var parsed struct {
			Error struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &parsed) == nil && parsed.Error.Type != "" {
			e.Type, e.Reason = parsed.Error.Type, parsed.Error.Reason
		}
		return e
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) put(ctx context.Context, path string, v any) error {
	var body []byte
	if v != nil {
		var err error
		if body, err = json.Marshal(v); err != nil {
			return err
		}
	}
	return c.do(ctx, http.MethodPut, path, body, "application/json", nil)
}

// Version returns the cluster's version: a ping.
func (c *Client) Version(ctx context.Context) (string, error) {
	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := c.do(ctx, http.MethodGet, "/", nil, "", &info); err != nil {
		return "", err
	}
	return info.Version.Number, nil
}

// PutIndexTemplate creates or replaces a composable index template.
func (c *Client) PutIndexTemplate(ctx context.Context, name string, template any) error {
	return c.put(ctx, "/_index_template/"+url.PathEscape(name), template)
}

// CreateDataStream creates a data stream; one that exists is no error.
func (c *Client) CreateDataStream(ctx context.Context, name string) error {
	err := c.put(ctx, "/_data_stream/"+url.PathEscape(name), nil)
	if IsType(err, "resource_already_exists_exception") {
		return nil
	}
	return err
}

// PutLifecycle sets how long a data stream keeps its data: the data stream
// lifecycle deletes whole backing indices once they are older.
func (c *Client) PutLifecycle(ctx context.Context, name string, retention time.Duration) error {
	return c.put(ctx, "/_data_stream/"+url.PathEscape(name)+"/_lifecycle",
		map[string]string{"data_retention": FormatDuration(retention)})
}

// DataStream is a data stream as Elasticsearch describes it.
type DataStream struct {
	Name      string `json:"name"`
	Lifecycle *struct {
		Enabled       bool   `json:"enabled"`
		DataRetention string `json:"data_retention"`
	} `json:"lifecycle"`
	Indices []struct {
		Name string `json:"index_name"`
	} `json:"indices"`
}

// DataStreams lists the data streams matching pattern, e.g. "cosmicdust-*".
func (c *Client) DataStreams(ctx context.Context, pattern string) ([]DataStream, error) {
	var out struct {
		DataStreams []DataStream `json:"data_streams"`
	}
	err := c.do(ctx, http.MethodGet, "/_data_stream/"+url.PathEscape(pattern)+"?expand_wildcards=all", nil, "", &out)
	if IsType(err, "index_not_found_exception") {
		return nil, nil
	}
	return out.DataStreams, err
}

// DeleteDataStreams deletes the data streams matching pattern, with their
// data. For tests.
func (c *Client) DeleteDataStreams(ctx context.Context, pattern string) error {
	err := c.do(ctx, http.MethodDelete, "/_data_stream/"+url.PathEscape(pattern), nil, "", nil)
	if IsType(err, "index_not_found_exception") {
		return nil
	}
	return err
}

// DeleteIndexTemplates deletes the index templates matching pattern. For
// tests.
func (c *Client) DeleteIndexTemplates(ctx context.Context, pattern string) error {
	err := c.do(ctx, http.MethodDelete, "/_index_template/"+url.PathEscape(pattern), nil, "", nil)
	if IsType(err, "resource_not_found_exception") || IsType(err, "index_template_missing_exception") {
		return nil
	}
	return err
}

// BulkItem is the answer to one action of a bulk request.
type BulkItem struct {
	Status int
	Type   string // the error's type, if any
	Reason string
}

// Bulk sends a bulk request: newline-delimited actions and documents. It
// fails only if the request as a whole failed; the items say how each action
// went, in order.
func (c *Client) Bulk(ctx context.Context, body []byte) ([]BulkItem, error) {
	var out struct {
		Items []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := c.do(ctx, http.MethodPost, "/_bulk", body, "application/x-ndjson", &out); err != nil {
		return nil, err
	}
	items := make([]BulkItem, len(out.Items))
	for i, it := range out.Items {
		for _, r := range it { // one key: the action
			items[i].Status = r.Status
			if r.Error != nil {
				items[i].Type, items[i].Reason = r.Error.Type, r.Error.Reason
			}
		}
	}
	return items, nil
}

// Refresh makes what was indexed into the matching indices searchable now.
// For tests.
func (c *Client) Refresh(ctx context.Context, pattern string) error {
	return c.do(ctx, http.MethodPost, "/"+url.PathEscape(pattern)+"/_refresh", nil, "", nil)
}

// Count counts the documents of the matching indices, optionally only those
// matching a query string ("" for all).
func (c *Client) Count(ctx context.Context, pattern, query string) (int64, error) {
	path := "/" + url.PathEscape(pattern) + "/_count"
	if query != "" {
		path += "?q=" + url.QueryEscape(query)
	}
	var out struct {
		Count int64 `json:"count"`
	}
	err := c.do(ctx, http.MethodGet, path, nil, "", &out)
	if IsType(err, "index_not_found_exception") {
		return 0, nil
	}
	return out.Count, err
}

// FormatDuration writes a duration as Elasticsearch time units take it.
func FormatDuration(d time.Duration) string {
	for _, u := range []struct {
		unit string
		d    time.Duration
	}{{"d", 24 * time.Hour}, {"h", time.Hour}, {"m", time.Minute}, {"s", time.Second}} {
		if d >= u.d && d%u.d == 0 {
			return fmt.Sprintf("%d%s", d/u.d, u.unit)
		}
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}
