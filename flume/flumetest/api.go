// Package flumetest is Flume's API for tests: an HTTP server answering
// the routes this module calls, in the envelope and with the examples
// Flume's documentation gives, over a state the test fills in. It records
// what arrived. None of it was checked against Flume's servers.
package flumetest

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"time"
)

// The account a new API accepts.
const (
	ClientID     = "client-id"
	ClientSecret = "client-secret"
	Username     = "owner@example.com"
	Password     = "a password"
)

// The devices a new API holds, the documentation's sensor and a bridge
// for it.
const (
	SensorID = "6721255604737738501"
	BridgeID = "6782193059647718740"
)

const (
	week      = 7 * 24 * time.Hour
	maxBody   = 1 << 20
	codeOK    = 602
	codeEmpty = 610
)

// Request is one request as it arrived.
type Request struct {
	Method string
	Path   string
	Query  url.Values
	// Authorization is the header as sent.
	Authorization string
	// Body is the body as sent.
	Body []byte
}

// API is the fake. A test sets its exported fields between requests; the
// server holds its own lock around them.
type API struct {
	*httptest.Server

	mu sync.Mutex
	// Lifetime is how long a token is granted for; zero is a week, as
	// Flume grants.
	Lifetime time.Duration
	// Devices is the data of the devices route.
	Devices json.RawMessage
	// Results is the query route's answer, each query's readings under
	// its request_id; a query with none there is left out of the answer.
	Results map[string]json.RawMessage
	// Flow is the data of the current flow rate route.
	Flow json.RawMessage
	// Alerts is the data of the usage alerts route.
	Alerts json.RawMessage
	// Fail, when not zero, is the status every route but the token's
	// answers with, in Flume's envelope for an error.
	Fail int
	// Raw, when not nil, is the whole body every route but the token's
	// answers with, under a 200.
	Raw []byte
	// Logins counts the tokens granted.
	Logins int
	// Requests is every request received, the token's among them.
	Requests []Request

	token string
}

// New starts an API holding the documentation's examples. The caller
// closes it.
func New() *API {
	api := &API{
		Devices: json.RawMessage(`[
			{"id": "` + BridgeID + `", "type": 1, "location_id": 44565, "user_id": 1682, "bridge_id": null,
			 "oriented": null, "last_seen": "2021-12-29T05:47:40.000Z", "connected": true,
			 "battery_level": null, "product": "flume2"},
			{"id": "` + SensorID + `", "type": 2, "location_id": 44565, "user_id": 1682,
			 "bridge_id": "` + BridgeID + `", "oriented": true, "last_seen": "2021-12-29T05:47:34.000Z",
			 "connected": true, "battery_level": "medium", "product": "flume2"}]`),
		Results: map[string]json.RawMessage{},
		Flow:    json.RawMessage(`[{"active": true, "gpm": 0.78, "datetime": "2024-01-01 15:22:00"}]`),
		Alerts: json.RawMessage(`[
			{"id": 5, "device_id": "` + SensorID + `", "triggered_datetime": "2017-03-06T10:00:00.000Z",
			 "flume_leak": false, "event_rule_name": "Custom Rule"},
			{"id": 4, "device_id": "` + SensorID + `", "triggered_datetime": "2017-03-06T10:00:00.000Z",
			 "flume_leak": true, "event_rule_name": "Flume Smart Leak Rule"}]`),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /oauth/token", api.grant)
	mux.HandleFunc("GET /me/devices", api.authorized(func(*http.Request, []byte) (json.RawMessage, bool) {
		return api.Devices, true
	}))
	mux.HandleFunc("POST /me/devices/{device}/query", api.authorized(api.query))
	mux.HandleFunc("GET /me/devices/{device}/query/active", api.authorized(
		func(req *http.Request, _ []byte) (json.RawMessage, bool) {
			return api.Flow, req.PathValue("device") == SensorID
		},
	))
	mux.HandleFunc("GET /me/usage-alerts", api.authorized(func(*http.Request, []byte) (json.RawMessage, bool) {
		return api.Alerts, true
	}))

	api.Server = httptest.NewServer(mux)

	return api
}

// Received is the requests so far, and how many tokens were granted.
func (a *API) Received() ([]Request, int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]Request(nil), a.Requests...), a.Logins
}

// record keeps a request and returns its body.
func (a *API) record(req *http.Request) []byte {
	body, _ := io.ReadAll(io.LimitReader(req.Body, maxBody))

	a.Requests = append(a.Requests, Request{
		Method:        req.Method,
		Path:          req.URL.Path,
		Query:         req.URL.Query(),
		Authorization: req.Header.Get("Authorization"),
		Body:          body,
	})

	return body
}

// grant is the token route: a token for the account's password grant,
// Flume's invalid_client for anything else.
func (a *API) grant(writer http.ResponseWriter, req *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var asked map[string]string

	_ = json.Unmarshal(a.record(req), &asked)

	if asked["grant_type"] != "password" || asked["client_id"] != ClientID ||
		asked["client_secret"] != ClientSecret || asked["username"] != Username || asked["password"] != Password {
		refuse(writer, http.StatusBadRequest, 0, "invalid_client", `["Client credentials are invalid"]`)

		return
	}

	lifetime := a.Lifetime
	if lifetime == 0 {
		lifetime = week
	}

	a.Logins++
	a.token = fmt.Sprintf("token-%d", a.Logins)

	answer(writer, json.RawMessage(fmt.Sprintf(
		`[{"token_type": "bearer", "access_token": %q, "expires_in": %d, "refresh_token": "refresh-%d"}]`,
		a.token, int64(lifetime/time.Second), a.Logins,
	)))
}

// authorized wraps a route behind the token last granted, and behind
// Fail and Raw; the route gives its data and whether the record exists.
func (a *API) authorized(route func(*http.Request, []byte) (json.RawMessage, bool)) http.HandlerFunc {
	return func(writer http.ResponseWriter, req *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()

		body := a.record(req)

		switch {
		case a.token == "" || req.Header.Get("Authorization") != "Bearer "+a.token:
			refuse(writer, http.StatusUnauthorized, 0, "invalid_token", `["The access token provided is invalid"]`)
		case a.Fail != 0:
			refuse(writer, a.Fail, 0, http.StatusText(a.Fail), "null")
		case a.Raw != nil:
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write(a.Raw)
		default:
			if data, found := route(req, body); found {
				answer(writer, data)
			} else {
				refuse(writer, http.StatusNotFound, codeEmpty, "Record could not be found", "null")
			}
		}
	}
}

// askedQuery is a query as far as the fake reads it.
type askedQuery struct {
	RequestID string `json:"request_id"`
}

// query answers each query asked with what Results holds under its
// request_id.
func (a *API) query(req *http.Request, body []byte) (json.RawMessage, bool) {
	var asked map[string][]askedQuery

	_ = json.Unmarshal(body, &asked)

	results := map[string]json.RawMessage{}

	for _, query := range asked["queries"] {
		if readings, held := a.Results[query.RequestID]; held {
			results[query.RequestID] = readings
		}
	}

	data, _ := json.Marshal([]map[string]json.RawMessage{results})

	return data, req.PathValue("device") == SensorID
}

// answer writes a success in Flume's envelope.
func answer(writer http.ResponseWriter, data json.RawMessage) {
	writer.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(writer,
		`{"success": true, "code": %d, "message": "Request OK", "http_code": 200, "http_message": "OK", `+
			`"detailed": null, "data": %s, "count": 1, "pagination": null}`,
		codeOK, data)
}

// refuse writes an error in Flume's envelope.
func refuse(writer http.ResponseWriter, status, code int, message, detailed string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	fmt.Fprintf(writer,
		`{"success": false, "code": %d, "message": %q, "http_code": %d, "http_message": %q, `+
			`"detailed": %s, "data": [], "count": 0, "pagination": null}`,
		code, message, status, http.StatusText(status), detailed)
}
