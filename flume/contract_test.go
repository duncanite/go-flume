package flume_test

// The contract, from Flume's documentation (https://flumetech.readme.io),
// each rule under the page it is on.
//
// docs/authentication, reference/get-tokens:
//   - POST /oauth/token takes JSON: grant_type "password", client_id,
//     client_secret, username, password. Its data is one object with
//     access_token and expires_in, in seconds.
//   - Every other route takes "Authorization: Bearer <access token>".
//   - Bad client credentials are a 400.
//
// reference/introduction: /users/{user_id} may be written /me.
//
// docs/response: every answer is an envelope with success, code, message,
// detailed and data; data is a list. 401, 403, 404, 429 and 500 are
// errors; 429 is the rate limit.
//
// reference/get-a-users-devices: GET /users/{id}/devices. A bridge has
// type 1, a sensor type 2; last_seen is UTC with a Z.
//
// docs/querying-samples, reference/query-a-user-device: POST
// /users/{id}/devices/{device}/query takes {"queries": [...]}. Each query
// has request_id, bucket (MIN, HR, DAY, MON, YR) and since_datetime, and
// may have until_datetime, group_multiplier (100 at most), operation,
// units and sort_direction. The first element of data holds each query's
// readings under its request_id, as datetime and value; with an operation
// there is one reading and no datetime.
//
// docs/datetime-specification: a query's datetimes, and a reading's, are
// "YYYY-MM-DD HH:II:SS" in the device's own zone, with no offset.
//
// reference/get-current-flow-rate: GET .../query/active gives active, gpm
// and datetime.
//
// reference/fetch-user-usage-alerts: GET /users/{id}/usage-alerts takes
// limit, sort_direction, device_id and flume_leak.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duncanite/go-flume/flume"
	"github.com/duncanite/go-flume/flume/flumetest"
)

// pacific is a zone eight hours behind UTC, for a device that is not where
// the test runs.
func pacific() *time.Location { return time.FixedZone("device", -8*60*60) }

func account() flume.Credentials {
	return flume.Credentials{
		ClientID:     flumetest.ClientID,
		ClientSecret: flumetest.ClientSecret,
		Username:     flumetest.Username,
		Password:     flumetest.Password,
	}
}

func connected(t *testing.T) (*flume.Client, *flumetest.API) {
	t.Helper()

	api := flumetest.New()
	t.Cleanup(api.Close)

	return flume.New(account(), flume.Options{BaseURL: api.URL, HTTP: api.Client(), Zone: pacific()}), api
}

// TestTokenAndDevices: the first call asks for a token by the password
// grant, in JSON; the token goes with every call as a bearer; one token
// serves the calls that follow; the devices come back as bridge and
// sensor.
func TestTokenAndDevices(t *testing.T) {
	t.Parallel()

	client, api := connected(t)

	devices, err := client.Devices(t.Context())
	if err != nil || len(devices) != 2 {
		t.Fatalf("Devices = %+v, %v", devices, err)
	}

	bridge, sensor := devices[0], devices[1]
	if bridge.Kind != flume.Bridge || bridge.ID != flumetest.BridgeID || bridge.BatteryLevel != "" ||
		!bridge.Connected {
		t.Errorf("the bridge reads %+v", bridge)
	}

	seen := time.Date(2021, time.December, 29, 5, 47, 34, 0, time.UTC)
	if sensor.Kind != flume.Sensor || sensor.ID != flumetest.SensorID || sensor.BridgeID != flumetest.BridgeID ||
		sensor.BatteryLevel != "medium" || !sensor.Oriented || !sensor.LastSeen.Equal(seen) ||
		sensor.Product != "flume2" || sensor.LocationID != 44565 {
		t.Errorf("the sensor reads %+v", sensor)
	}

	if _, err = client.Devices(t.Context()); err != nil {
		t.Fatal(err)
	}

	requests, logins := api.Received()
	if logins != 1 || len(requests) != 3 {
		t.Fatalf("two calls made %d requests and %d logins, want 3 and 1", len(requests), logins)
	}

	var asked map[string]string
	if err = json.Unmarshal(requests[0].Body, &asked); err != nil || len(asked) != 5 ||
		asked["grant_type"] != "password" || asked["client_id"] != flumetest.ClientID ||
		asked["client_secret"] != flumetest.ClientSecret || asked["username"] != flumetest.Username ||
		asked["password"] != flumetest.Password {
		t.Errorf("the token request carried %v, %v", asked, err)
	}

	if requests[0].Method != http.MethodPost || requests[0].Path != "/oauth/token" ||
		requests[1].Method != http.MethodGet || requests[1].Path != "/me/devices" ||
		requests[1].Authorization != "Bearer token-1" || requests[2].Authorization != "Bearer token-1" {
		t.Errorf("the requests were %+v", requests)
	}
}

// TestTokenIsRenewed: a token within a minute of its end is not used; the
// call asks for another first.
func TestTokenIsRenewed(t *testing.T) {
	t.Parallel()

	client, api := connected(t)
	api.Lifetime = 30 * time.Second

	for range 2 {
		if _, err := client.Devices(t.Context()); err != nil {
			t.Fatal(err)
		}
	}

	requests, logins := api.Received()
	if logins != 2 || requests[3].Authorization != "Bearer token-2" {
		t.Fatalf("two calls on a thirty-second token made %d logins, the last under %q",
			logins, requests[3].Authorization)
	}
}

// TestTokenKept: the token a Client holds is read from it and given to
// another, which asks Flume for none; one at its end, and one Flume
// refuses, are asked for again, once, and a token Flume refuses as soon as
// it gave it is not asked for again, nor is one under a 403. A token whose
// life Flume did not tell is sent until it is refused. A Token formatted
// shows nothing of itself.
func TestTokenKept(t *testing.T) {
	t.Parallel()

	first, api := connected(t)
	ctx := t.Context()

	if token, held := first.Token(); held {
		t.Fatalf("Token before any request = %+v", token)
	}

	if _, err := first.Devices(ctx); err != nil {
		t.Fatalf("Devices: %v", err)
	}

	token, held := first.Token()
	if left := time.Until(token.Expires); !held || token.Access != "token-1" ||
		left < 7*24*time.Hour-time.Minute || left > 7*24*time.Hour {
		t.Fatalf("Token after a request: held %t, %q, ending in %s", held, token.Access, left)
	}

	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if printed := fmt.Sprintf(verb, token) + fmt.Sprintf(verb, &token); strings.Contains(printed, "token-1") ||
			strings.Contains(token.LogValue().String(), "token-1") {
			t.Errorf("a Token under %s shows itself: %s", verb, printed)
		}
	}

	restored := func(given flume.Token) *flume.Client {
		return flume.New(account(), flume.Options{BaseURL: api.URL, HTTP: api.Client(), Token: given})
	}

	asks := func(what string, client *flume.Client, tokens int) {
		t.Helper()

		for range 2 {
			if _, err := client.Devices(ctx); err != nil {
				t.Fatalf("Devices, %s: %v", what, err)
			}
		}

		if _, logins := api.Received(); logins != tokens {
			t.Errorf("%s: %d tokens asked for so far, want %d", what, logins, tokens)
		}
	}

	asks("a Client given the token another holds", restored(token), 1)
	asks("a Client given it with no end told", restored(flume.Token{Access: token.Access}), 1)
	asks("a Client given a token at its end", restored(flume.Token{Access: token.Access, Expires: time.Now()}), 2)

	// The token Flume gave last is the one it takes now.
	late := restored(flume.Token{Access: "token-1", Expires: time.Now().Add(time.Hour)})
	asks("a Client given a token Flume refuses", late, 3)

	if token, held = late.Token(); !held || token.Access != "token-3" {
		t.Errorf("Token after Flume refused the one given = %q, %t", token.Access, held)
	}

	// A life that is none is a life not told.
	api.Lifetime = -time.Second
	unending := restored(flume.Token{})
	asks("a Client whose token has no life told", unending, 4)

	if token, held = unending.Token(); !held || !token.Expires.IsZero() {
		t.Errorf("Token with no life told ends at %s, held %t; want no end", token.Expires, held)
	}

	// A 403 says nothing against the token: none is asked for.
	api.Fail = http.StatusForbidden

	if _, err := restored(token).Devices(ctx); !errors.Is(err, flume.ErrCredentials) {
		t.Errorf("Devices that Flume forbids = %v, want ErrCredentials", err)
	}

	if _, logins := api.Received(); logins != 4 {
		t.Errorf("a kept token under a 403: %d tokens asked for so far, want 4", logins)
	}

	// Flume refuses every token: the one kept is asked for again, once,
	// and the one it has just given is not.
	api.Fail = http.StatusUnauthorized

	if _, err := restored(token).Devices(ctx); !errors.Is(err, flume.ErrCredentials) {
		t.Errorf("Devices under a token Flume refuses, then under another it refuses = %v, want ErrCredentials", err)
	}

	if _, logins := api.Received(); logins != 5 {
		t.Errorf("a kept token refused, and the next one too: %d tokens asked for so far, want 5", logins)
	}

	if _, err := restored(flume.Token{}).Devices(ctx); !errors.Is(err, flume.ErrCredentials) {
		t.Errorf("Devices under a token refused as soon as given = %v, want ErrCredentials", err)
	}

	if _, logins := api.Received(); logins != 6 {
		t.Errorf("a token refused as soon as given: %d tokens asked for so far, want 6", logins)
	}
}

// TestTokenRefused: a kept token Flume no longer takes, met by many calls
// at once, is asked for again once for them all, and each call is
// answered. When the token cannot be had again, that is the error, and
// the call is not sent a second time.
func TestTokenRefused(t *testing.T) {
	t.Parallel()

	api := flumetest.New()
	t.Cleanup(api.Close)

	stale := flume.Token{Access: "token-0", Expires: time.Now().Add(time.Hour)}
	client := flume.New(account(), flume.Options{BaseURL: api.URL, HTTP: api.Client(), Token: stale})

	const callers = 32

	var group sync.WaitGroup

	failed := make(chan error, callers)

	for range callers {
		group.Go(func() {
			_, err := client.Devices(t.Context())
			failed <- err
		})
	}

	group.Wait()
	close(failed)

	for err := range failed {
		if err != nil {
			t.Errorf("Devices, one of %d at once under a token Flume no longer takes: %v", callers, err)
		}
	}

	if _, logins := api.Received(); logins != 1 {
		t.Errorf("%d calls at once under a token Flume no longer takes asked for %d tokens, want 1", callers, logins)
	}

	if token, held := client.Token(); !held || token.Access != "token-1" {
		t.Errorf("Token after it = %q, %t; want the one Flume gave", token.Access, held)
	}

	wrong := account()
	wrong.Password = "not the password"

	before, _ := api.Received()

	_, err := flume.New(wrong, flume.Options{BaseURL: api.URL, HTTP: api.Client(), Token: stale}).Devices(t.Context())
	if !errors.Is(err, flume.ErrCredentials) || !strings.Contains(err.Error(), "invalid_client") {
		t.Errorf("Devices under a token and a password Flume refuses = %v, want the token route's refusal", err)
	}

	if after, _ := api.Received(); len(after)-len(before) != 2 {
		t.Errorf("a token that could not be had again took %d requests, want the call and the token's",
			len(after)-len(before))
	}
}

// TestCredentialsRefused: credentials Flume does not take are
// ErrCredentials, and the error does not carry them.
func TestCredentialsRefused(t *testing.T) {
	t.Parallel()

	api := flumetest.New()
	t.Cleanup(api.Close)

	wrong := account()
	wrong.Password = "not the password"

	_, err := flume.New(wrong, flume.Options{BaseURL: api.URL, HTTP: api.Client()}).Devices(t.Context())
	if !errors.Is(err, flume.ErrCredentials) || !strings.Contains(err.Error(), "invalid_client") {
		t.Fatalf("Devices under a wrong password = %v, want ErrCredentials with Flume's message", err)
	}

	for _, secret := range []string{wrong.Password, wrong.ClientSecret, wrong.ClientID, wrong.Username} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("the error carries a credential: %v", err)
		}
	}
}

// TestSecretsDoNotPrint: the credentials, and a client that holds them and
// a token, show none of the four nor the token however they are printed:
// formatted with a verb fmt knows or not, by value or through a pointer,
// inside another struct or a map, the client in an unexported field too,
// as JSON, in a log record of either handler.
func TestSecretsDoNotPrint(t *testing.T) {
	t.Parallel()

	client, _ := connected(t)
	if _, err := client.Devices(t.Context()); err != nil {
		t.Fatal(err)
	}

	credentials := account()
	holder := struct {
		Credentials flume.Credentials
		Pointer     *flume.Credentials
		Client      *flume.Client
	}{credentials, &credentials, client}

	// Out of reach of the Client's own formatting: fmt asks no method of an
	// unexported field.
	unexported := struct{ client *flume.Client }{client}

	// A Client held by value: itself, in a field exported or not, in a map.
	values := struct {
		Client flume.Client
		client flume.Client
		Map    map[string]flume.Client
	}{*client, *client, map[string]flume.Client{"one": *client}}

	var printed bytes.Buffer

	for _, value := range []any{
		credentials, &credentials, client, holder, &holder, unexported, &unexported, *client, values, &values,
	} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%d", "%x"} {
			fmt.Fprintf(&printed, verb+"\n", value)
		}

		marshalled, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}

		printed.Write(marshalled)

		slog.New(slog.NewJSONHandler(&printed, nil)).Info("made", "value", value)
		slog.New(slog.NewTextHandler(&printed, nil)).Info("made", "value", value)
	}

	for _, secret := range []string{
		flumetest.ClientID, flumetest.ClientSecret, flumetest.Username, flumetest.Password, "token-1",
	} {
		if strings.Contains(printed.String(), secret) {
			t.Errorf("%q was printed:\n%s", secret, printed.String())
		}
	}
}

// TestQuery: the range goes out as the wall clock of the client's zone;
// what is not set is not sent; each query's readings come back under its
// ID, as instants, and a folded query's reading has no time.
func TestQuery(t *testing.T) {
	t.Parallel()

	client, api := connected(t)
	api.Results["days"] = json.RawMessage(
		`[{"datetime": "2026-03-01 00:00:00", "value": 114.9}, {"datetime": "2026-03-02 00:00:00", "value": 115}]`,
	)
	api.Results["total"] = json.RawMessage(`[{"value": 4788.875}]`)

	// Midnight on the first of March where the device is, said in UTC.
	since := time.Date(2026, time.March, 1, 8, 0, 0, 0, time.UTC)

	readings, err := client.Query(t.Context(), flumetest.SensorID,
		flume.Query{ID: "days", Bucket: flume.Day, Since: since},
		flume.Query{
			ID: "total", Bucket: flume.Month, Since: since, Until: since.Add(48 * time.Hour),
			Multiplier: 3, Operation: flume.Sum, Units: flume.Liters, Descending: true,
		},
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	days, total := readings["days"], readings["total"]
	if len(readings) != 2 || len(days) != 2 || !days[0].Time.Equal(since) || days[0].Value != 114.9 ||
		!days[1].Time.Equal(since.Add(24*time.Hour)) || days[1].Value != 115 {
		t.Errorf("the days read %+v", days)
	}

	if len(total) != 1 || !total[0].Time.IsZero() || total[0].Value != 4788.875 {
		t.Errorf("the total reads %+v", total)
	}

	requests, _ := api.Received()
	sent := requests[len(requests)-1]

	const want = `{"queries":[` +
		`{"request_id":"days","bucket":"DAY","since_datetime":"2026-03-01 00:00:00"},` +
		`{"request_id":"total","bucket":"MON","since_datetime":"2026-03-01 00:00:00",` +
		`"until_datetime":"2026-03-03 00:00:00","group_multiplier":3,"operation":"SUM",` +
		`"units":"LITERS","sort_direction":"DESC"}]}`

	if sent.Method != http.MethodPost || sent.Path != "/me/devices/"+flumetest.SensorID+"/query" ||
		string(sent.Body) != want {
		t.Errorf("the query went out as %s %s\n%s\nwant\n%s", sent.Method, sent.Path, sent.Body, want)
	}
}

// TestQueriesRefused: a query that cannot be sent, and an empty device id,
// are ErrArgument, and nothing is sent, the token request included.
func TestQueriesRefused(t *testing.T) {
	t.Parallel()

	client, api := connected(t)
	since := time.Date(2026, time.March, 1, 0, 0, 0, 0, time.UTC)
	good := flume.Query{ID: "q", Bucket: flume.Day, Since: since}

	for name, queries := range map[string][]flume.Query{
		"no query":             {},
		"no ID":                {{Bucket: flume.Day, Since: since}},
		"an ID twice":          {good, good},
		"an unknown bucket":    {{ID: "q", Bucket: "WEEK", Since: since}},
		"no bucket":            {{ID: "q", Since: since}},
		"no start":             {{ID: "q", Bucket: flume.Day}},
		"a multiplier of 101":  {{ID: "q", Bucket: flume.Day, Since: since, Multiplier: 101}},
		"a negative multipler": {{ID: "q", Bucket: flume.Day, Since: since, Multiplier: -1}},
	} {
		if _, err := client.Query(t.Context(), flumetest.SensorID, queries...); !errors.Is(err, flume.ErrArgument) {
			t.Errorf("Query with %s = %v, want ErrArgument", name, err)
		}
	}

	if _, err := client.Query(t.Context(), "", good); !errors.Is(err, flume.ErrArgument) {
		t.Errorf("Query of no device = %v, want ErrArgument", err)
	}

	if _, err := client.Flow(t.Context(), ""); !errors.Is(err, flume.ErrArgument) {
		t.Errorf("Flow of no device = %v, want ErrArgument", err)
	}

	if requests, _ := api.Received(); len(requests) != 0 {
		t.Errorf("the refused calls sent %+v", requests)
	}

	// The ceiling itself is taken.
	if _, err := client.Query(t.Context(), flumetest.SensorID,
		flume.Query{ID: "q", Bucket: flume.Day, Since: since, Multiplier: 100}); !errors.Is(err, flume.ErrAnswer) {
		t.Errorf("Query with a multiplier of 100 = %v, want it sent, and no result for it", err)
	}
}

// TestFlow: whether water runs, the rate, and when, read in the client's
// zone.
func TestFlow(t *testing.T) {
	t.Parallel()

	client, api := connected(t)

	flow, err := client.Flow(t.Context(), flumetest.SensorID)
	if err != nil || !flow.Active || flow.GallonsPerMinute != 0.78 ||
		!flow.At.Equal(time.Date(2024, time.January, 1, 23, 22, 0, 0, time.UTC)) {
		t.Fatalf("Flow = %+v, %v", flow, err)
	}

	requests, _ := api.Received()
	if last := requests[len(requests)-1]; last.Method != http.MethodGet ||
		last.Path != "/me/devices/"+flumetest.SensorID+"/query/active" {
		t.Errorf("Flow asked %s %s", last.Method, last.Path)
	}
}

// TestAlerts: the alerts as Flume lists them, asked for latest first; the
// filter goes out as the route's parameters, and an empty one sends no
// more than the order.
func TestAlerts(t *testing.T) {
	t.Parallel()

	client, api := connected(t)

	alerts, err := client.Alerts(t.Context(), flume.AlertFilter{})
	if err != nil || len(alerts) != 2 {
		t.Fatalf("Alerts = %+v, %v", alerts, err)
	}

	raised := time.Date(2017, time.March, 6, 10, 0, 0, 0, time.UTC)
	if leak := alerts[1]; leak.ID != 4 || !leak.Leak || leak.Rule != "Flume Smart Leak Rule" ||
		leak.DeviceID != flumetest.SensorID || !leak.Triggered.Equal(raised) || alerts[0].Leak {
		t.Errorf("the alerts read %+v", alerts)
	}

	if _, err = client.Alerts(t.Context(),
		flume.AlertFilter{Device: flumetest.SensorID, Leaks: true, Limit: 5}); err != nil {
		t.Fatal(err)
	}

	requests, _ := api.Received()
	plain, filtered := requests[len(requests)-2], requests[len(requests)-1]

	if plain.Path != "/me/usage-alerts" || plain.Query.Encode() != "sort_direction=DESC" {
		t.Errorf("an empty filter asked %s?%s", plain.Path, plain.Query.Encode())
	}

	if want := "device_id=" + flumetest.SensorID + "&flume_leak=true&limit=5&sort_direction=DESC"; filtered.Query.Encode() != want {
		t.Errorf("the filter went out as %s, want %s", filtered.Query.Encode(), want)
	}
}

// TestRefusals: each class of answer that is not a success, by its status;
// a device that is not the account's; and the answers that are not in
// the documented shape.
func TestRefusals(t *testing.T) {
	t.Parallel()

	for status, want := range map[int]error{
		http.StatusTooManyRequests:     flume.ErrRateLimited,
		http.StatusNotFound:            flume.ErrNotFound,
		http.StatusUnauthorized:        flume.ErrCredentials,
		http.StatusForbidden:           flume.ErrCredentials,
		http.StatusBadRequest:          flume.ErrRefused,
		http.StatusInternalServerError: flume.ErrRefused,
	} {
		client, api := connected(t)
		api.Fail = status

		if _, err := client.Devices(t.Context()); !errors.Is(err, want) ||
			!strings.Contains(err.Error(), http.StatusText(status)) {
			t.Errorf("Devices answered %d = %v, want %v with Flume's message", status, err, want)
		}
	}

	client, api := connected(t)

	if _, err := client.Flow(t.Context(), "1234"); !errors.Is(err, flume.ErrNotFound) {
		t.Errorf("Flow of a device that is not the account's = %v, want ErrNotFound", err)
	}

	for name, raw := range map[string]string{
		"not JSON":            `<html>`,
		"data that is a map":  `{"success": true, "data": {"active": true}}`,
		"an empty list":       `{"success": true, "data": []}`,
		"a time out of shape": `{"success": true, "data": [{"active": true, "gpm": 1, "datetime": "yesterday"}]}`,
	} {
		api.Raw = []byte(raw)

		if _, err := client.Flow(t.Context(), flumetest.SensorID); !errors.Is(err, flume.ErrAnswer) {
			t.Errorf("Flow answered %s = %v, want ErrAnswer", name, err)
		}
	}

	api.Raw = []byte(`{"success": false, "code": 94, "message": "A provided parameter failed validation", "data": []}`)

	if _, err := client.Devices(t.Context()); !errors.Is(err, flume.ErrRefused) ||
		!strings.Contains(err.Error(), "code 94") {
		t.Errorf("a 200 that says it failed = %v, want ErrRefused with Flume's code", err)
	}

	// A good answer, then padding past what an answer may be.
	api.Raw = append([]byte(`{"success": true, "data": []}`), bytes.Repeat([]byte(" "), 16<<20)...)

	if _, err := client.Devices(t.Context()); !errors.Is(err, flume.ErrAnswer) {
		t.Errorf("an answer over the limit = %v, want ErrAnswer", err)
	}
}
