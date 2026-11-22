package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duncanite/go-flume/flume/flumetest"
	"github.com/duncanite/go-flume/internal/cli"
)

// environment is the account the fake accepts, as the command reads it.
func environment() map[string]string {
	return map[string]string{
		cli.EnvClientID:     flumetest.ClientID,
		cli.EnvClientSecret: flumetest.ClientSecret,
		cli.EnvUsername:     flumetest.Username,
		cli.EnvPassword:     flumetest.Password,
	}
}

// run is the command against the fake, its devices in UTC, under env.
func run(t *testing.T, api *flumetest.API, env map[string]string, args ...string) (string, int) {
	t.Helper()

	var out, errs bytes.Buffer

	args = append([]string{"--api", api.URL, "--zone", "UTC"}, args...)
	status := cli.Run(t.Context(), args, &out, &errs, api.Client(), func(name string) string { return env[name] })

	return out.String() + errs.String(), status
}

func started(t *testing.T) *flumetest.API {
	t.Helper()

	api := flumetest.New()
	t.Cleanup(api.Close)

	return api
}

// lines is the output as rows of fields, the spacing of a table aside.
func lines(out string) [][]string {
	var rows [][]string

	for line := range strings.SplitSeq(strings.TrimSpace(out), "\n") {
		rows = append(rows, strings.Fields(line))
	}

	return rows
}

// TestTheServicesWords: what the service says is shown and never obeyed. A
// rule's name and a device's product, with a line's end, a tab, a
// direction mark and an escape sequence in them, are written out in the
// listings, and forge no line and no column of the command's.
func TestTheServicesWords(t *testing.T) {
	t.Parallel()

	const (
		// As JSON writes it, and as the command shows it.
		hostile = `Rule\nflume: all is well\t\u202e\u001b[2J`
		shown   = `Rule\nflume: all is well\t\u202e\x1b[2J`
	)

	api := started(t)
	api.Alerts = json.RawMessage(`[{"id": 5, "device_id": "` + flumetest.SensorID + `",
		"triggered_datetime": "2017-03-06T10:00:00.000Z", "flume_leak": false, "event_rule_name": "` + hostile + `"}]`)
	api.Devices = json.RawMessage(`[{"id": "` + flumetest.SensorID + `", "type": 2, "connected": true,
		"battery_level": "` + hostile + `", "product": "` + hostile + `"}]`)

	out, status := run(t, api, environment(), "alerts")
	if status != cli.ExitOK || strings.Count(out, "\n") != 2 || strings.Count(out, shown) != 1 {
		t.Errorf("alerts with a hostile rule exits %d saying\n%s", status, out)
	}

	out, status = run(t, api, environment(), "devices")
	if status != cli.ExitOK || strings.Count(out, "\n") != 2 || strings.Count(out, shown) != 2 {
		t.Errorf("devices with hostile words exits %d saying\n%s", status, out)
	}
}

// TestTheCommands: each command against the account, its output as a
// person reads it; a command that is given no sensor finds the account's
// only one.
func TestTheCommands(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Results["usage"] = json.RawMessage(
		`[{"datetime": "2026-03-01 00:00:00", "value": 12.5}, {"datetime": "2026-03-01 01:00:00", "value": 0}]`,
	)

	for _, step := range []struct {
		args []string
		want string
	}{
		{[]string{"devices"}, strings.Join([]string{
			"ID KIND PRODUCT CONNECTED LAST SEEN BATTERY BRIDGE",
			flumetest.BridgeID + " bridge flume2 yes 2021-12-29 05:47 - -",
			flumetest.SensorID + " sensor flume2 yes 2021-12-29 05:47 medium " + flumetest.BridgeID,
		}, "\n")},
		{[]string{"flow"}, "water is running: 0.78 gallons a minute (measured 2024-01-01 15:22)"},
		{[]string{"flow", flumetest.SensorID}, "water is running: 0.78 gallons a minute (measured 2024-01-01 15:22)"},
		{[]string{"usage"}, "FROM GALLONS\n2026-03-01 00:00 12.50\n2026-03-01 01:00 0.00"},
		{[]string{"alerts"}, strings.Join([]string{
			"ID RAISED LEAK RULE DEVICE",
			"5 2017-03-06 10:00 no Custom Rule " + flumetest.SensorID,
			"4 2017-03-06 10:00 yes Flume Smart Leak Rule " + flumetest.SensorID,
		}, "\n")},
	} {
		out, status := run(t, api, environment(), step.args...)

		var got []string
		for _, row := range lines(out) {
			got = append(got, strings.Join(row, " "))
		}

		if status != cli.ExitOK || strings.Join(got, "\n") != step.want {
			t.Errorf("%v: status %d, output:\n%s\nwant\n%s", step.args, status, out, step.want)
		}
	}
}

// TestUsage: the range starts --last ago, in the bucket and the unit
// asked; --total asks for the sum and prints the one value.
func TestUsage(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Results["usage"] = json.RawMessage(`[{"value": 41.25}]`)

	out, status := run(t, api, environment(), "usage", flumetest.SensorID,
		"--last", "2h", "--bucket", "minute", "--units", "liters", "--total")
	if status != cli.ExitOK || out != "41.25 liters over the last 2h0m0s\n" {
		t.Fatalf("usage --total: status %d, output %q", status, out)
	}

	requests, _ := api.Received()

	var sent struct {
		Queries []map[string]any `json:"queries"`
	}

	if err := json.Unmarshal(requests[len(requests)-1].Body, &sent); err != nil || len(sent.Queries) != 1 {
		t.Fatalf("the query went out as %s, %v", requests[len(requests)-1].Body, err)
	}

	query := sent.Queries[0]

	since, err := time.Parse(time.DateTime, query["since_datetime"].(string))
	if err != nil || query["bucket"] != "MIN" || query["units"] != "LITERS" || query["operation"] != "SUM" {
		t.Fatalf("the query was %v, %v", query, err)
	}

	if ago := time.Since(since); ago < 2*time.Hour || ago > 2*time.Hour+time.Minute {
		t.Errorf("--last 2h started the range %s ago", ago)
	}

	if out, status = run(t, api, environment(), "usage", "--last", "0s"); status != cli.ExitError {
		t.Errorf("usage --last 0s: status %d, output %q", status, out)
	}
}

// TestUsageBetweenDates: --from and --to give the range as dates of the
// devices' zone, a time to the minute or none; --to may be left out; what
// is no date is refused, and so is --to alone.
func TestUsageBetweenDates(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Results["usage"] = json.RawMessage(`[{"value": 9}]`)

	sent := func() map[string]any {
		requests, _ := api.Received()

		var body struct {
			Queries []map[string]any `json:"queries"`
		}

		if err := json.Unmarshal(requests[len(requests)-1].Body, &body); err != nil || len(body.Queries) != 1 {
			t.Fatalf("the query went out as %s, %v", requests[len(requests)-1].Body, err)
		}

		return body.Queries[0]
	}

	out, status := run(t, api, environment(), "usage", "--total", "--from", "2025-01-31", "--to", "2025-03-01 18:30")
	if status != cli.ExitOK || out != "9.00 gallons from 2025-01-31 00:00 to 2025-03-01 18:30\n" {
		t.Fatalf("usage between dates: status %d, output %q", status, out)
	}

	if query := sent(); query["since_datetime"] != "2025-01-31 00:00:00" ||
		query["until_datetime"] != "2025-03-01 18:30:00" {
		t.Errorf("the range went out as %v", query)
	}

	out, status = run(t, api, environment(), "usage", "--total", "--from", "2025-01-31 06:00")
	if status != cli.ExitOK || out != "9.00 gallons from 2025-01-31 06:00\n" {
		t.Fatalf("usage from a date: status %d, output %q", status, out)
	}

	if query := sent(); query["since_datetime"] != "2025-01-31 06:00:00" || query["until_datetime"] != nil {
		t.Errorf("the open range went out as %v", query)
	}

	for _, args := range [][]string{
		{"usage", "--from", "yesterday"},
		{"usage", "--from", "2025-01-31", "--to", "31/03/2025"},
		{"usage", "--to", "2025-03-01"},
	} {
		if out, status = run(t, api, environment(), args...); status != cli.ExitError {
			t.Errorf("%v: status %d, output %q", args, status, out)
		}
	}
}

// TestAlertsFilter: the flags go out as the route's parameters, and no
// alert is said so.
func TestAlertsFilter(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Alerts = json.RawMessage(`[]`)

	out, status := run(t, api, environment(), "alerts", "--leaks", "--sensor", flumetest.SensorID, "--limit", "3")
	if status != cli.ExitOK || out != "no alert\n" {
		t.Fatalf("alerts: status %d, output %q", status, out)
	}

	requests, _ := api.Received()

	want := "device_id=" + flumetest.SensorID + "&flume_leak=true&limit=3&sort_direction=DESC"
	if got := requests[len(requests)-1].Query.Encode(); got != want {
		t.Errorf("alerts asked %s, want %s", got, want)
	}
}

// TestCredentialsComeFromTheEnvironment: a credential that is not set is
// named by its variable, nothing is sent, and no value that is set is
// printed.
func TestCredentialsComeFromTheEnvironment(t *testing.T) {
	t.Parallel()

	api := started(t)

	env := environment()
	delete(env, cli.EnvClientSecret)
	delete(env, cli.EnvPassword)

	out, status := run(t, api, env, "devices")
	if status != cli.ExitError || !strings.Contains(out, cli.EnvClientSecret+", "+cli.EnvPassword) {
		t.Fatalf("devices with two variables unset: status %d, output %q", status, out)
	}

	if strings.Contains(out, flumetest.ClientID) || strings.Contains(out, flumetest.Username) {
		t.Errorf("a credential was printed: %q", out)
	}

	if requests, _ := api.Received(); len(requests) != 0 {
		t.Errorf("the command sent %+v", requests)
	}
}

// TestRefusals: what Flume refuses is printed with Flume's words and the
// status is an error; so are a zone that is no zone, and an account with
// more sensors than one when none is named.
func TestRefusals(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Fail = http.StatusTooManyRequests

	out, status := run(t, api, environment(), "devices")
	if status != cli.ExitError || !strings.Contains(out, "flume: flume's request limit is reached") {
		t.Errorf("devices at the rate limit: status %d, output %q", status, out)
	}

	api.Fail = 0

	var errs bytes.Buffer

	status = cli.Run(t.Context(), []string{"--api", api.URL, "--zone", "Nowhere/Never", "devices"},
		&errs, &errs, api.Client(), func(name string) string { return environment()[name] })
	if status != cli.ExitError || !strings.Contains(errs.String(), `the zone "Nowhere/Never"`) {
		t.Errorf("a zone that is none: status %d, output %q", status, errs.String())
	}

	api.Devices = json.RawMessage(`[{"id": "1", "type": 2}, {"id": "2", "type": 2}, {"id": "3", "type": 1}]`)

	out, status = run(t, api, environment(), "flow")
	if status != cli.ExitError || !strings.Contains(out, "the account has 2 sensors, name one: 1, 2") {
		t.Errorf("flow on an account with two sensors: status %d, output %q", status, out)
	}

	// A bridge's id is no sensor's.
	out, status = run(t, api, environment(), "flow", "3")
	if status != cli.ExitError || !strings.Contains(out, `the account has no sensor "3"`) {
		t.Errorf("flow of a bridge: status %d, output %q", status, out)
	}
}

// TestASensorNotConnected: for a sensor Flume does not hear, flow and usage
// say so, with when it was last heard, before what the API answers; not
// for one it hears, and not for a range that ended while it was heard.
func TestASensorNotConnected(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Results["usage"] = json.RawMessage(`[{"value": 0}]`)

	const warning = "the sensor is not connected"

	now := [][]string{{"flow"}, {"flow", flumetest.SensorID}, {"usage", "--total"}}

	for _, args := range now {
		if out, status := run(t, api, environment(), args...); status != cli.ExitOK || strings.Contains(out, warning) {
			t.Errorf("%v on a connected sensor: status %d, output %q", args, status, out)
		}
	}

	api.Devices = json.RawMessage(`[{"id": "` + flumetest.SensorID + `", "type": 2, "connected": false,
		"last_seen": "2020-01-02T03:04:00.000Z"}]`)

	const silent = warning + ": nothing after 2020-01-02 03:04, when it was last heard, is a measurement\n"

	for _, args := range append(now,
		[]string{"usage", "--total", "--from", "2019-12-01"},
		[]string{"usage", "--total", "--from", "2019-12-01", "--to", "2020-01-02 03:05"},
	) {
		if out, status := run(t, api, environment(), args...); status != cli.ExitOK || !strings.HasPrefix(out, silent) {
			t.Errorf("%v on a sensor not connected: status %d, output %q", args, status, out)
		}
	}

	// The range ends on the minute the sensor was last heard: all of it was
	// measured.
	out, status := run(t, api, environment(), "usage", "--total", "--from", "2019-12-01", "--to", "2020-01-02 03:04")
	if status != cli.ExitOK || strings.Contains(out, warning) {
		t.Errorf("usage of a range the sensor was heard through: status %d, output %q", status, out)
	}

	// A range that is no range is refused before anything is said of the sensor.
	out, status = run(t, api, environment(), "usage", "--from", "2019-02-30")
	if status != cli.ExitError || strings.Contains(out, warning) {
		t.Errorf("usage from a date that is none: status %d, output %q", status, out)
	}

	api.Devices = json.RawMessage(`[{"id": "` + flumetest.SensorID + `", "type": 2, "connected": false}]`)

	if out, _ = run(t, api, environment(), "flow"); !strings.HasPrefix(out, warning+" and was never heard: ") {
		t.Errorf("flow on a sensor never heard: %q", out)
	}
}

// TestASensorByName: on an account with two sensors, the one named is the
// one asked; a name the account does not have is refused, on an account
// with one sensor too.
func TestASensorByName(t *testing.T) {
	t.Parallel()

	api := started(t)

	out, status := run(t, api, environment(), "flow", "999")
	if status != cli.ExitError || !strings.Contains(out, `the account has no sensor "999"`) {
		t.Errorf("flow of a sensor the account lacks: status %d, output %q", status, out)
	}

	api.Devices = json.RawMessage(`[{"id": "` + flumetest.SensorID + `", "type": 2, "connected": true},
		{"id": "2", "type": 2, "connected": true}]`)

	out, status = run(t, api, environment(), "flow", flumetest.SensorID)
	if status != cli.ExitOK || !strings.HasPrefix(out, "water is running") {
		t.Fatalf("flow of one of two sensors: status %d, output %q", status, out)
	}

	requests, _ := api.Received()
	if last := requests[len(requests)-1]; last.Path != "/me/devices/"+flumetest.SensorID+"/query/active" {
		t.Errorf("flow of the sensor named asked %s", last.Path)
	}
}

// TestDatesAreTheDevicesZone: a date is the wall clock of --zone, not of
// UTC: midnight in Tokyo goes out as midnight, and is said as midnight.
func TestDatesAreTheDevicesZone(t *testing.T) {
	t.Parallel()

	api := started(t)
	api.Results["usage"] = json.RawMessage(`[{"value": 1}]`)

	var out, errs bytes.Buffer

	status := cli.Run(t.Context(),
		[]string{"--api", api.URL, "--zone", "Asia/Tokyo", "usage", "--total", "--from", "2025-01-31"},
		&out, &errs, api.Client(), func(name string) string { return environment()[name] })
	if status != cli.ExitOK || out.String() != "1.00 gallons from 2025-01-31 00:00\n" {
		t.Fatalf("usage from a date in Tokyo: status %d, output %q%q", status, out.String(), errs.String())
	}

	requests, _ := api.Received()
	if sent := string(
		requests[len(requests)-1].Body,
	); !strings.Contains(
		sent,
		`"since_datetime":"2025-01-31 00:00:00"`,
	) {
		t.Errorf("midnight in Tokyo went out as %s", sent)
	}
}
