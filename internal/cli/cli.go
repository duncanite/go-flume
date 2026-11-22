// Package cli is the flume command: what Flume's cloud API says of an
// account's devices, the water a sensor measured, whether water runs now,
// and the alerts Flume raised. The account's credentials come from the
// environment and from nowhere else.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kong"

	"github.com/duncanite/go-flume/flume"
	"github.com/duncanite/go-flume/internal/terminal"
)

// Exit statuses.
const (
	ExitOK    = 0
	ExitError = 1
)

// The environment variables the credentials are read from.
const (
	EnvClientID     = "FLUME_CLIENT_ID"
	EnvClientSecret = "FLUME_CLIENT_SECRET" // #nosec G101 -- the name of an environment variable, not a credential
	EnvUsername     = "FLUME_USERNAME"
	EnvPassword     = "FLUME_PASSWORD"
)

// clock is how the command prints a time, in the devices' zone.
const clock = "2006-01-02 15:04"

// errUsage is a command line, or an environment, the command cannot act
// on.
var errUsage = errors.New("cannot run")

// exitStatus is how the parser's own exit, on --help, leaves Run: kong
// calls its exit function and goes on, so the function panics with the
// status and Run recovers it.
type exitStatus int

type rootCommand struct {
	API  string `default:"https://api.flumewater.com" env:"FLUME_API"                                                                      help:"Where Flume's API is."`
	Zone string `env:"FLUME_ZONE"                     help:"The time zone the devices are in, as an IANA name; this machine's when empty."`

	Devices devicesCommand `cmd:"" help:"List the account's bridges and sensors."`
	Flow    flowCommand    `cmd:"" help:"Say whether water is running now, and how fast."`
	Usage   usageCommand   `cmd:"" help:"Print the water a sensor measured."`
	Alerts  alertsCommand  `cmd:"" help:"List the usage alerts Flume raised, the latest first."`

	out      io.Writer
	client   *flume.Client
	location *time.Location
}

type sensor struct {
	Sensor string `arg:"" help:"The sensor's id; the account's only sensor when left out." optional:""`
}

type (
	devicesCommand struct{}
	flowCommand    struct{ sensor }
	usageCommand   struct {
		sensor

		Last   time.Duration `default:"24h"                                                                                                                                                                 help:"How far back from now the readings start, in hours, minutes and seconds: 6h, 90m, 720h."`
		From   string        `help:"Where the readings start, in place of --last: a date, or a date and a time, in the devices' zone, as 2025-01-31 or \"2025-01-31 18:00\". A range spans a year at most."`
		To     string        `help:"Where the readings end, in the same form; now when left out."`
		Bucket string        `default:"hour"                                                                                                                                                                enum:"minute,hour,day,month,year"                                                              help:"The span of each reading: minute, hour, day, month or year."`
		Units  string        `default:"gallons"                                                                                                                                                             enum:"gallons,liters,cubic-feet,cubic-meters"                                                  help:"The unit: gallons, liters, cubic-feet or cubic-meters."`
		Total  bool          `help:"Print one value, the sum over the whole range, in place of each reading."`
	}
	alertsCommand struct {
		Sensor string `help:"Keep one device's alerts."`
		Leaks  bool   `help:"Keep the alerts Flume's leak rule raised."`
		Limit  int    `default:"20"                                     help:"How many alerts to print."`
	}
)

// Run parses args and runs the command. client makes every request to
// Flume; getenv reads the environment, where the credentials are.
func Run(
	ctx context.Context,
	args []string,
	stdout, stderr io.Writer,
	client *http.Client,
	getenv func(string) string,
) (status int) {
	defer func() {
		if recovered := recover(); recovered != nil {
			exited, ok := recovered.(exitStatus)
			if !ok {
				panic(recovered)
			}

			status = int(exited)
		}
	}()

	root := rootCommand{out: stdout}

	parser, err := kong.New(&root,
		kong.Name("flume"),
		kong.Description("Ask Flume's cloud API about an account's water monitors. The credentials are read from "+
			EnvClientID+", "+EnvClientSecret+", "+EnvUsername+" and "+EnvPassword+"."),
		kong.UsageOnError(),
		kong.Writers(stdout, stderr),
		kong.Exit(func(code int) { panic(exitStatus(code)) }),
		kong.BindTo(ctx, (*context.Context)(nil)),
	)
	if err != nil {
		terminal.Printf(stderr, "flume: %s\n", err)

		return ExitError
	}

	kctx, err := parser.Parse(args)
	if err != nil {
		parser.Errorf("%s", err)

		return ExitError
	}

	if err = root.connect(client, getenv); err == nil {
		err = kctx.Run(&root)
	}

	if err != nil {
		terminal.Printf(stderr, "flume: %s\n", err)

		return ExitError
	}

	return ExitOK
}

// connect makes the API client from the flags and the environment; a
// credential that is not set is said by its variable's name.
func (root *rootCommand) connect(client *http.Client, getenv func(string) string) error {
	var missing []string

	for _, name := range []string{EnvClientID, EnvClientSecret, EnvUsername, EnvPassword} {
		if getenv(name) == "" {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("%w: not set in the environment: %s", errUsage, strings.Join(missing, ", "))
	}

	//nolint:gosmopolitan // the default is a device in the house the command runs in; --zone says otherwise
	root.location = time.Local

	if root.Zone != "" {
		zone, err := time.LoadLocation(root.Zone)
		if err != nil {
			return fmt.Errorf("%w: the zone %q: %w", errUsage, root.Zone, err)
		}

		root.location = zone
	}

	root.client = flume.New(flume.Credentials{
		ClientID:     getenv(EnvClientID),
		ClientSecret: getenv(EnvClientSecret),
		Username:     getenv(EnvUsername),
		Password:     getenv(EnvPassword),
	}, flume.Options{BaseURL: root.API, HTTP: client, Zone: root.location})

	return nil
}

// only is the sensor a command acts on: the one named, or the account's
// only one. until is the end of what the command will ask about, zero for
// now. A sensor Flume does not hear is said so first when the question
// reaches past the last time it was heard: what the API answers from
// there on, no water running, zero gallons, is no measurement.
func (root *rootCommand) only(ctx context.Context, named string, until time.Time) (string, error) {
	devices, err := root.client.Devices(ctx)
	if err != nil {
		return "", err
	}

	var sensors []flume.Device

	for _, device := range devices {
		if device.Kind == flume.Sensor && (named == "" || device.ID == named) {
			sensors = append(sensors, device)
		}
	}

	if len(sensors) != 1 {
		if named != "" {
			return "", fmt.Errorf("%w: the account has no sensor %q", errUsage, named)
		}

		ids := make([]string, 0, len(sensors))
		for _, device := range sensors {
			ids = append(ids, device.ID)
		}

		return "", fmt.Errorf("%w: the account has %d sensors, name one: %s",
			errUsage, len(sensors), strings.Join(ids, ", "))
	}

	switch found := sensors[0]; {
	case found.Connected:
	case found.LastSeen.IsZero():
		terminal.Printf(
			root.out,
			"the sensor is not connected and was never heard: nothing that follows is a measurement\n",
		)
	case until.IsZero() || until.After(found.LastSeen):
		terminal.Printf(
			root.out,
			"the sensor is not connected: nothing after %s, when it was last heard, is a measurement\n",
			found.LastSeen.In(root.location).Format(clock),
		)
	}

	return sensors[0].ID, nil
}

func (*devicesCommand) Run(ctx context.Context, root *rootCommand) error {
	devices, err := root.client.Devices(ctx)
	if err != nil {
		return err
	}

	table := tabwriter.NewWriter(root.out, 0, 0, 2, ' ', 0)

	terminal.Printf(table, "ID\tKIND\tPRODUCT\tCONNECTED\tLAST SEEN\tBATTERY\tBRIDGE\n")

	for _, device := range devices {
		kind := "sensor"
		if device.Kind == flume.Bridge {
			kind = "bridge"
		}

		seen := "-"
		if !device.LastSeen.IsZero() {
			seen = device.LastSeen.In(root.location).Format(clock)
		}

		terminal.Printf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			device.ID, kind, device.Product, yesNo(device.Connected), seen,
			dash(device.BatteryLevel), dash(device.BridgeID))
	}

	return flush(table)
}

func (cmd *flowCommand) Run(ctx context.Context, root *rootCommand) error {
	sensorID, err := root.only(ctx, cmd.Sensor, time.Time{})
	if err != nil {
		return err
	}

	flow, err := root.client.Flow(ctx, sensorID)
	if err != nil {
		return err
	}

	if !flow.Active {
		terminal.Printf(root.out, "no water is running (measured %s)\n", flow.At.In(root.location).Format(clock))

		return nil
	}

	terminal.Printf(root.out, "water is running: %.2f gallons a minute (measured %s)\n",
		flow.GallonsPerMinute, flow.At.In(root.location).Format(clock))

	return nil
}

func (cmd *usageCommand) Run(ctx context.Context, root *rootCommand) error {
	asked, err := cmd.span(root.location)
	if err != nil {
		return err
	}

	sensorID, err := root.only(ctx, cmd.Sensor, asked.until)
	if err != nil {
		return err
	}

	query := flume.Query{
		ID:     "usage",
		Bucket: buckets()[cmd.Bucket],
		Since:  asked.since,
		Until:  asked.until,
		Units:  units()[cmd.Units],
	}

	if cmd.Total {
		query.Operation = flume.Sum
	}

	readings, err := root.client.Query(ctx, sensorID, query)
	if err != nil {
		return err
	}

	if cmd.Total {
		for _, reading := range readings[query.ID] {
			terminal.Printf(root.out, "%.2f %s %s\n", reading.Value, cmd.Units, asked.said)
		}

		return nil
	}

	table := tabwriter.NewWriter(root.out, 0, 0, 2, ' ', tabwriter.AlignRight)

	terminal.Printf(table, "FROM\t%s\t\n", strings.ToUpper(cmd.Units))

	for _, reading := range readings[query.ID] {
		terminal.Printf(table, "%s\t%.2f\t\n", reading.Time.In(root.location).Format(clock), reading.Value)
	}

	return flush(table)
}

// timeRange is a range to ask about, and how to say it.
type timeRange struct {
	since time.Time
	until time.Time
	said  string
}

// span is the range the flags ask for: --from to --to, or to now, when
// --from is given, the last --last otherwise.
func (cmd *usageCommand) span(zone *time.Location) (timeRange, error) {
	if cmd.From == "" {
		if cmd.To != "" || cmd.Last <= 0 {
			return timeRange{}, fmt.Errorf("%w: --to wants --from, and --last a time above zero", errUsage)
		}

		return timeRange{since: time.Now().Add(-cmd.Last), said: "over the last " + cmd.Last.String()}, nil
	}

	since, err := moment(cmd.From, zone)
	if err != nil {
		return timeRange{}, err
	}

	asked := timeRange{since: since, said: "from " + since.Format(clock)}

	if cmd.To != "" {
		if asked.until, err = moment(cmd.To, zone); err != nil {
			return timeRange{}, err
		}

		asked.said += " to " + asked.until.Format(clock)
	}

	return asked, nil
}

// moment reads a date, or a date and a time to the minute, in zone.
func moment(written string, zone *time.Location) (time.Time, error) {
	for _, layout := range []string{clock, time.DateOnly} {
		if read, err := time.ParseInLocation(layout, written, zone); err == nil {
			return read, nil
		}
	}

	return time.Time{}, fmt.Errorf("%w: %q is not a date as 2025-01-31, nor with a time as \"2025-01-31 18:00\"",
		errUsage, written)
}

func (cmd *alertsCommand) Run(ctx context.Context, root *rootCommand) error {
	alerts, err := root.client.Alerts(ctx, flume.AlertFilter{Device: cmd.Sensor, Leaks: cmd.Leaks, Limit: cmd.Limit})
	if err != nil {
		return err
	}

	if len(alerts) == 0 {
		terminal.Printf(root.out, "no alert\n")

		return nil
	}

	table := tabwriter.NewWriter(root.out, 0, 0, 2, ' ', 0)

	terminal.Printf(table, "ID\tRAISED\tLEAK\tRULE\tDEVICE\n")

	for _, alert := range alerts {
		terminal.Printf(table, "%d\t%s\t%s\t%s\t%s\n",
			alert.ID, alert.Triggered.In(root.location).Format(clock), yesNo(alert.Leak), alert.Rule, alert.DeviceID)
	}

	return flush(table)
}

func buckets() map[string]flume.Bucket {
	return map[string]flume.Bucket{
		"minute": flume.Minute, "hour": flume.Hour, "day": flume.Day, "month": flume.Month, "year": flume.Year,
	}
}

func units() map[string]flume.Units {
	return map[string]flume.Units{
		"gallons": flume.Gallons, "liters": flume.Liters,
		"cubic-feet": flume.CubicFeet, "cubic-meters": flume.CubicMeters,
	}
}

func yesNo(yes bool) string {
	if yes {
		return "yes"
	}

	return "no"
}

func dash(value string) string {
	if value == "" {
		return "-"
	}

	return value
}

func flush(table *tabwriter.Writer) error {
	if err := table.Flush(); err != nil {
		return fmt.Errorf("writing the table: %w", err)
	}

	return nil
}
