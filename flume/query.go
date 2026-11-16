package flume

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Bucket is the span of time one reading covers.
type Bucket string

// The buckets the API takes.
const (
	Minute Bucket = "MIN"
	Hour   Bucket = "HR"
	Day    Bucket = "DAY"
	Month  Bucket = "MON"
	Year   Bucket = "YR"
)

// Operation folds a query's readings into one value.
type Operation string

// The operations the API takes.
const (
	Sum     Operation = "SUM"
	Average Operation = "AVG"
	Minimum Operation = "MIN"
	Maximum Operation = "MAX"
	Count   Operation = "CNT"
)

// Units is the unit a query's values come in.
type Units string

// The units the API takes; it answers in gallons when none is given.
const (
	Gallons     Units = "GALLONS"
	Liters      Units = "LITERS"
	CubicFeet   Units = "CUBIC_FEET"
	CubicMeters Units = "CUBIC_METERS"
)

// maxMultiplier is the API's ceiling on a query's multiplier.
const maxMultiplier = 100

// Query is one question about the water a sensor measured. Several go in
// one request, each answered under its ID.
type Query struct {
	// ID names the query in the answer; it has to be there and its own.
	ID string
	// Bucket is the span of each reading.
	Bucket Bucket
	// Since is the start of the range, included; Until is its end, not
	// included, and now when zero. A range spans a year at most. Both are
	// instants: the client writes them as the wall clock of its Zone.
	Since time.Time
	Until time.Time
	// Multiplier groups buckets, three months a reading with Month; zero
	// is one, a hundred the most.
	Multiplier int
	// Operation, when set, folds the readings into one, which has no
	// time.
	Operation Operation
	// Units is the unit of the values; empty is gallons.
	Units Units
	// Descending gives the latest reading first.
	Descending bool
}

// Reading is the water measured over one bucket, which starts at Time, or
// a whole query folded by its Operation, with a zero Time.
type Reading struct {
	Time  time.Time
	Value float64
}

// The query route's request and answer.
type (
	wireQueries struct {
		Queries []wireQuery `json:"queries"`
	}
	wireQuery struct {
		RequestID  string    `json:"request_id"`
		Bucket     Bucket    `json:"bucket"`
		Since      string    `json:"since_datetime"`
		Until      string    `json:"until_datetime,omitempty"`
		Multiplier int       `json:"group_multiplier,omitempty"`
		Operation  Operation `json:"operation,omitempty"`
		Units      Units     `json:"units,omitempty"`
		Sort       string    `json:"sort_direction,omitempty"`
	}
	wireReading struct {
		Datetime string  `json:"datetime"`
		Value    float64 `json:"value"`
	}
)

// Query asks the sensor with this id every query in one request, and
// returns each one's readings under its ID.
func (c *Client) Query(ctx context.Context, sensor string, queries ...Query) (map[string][]Reading, error) {
	path, err := devicePath(sensor, "/query")
	if err != nil {
		return nil, err
	}

	body, err := c.wire(queries)
	if err != nil {
		return nil, err
	}

	var answers []map[string][]wireReading

	if err = c.call(ctx, http.MethodPost, path, nil, body, &answers); err != nil {
		return nil, err
	}

	answer, err := only(answers, "query result")
	if err != nil {
		return nil, err
	}

	readings := make(map[string][]Reading, len(queries))

	for _, query := range queries {
		answered, found := answer[query.ID]
		if !found {
			return nil, fmt.Errorf("%w: no result for the query %q", ErrAnswer, query.ID)
		}

		readings[query.ID] = make([]Reading, 0, len(answered))

		for _, reading := range answered {
			started, err := c.instant(reading.Datetime)
			if err != nil {
				return nil, err
			}

			readings[query.ID] = append(readings[query.ID], Reading{Time: started, Value: reading.Value})
		}
	}

	return readings, nil
}

// wire checks the queries and writes them as the route takes them.
func (c *Client) wire(queries []Query) (*wireQueries, error) {
	if len(queries) == 0 {
		return nil, fmt.Errorf("%w: no query", ErrArgument)
	}

	body := &wireQueries{Queries: make([]wireQuery, 0, len(queries))}
	seen := make(map[string]struct{}, len(queries))

	for _, query := range queries {
		if _, twice := seen[query.ID]; twice || query.ID == "" {
			return nil, fmt.Errorf("%w: a query's ID %q is empty or used twice", ErrArgument, query.ID)
		}

		seen[query.ID] = struct{}{}

		switch query.Bucket {
		case Minute, Hour, Day, Month, Year:
		default:
			return nil, fmt.Errorf("%w: the query %q has the bucket %q", ErrArgument, query.ID, query.Bucket)
		}

		if query.Since.IsZero() || query.Multiplier < 0 || query.Multiplier > maxMultiplier {
			return nil, fmt.Errorf(
				"%w: the query %q has no start, or a multiplier outside 0 to %d",
				ErrArgument, query.ID, maxMultiplier,
			)
		}

		sent := wireQuery{
			RequestID:  query.ID,
			Bucket:     query.Bucket,
			Since:      query.Since.In(c.zone).Format(wallClock),
			Multiplier: query.Multiplier,
			Operation:  query.Operation,
			Units:      query.Units,
		}

		if !query.Until.IsZero() {
			sent.Until = query.Until.In(c.zone).Format(wallClock)
		}

		if query.Descending {
			sent.Sort = "DESC"
		}

		body.Queries = append(body.Queries, sent)
	}

	return body, nil
}

// instant reads a wall clock of the client's zone; an absent one, a
// folded query's, is the zero time.
func (c *Client) instant(clock string) (time.Time, error) {
	if clock == "" {
		return time.Time{}, nil
	}

	read, err := time.ParseInLocation(wallClock, clock, c.zone)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: the time %q: %w", ErrAnswer, clock, err)
	}

	return read, nil
}
