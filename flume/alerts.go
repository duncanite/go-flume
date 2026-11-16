package flume

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Alert is one usage alert Flume raised: a rule of the account's, or
// Flume's own leak rule, met by what a sensor measured.
type Alert struct {
	ID       int64  `json:"id"`
	DeviceID string `json:"device_id"`
	// Triggered is when the rule was met.
	Triggered time.Time `json:"triggered_datetime"`
	// Leak is whether Flume's leak rule is the one that raised it.
	Leak bool `json:"flume_leak"`
	// Rule is the rule's name, "Flume Smart Leak Rule".
	Rule string `json:"event_rule_name"`
}

// AlertFilter narrows Alerts; the zero value is every device's alerts,
// fifty at most.
type AlertFilter struct {
	// Device keeps one device's alerts.
	Device string
	// Leaks keeps the alerts Flume's leak rule raised.
	Leaks bool
	// Limit is how many to return; zero is the API's fifty.
	Limit int
}

// Alerts is the usage alerts raised on the account's devices, by
// descending id: the latest first, as long as Flume numbers them in order.
func (c *Client) Alerts(ctx context.Context, filter AlertFilter) ([]Alert, error) {
	query := url.Values{"sort_direction": {"DESC"}}

	if filter.Device != "" {
		query.Set("device_id", filter.Device)
	}

	if filter.Leaks {
		query.Set("flume_leak", "true")
	}

	if filter.Limit > 0 {
		query.Set("limit", strconv.Itoa(filter.Limit))
	}

	var alerts []Alert

	if err := c.call(ctx, http.MethodGet, "/me/usage-alerts", query, nil, &alerts); err != nil {
		return nil, err
	}

	return alerts, nil
}
