package flume

import (
	"context"
	"net/http"
	"time"
)

// Kind is what a device is, the API's type: a bridge or a sensor.
type Kind int

// The kinds the documentation gives.
const (
	Bridge Kind = 1
	Sensor Kind = 2
)

// Device is one device on the account, a bridge or the sensor behind it.
// What the API leaves out or gives as null for a kind stays zero.
type Device struct {
	ID   string `json:"id"`
	Kind Kind   `json:"type"`
	// Product is Flume's word for the model, "flume2".
	Product string `json:"product"`
	// BridgeID is the bridge a sensor reports through.
	BridgeID   string `json:"bridge_id"`
	LocationID int64  `json:"location_id"`
	// Connected is whether Flume hears the device now; LastSeen is when
	// it last did.
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
	// BatteryLevel is a sensor's, in Flume's words, "medium".
	BatteryLevel string `json:"battery_level"`
	// Oriented is whether a sensor is seated on its meter.
	Oriented bool `json:"oriented"`
}

// Devices is the account's devices, bridges and sensors, the first fifty.
func (c *Client) Devices(ctx context.Context) ([]Device, error) {
	var devices []Device

	if err := c.call(ctx, http.MethodGet, "/me/devices", nil, nil, &devices); err != nil {
		return nil, err
	}

	return devices, nil
}

// Flow is whether water runs through a sensor's meter now.
type Flow struct {
	Active bool
	// GallonsPerMinute is the rate; the route has no other unit.
	GallonsPerMinute float64
	// At is when Flume measured it.
	At time.Time
}

// Flow asks whether water is running at the sensor with this id.
func (c *Client) Flow(ctx context.Context, sensor string) (Flow, error) {
	path, err := devicePath(sensor, "/query/active")
	if err != nil {
		return Flow{}, err
	}

	var answers []struct {
		Active   bool    `json:"active"`
		GPM      float64 `json:"gpm"`
		Datetime string  `json:"datetime"`
	}

	if err = c.call(ctx, http.MethodGet, path, nil, nil, &answers); err != nil {
		return Flow{}, err
	}

	answer, err := only(answers, "flow rate")
	if err != nil {
		return Flow{}, err
	}

	measured, err := c.instant(answer.Datetime)
	if err != nil {
		return Flow{}, err
	}

	return Flow{Active: answer.Active, GallonsPerMinute: answer.GPM, At: measured}, nil
}
