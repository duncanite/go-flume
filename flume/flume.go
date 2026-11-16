// Package flume is a client of Flume's cloud API, the one interface Flume
// supports for an owner's own water monitor: the devices on the account,
// the water they measured, whether water runs now, and the alerts Flume
// raised. It is REST and JSON over HTTPS under an OAuth2 password grant,
// for personal use, 120 requests an hour.
//
// The contract is Flume's documentation, https://flumetech.readme.io:
// docs/design.md says what was read there and what was not.
package flume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// BaseURL is where Flume's documentation puts the API.
const BaseURL = "https://api.flumewater.com"

const (
	// maxAnswer bounds one answer read; a year by the day is a few
	// kilobytes.
	maxAnswer = 16 << 20
	// renewAhead is how long before its end a token is no longer used.
	renewAhead = time.Minute
	// wallClock is how the API writes a time in a device's own zone, with
	// no offset: a query's range, a reading's time.
	wallClock = time.DateTime
)

// The errors a Client hands back, to test with errors.Is. Each carries
// Flume's own code and message when Flume gave them.
var (
	// ErrCredentials: Flume refused the client, the user or the token.
	ErrCredentials = errors.New("flume refused the credentials")
	// ErrRateLimited: the hourly limit is reached; Flume answered 429.
	ErrRateLimited = errors.New("flume's request limit is reached")
	// ErrNotFound: no such record, a device's id that is not the
	// account's among them.
	ErrNotFound = errors.New("flume has no such record")
	// ErrRefused: any other answer that is not a success.
	ErrRefused = errors.New("flume refused the request")
	// ErrAnswer: the answer is not in the shape the documentation gives,
	// or is larger than an answer may be.
	ErrAnswer = errors.New("flume's answer is not in the expected shape")
	// ErrArgument: a device's id or a query that cannot be sent; nothing
	// was.
	ErrArgument = errors.New("not a valid argument")
)

// Credentials is what the token route takes: the client Flume's portal
// generates under API access, and the account it was generated for. All
// four are secrets. A Credentials formatted, logged or marshalled as JSON
// shows none of them; one kept in an unexported field of another struct
// is out of reach of these methods, and fmt prints it as it is.
type Credentials struct {
	ClientID     string
	ClientSecret string
	Username     string
	Password     string
}

// redacted is all a Credentials shows of itself.
const redacted = "flume credentials"

// Format keeps the credentials out of a formatted value, whatever the
// verb.
func (Credentials) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, redacted) }

// LogValue keeps them out of a log record.
func (Credentials) LogValue() slog.Value { return slog.StringValue(redacted) }

// MarshalJSON keeps them out of JSON.
func (Credentials) MarshalJSON() ([]byte, error) { return []byte(`"` + redacted + `"`), nil }

// Options shape a Client. The zero value talks to BaseURL through
// http.DefaultClient, for a device in this machine's time zone.
type Options struct {
	// BaseURL is where the API is; empty is BaseURL.
	BaseURL string
	// HTTP is the client every request goes through; nil is
	// http.DefaultClient.
	HTTP *http.Client
	// Zone is the time zone the account's devices are in. The API takes
	// a query's range and gives a reading's time as a wall clock there,
	// with no offset; nil is time.Local.
	Zone *time.Location
}

// Client is one account's access to the API. Its methods may be called
// from several goroutines.
//
//nolint:recvcheck // Format alone has a value receiver, so that a Client held by value is formatted by it too
type Client struct {
	base string
	http *http.Client
	zone *time.Location
	// Behind a func, not a pointer: where fmt cannot call Format, a Client
	// in another struct's unexported field, it prints through a pointer
	// for a verb it does not know, and never calls a func.
	held func() *secrets
}

// Format keeps what a Client holds out of a formatted value, whatever
// the verb, and whether the Client is held by value or by pointer.
func (Client) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, "flume client") }

// secrets is what a Client must not show: the credentials, and the token
// they bought.
type secrets struct {
	credentials Credentials

	mu      sync.Mutex
	token   string
	expires time.Time
}

// New is a client for the account the credentials are for. Nothing is
// sent until a method is called: the first asks for a token, and a call
// made within a minute of that token's end asks for another, by the
// password again and not by the refresh token, the password being held
// either way.
func New(credentials Credentials, opts Options) *Client {
	state := &secrets{credentials: credentials}

	client := &Client{
		base: opts.BaseURL,
		http: opts.HTTP,
		zone: opts.Zone,
		held: func() *secrets { return state },
	}

	if client.base == "" {
		client.base = BaseURL
	}

	if client.http == nil {
		client.http = http.DefaultClient
	}

	if client.zone == nil {
		//nolint:gosmopolitan // the default is a device in the house the program runs in; Options.Zone says otherwise
		client.zone = time.Local
	}

	return client
}

// envelope is what every answer is wrapped in.
type envelope struct {
	Success  bool            `json:"success"`
	Code     int             `json:"code"`
	Message  string          `json:"message"`
	Detailed json.RawMessage `json:"detailed"`
	Data     json.RawMessage `json:"data"`
}

// tokens is the token route's data.
type tokens struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

// grant is the token route's request.
type grant struct {
	GrantType    string `json:"grant_type"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Username     string `json:"username"`
	Password     string `json:"password"`
}

// call sends one request under the account's token and decodes the
// answer's data, always a list, into into.
func (c *Client) call(ctx context.Context, method, path string, query url.Values, body, into any) error {
	token, err := c.bearer(ctx)
	if err != nil {
		return err
	}

	return c.exchange(ctx, method, path, query, body, token, into)
}

// bearer is the token to send, asked for when there is none or the one
// held is at its end.
func (c *Client) bearer(ctx context.Context) (string, error) {
	held := c.held()

	held.mu.Lock()
	defer held.mu.Unlock()

	if held.token != "" && time.Now().Before(held.expires.Add(-renewAhead)) {
		return held.token, nil
	}

	var granted []tokens

	err := c.exchange(ctx, http.MethodPost, "/oauth/token", nil, grant{
		GrantType:    "password",
		ClientID:     held.credentials.ClientID,
		ClientSecret: held.credentials.ClientSecret,
		Username:     held.credentials.Username,
		Password:     held.credentials.Password,
	}, "", &granted)
	if err != nil {
		return "", err
	}

	if len(granted) == 0 || granted[0].AccessToken == "" {
		return "", fmt.Errorf("%w: the token route gave no token", ErrAnswer)
	}

	held.token = granted[0].AccessToken
	held.expires = time.Now().Add(time.Duration(granted[0].ExpiresIn) * time.Second)

	return held.token, nil
}

// exchange sends one request, with token when there is one, and decodes
// the answer's data when into is not nil.
func (c *Client) exchange(
	ctx context.Context,
	method, path string,
	query url.Values,
	body any,
	token string,
	into any,
) error {
	req, err := c.request(ctx, method, path, query, body, token)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAnswer+1))
	if err != nil {
		return fmt.Errorf("%s %s: reading the answer: %w", method, path, err)
	}

	if len(raw) > maxAnswer {
		return fmt.Errorf("%w: %s %s: over %d bytes", ErrAnswer, method, path, maxAnswer)
	}

	var answer envelope

	decodeErr := json.Unmarshal(raw, &answer)

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated ||
		decodeErr == nil && !answer.Success {
		return refusal(method, path, resp.StatusCode, &answer)
	}

	if decodeErr != nil {
		return fmt.Errorf("%w: %s %s: %w", ErrAnswer, method, path, decodeErr)
	}

	if into == nil {
		return nil
	}

	if err = json.Unmarshal(answer.Data, into); err != nil {
		return fmt.Errorf("%w: the data of %s %s: %w", ErrAnswer, method, path, err)
	}

	return nil
}

// request is one request as the API takes it: JSON both ways, the token
// as a bearer.
func (c *Client) request(
	ctx context.Context,
	method, path string,
	query url.Values,
	body any,
	token string,
) (*http.Request, error) {
	var payload io.Reader = http.NoBody

	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("%w: the body for %s: %w", ErrArgument, path, err)
		}

		payload = bytes.NewReader(raw)
	}

	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, target, payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s: %w", ErrArgument, method, path, err)
	}

	req.Header.Set("Accept", "application/json")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

// refusal is the error for an answer that is not a success: its class by
// the status, with Flume's code, message and details when it gave them.
// The token route refuses bad credentials with 400.
func refusal(method, path string, status int, answer *envelope) error {
	class := ErrRefused

	switch {
	case status == http.StatusTooManyRequests:
		class = ErrRateLimited
	case status == http.StatusNotFound:
		class = ErrNotFound
	case status == http.StatusUnauthorized, status == http.StatusForbidden,
		status == http.StatusBadRequest && path == "/oauth/token":
		class = ErrCredentials
	}

	said := answer.Message
	if details := string(answer.Detailed); details != "" && details != "null" {
		said += " " + details
	}

	return fmt.Errorf("%w: %s %s: HTTP %d, code %d: %s", class, method, path, status, answer.Code, said)
}

// only is the one element a route that answers one thing wraps in its
// list.
func only[T any](items []T, what string) (T, error) {
	if len(items) == 0 {
		var none T

		return none, fmt.Errorf("%w: no %s in the answer", ErrAnswer, what)
	}

	return items[0], nil
}

// devicePath is the path of a route on one device, the id escaped; an
// empty id is refused.
func devicePath(device, rest string) (string, error) {
	if device == "" {
		return "", fmt.Errorf("%w: an empty device id", ErrArgument)
	}

	return "/me/devices/" + url.PathEscape(device) + rest, nil
}
