// flume asks Flume's cloud API about an account's water monitors; the
// command is internal/cli.
package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/mycophonic/primordium/app"
	"github.com/mycophonic/primordium/app/shutdown"
	"github.com/mycophonic/primordium/network"

	"github.com/duncanite/go-flume/internal/cli"
)

// requestTimeout bounds one request to Flume's servers.
const requestTimeout = 30 * time.Second

func main() {
	app.Main(&app.Options{Name: "flume"}, func(ctx context.Context) error {
		client := &http.Client{Transport: network.NewTransport(), Timeout: requestTimeout}

		if code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, client, os.Getenv); code != cli.ExitOK {
			return shutdown.ExitError(code)
		}

		return nil
	})
}
