# go-flume

A Go library and command for the Flume 2 water monitor, over Flume's cloud API: the
devices on an account, a sensor's readings, whether water runs now, the usage alerts.
Written against Flume's documentation, and run against Flume's servers as one account:
[docs/design.md](docs/design.md) says which routes that covered.

The command reads the account from the environment and takes no credential as a flag:
`FLUME_CLIENT_ID` and `FLUME_CLIENT_SECRET`, generated under API access in the settings of
[Flume's portal](https://portal.flumewater.com), and `FLUME_USERNAME` and `FLUME_PASSWORD`,
the account's own. With those set:

```bash
just flume devices                          # bridges and sensors, connection, battery
just flume flow                             # is water running now, and how fast
just flume usage --last 6h --bucket minute  # readings; hour by default, over a day
just flume usage --last 720h --total        # one sum over the range
just flume usage --from 2025-01-01 --to 2025-07-01 --bucket month   # a past range
just flume alerts --leaks                   # what Flume's leak rule raised
```

Times are in this machine's zone unless `--zone` names the devices'. Flume allows 120
requests an hour; a command is two or three.

[docs/design.md](docs/design.md) is the research it starts from, what the device offers
and the ways into it, and the shape of the module with its decisions.
