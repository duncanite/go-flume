# go-flume: the design

Research into the Flume 2 water monitor, done on 2026-11-15 from public sources, to decide
what a Go module for it would be. Nothing is built and nothing here was run against a
device. The decisions at the end are open.

In short: a Flume is a cloud product. Its one supported interface is a REST API on
Flume's servers. The bridge has no documented interface on the LAN. The traffic between
the bridge and Flume's servers has been reverse-engineered and can be read locally, at
the price of opening the bridge. It does not fit Matter beyond a leak flag.

## The device

Two parts ([FCC ID 2AOX8-F2200](https://fccid.io/2AOX8-F2200),
[FCC ID 2AOX8-F2100](https://fccid.io/2AOX8-F2100),
[Hackaday, 2024](https://hackaday.com/2024/12/18/learning-about-the-flume-water-monitor/)):

- **The sensor** straps to the water meter. It is battery-powered and reads, with a
  magnetometer, the motion of the meter's own mechanism. What it sends on is read as a
  count, not a volume (the MQTT write-up below).
- **The bridge** is mains-powered and indoors. It receives the sensor over a 915 MHz
  radio and forwards to Flume's servers over 2.4 GHz Wi-Fi. Its processor is an ESP8266
  (the filing lists the Wi-Fi module under FCC ID 2AHMR-ESP12).

Flume's servers turn the counts into volume, detect leaks, and serve the phone app, the
Alexa and Google integrations, and the API.

## How the parts talk

### Sensor to bridge: 915 MHz

- An RFM69HCW module at each end, hopping over 50 channels from 902.5 MHz, 2-FSK
  (the bridge's FCC filing; [a write-up of the radio][radio]).
- Firmware and configuration updates for the sensor travel over the same link (the
  filing).
- The payload is AES-128. The key is 64 bits expanded by a fixed mapping; with the
  header and other predictable values, 44 bits were unknown, and one was found by
  brute force in about a day on rented hardware ([the same write-up][radio]). Flume told
  its author it was already working on improvements.

### Bridge to Flume: MQTT

From [one owner's write-up][mqtt] and the tool published with it,
[flumewatch](https://github.com/stevecrozz/flumewatch):

- The bridge is a client. It connects to `mqtt.prod.flumetech.com` on port 1883: MQTT
  over plain TCP, no TLS, its MQTT credentials in the clear.
- It publishes on `2/<type>/12/<device_id>/<counter>` and subscribes to
  `responses/<device_id>/#`, for itself and for its sensor.
- Each payload is encrypted with libhydrogen's `hydro_secretbox` under a 32-byte key
  that is the device's own, stored in the bridge's flash at `0x3f9010`, the same key
  both ways. Flume confirmed to the author that each device has a random key.
- Decrypted, the messages are JSON. By type: flow (1), a timestamped cumulative counter
  in batches about every 30 seconds; status (2), with fields read as the sensor's
  battery voltage, signal strengths and a temperature; boot logs (3); events (4);
  heartbeats (6), about every ten minutes; identification (7), with the firmware's
  branch and commit.
- The counter is in pulses. flumewatch estimates about 40 to the gallon, from the
  volumes of known fixtures and not from a measurement.
- Many status fields and every event code are unidentified.

### Flume to the owner: the cloud API

From Flume's own documentation
([the help article](https://help.flumewater.com/en/articles/3108017-flume-personal-api),
[querying](https://flumetech.readme.io/docs/querying-samples),
[restrictions](https://flumetech.readme.io/docs/rate-limiting),
[the reference](https://flumetech.readme.io/reference/introduction)):

- REST, JSON, at `https://api.flumewater.com`. Flume calls it cloud to cloud, for
  personal use only, and subject to change at any time.
- OAuth2 password grant: a client id and secret made in the owner's Flume portal, with
  the account's user name and password. A route under `/users/{user_id}` also answers
  under `/me`.
- 120 requests an hour for a personal client; past that, 429.
- `POST /users/{user_id}/devices/{device_id}/query` takes a list of queries, each with a
  `request_id`, `since_datetime`, an optional `until_datetime`, a `bucket` (`MIN`, `HR`,
  `DAY`, `MON`, `YR`), and optionally a `group_multiplier` up to 100, an `operation`
  (`SUM`, `AVG`, `MIN`, `MAX`, `CNT`) and `units` (`GALLONS`, `LITERS`, `CUBIC_FEET`,
  `CUBIC_METERS`). A query spans a year at most. Times carry no offset and are the
  device's zone's.
- Also there: the account's devices, notifications, usage alerts and their rules, leak
  alerts, budgets.
- Every answer is one envelope: `success`, `code`, `message`, `http_code`, `data`, and
  links to the next and previous page on a list.

Existing clients of it: [PyFlume](https://pypi.org/project/pyflume), which Home
Assistant's integration uses, and
[flume-mqtt-bridge](https://github.com/geoffmyers/flume-mqtt-bridge), a daemon that
republishes it to MQTT and whose README lists what Home Assistant's integration leaves
out: the sensor's battery, the device's connection state, the leak history.

## Three ways in

| | The cloud API | The bridge's MQTT | The radio |
|---|---|---|---|
| Supported by Flume | yes, for personal use | no | no |
| Works without Flume's servers | no | the relay forwards to them; whether the bridge runs with no server behind it is not known | not known |
| Data | volume by the minute and up, alerts, device state | pulses about every 30 seconds, battery, signal | the same packets the bridge gets |
| Limit | 120 requests an hour | none known | none known |
| Needs | an account and API credentials | the bridge opened, its key read over UART with esptool, and port 1883 redirected on the router | a software radio and the key found by search |
| Cost | depends on a service that may change | Flume says a redirected bridge gets no firmware updates and no support; a firmware change can end it | Flume says it is working on the weakness that makes it possible |

Reading the key off the bridge does not modify it: the write-up reads the flash in the
ESP8266's UART download mode and the bridge boots as before. It is the redirect that
costs the updates and the support. In the same exchange Flume raised a developer mode
that would let a bridge publish to a server of the owner's as well; nothing says it
exists.

## Matter

Flume has no Matter support and no HomeKit support, and no plan for either was found
([a 2026 review](https://smarthomecompared.com/water-leak-sensors/flume-2-smart-water-monitor),
[a Matter buying guide](https://microcontrollerslab.com/best-matter-water-flow-sensors-buying-guide/)).

What Matter has for water, read in the reference implementation,
[connectedhomeip](https://github.com/project-chip/connectedhomeip), under
`src/app/zap-templates/zcl/data-model/chip/`:

- **A leak:** the Water Leak Detector device type (0x0043), one boolean. Flume's leak
  alert fits it.
- **A flow rate:** the Flow Measurement cluster (0x0404), `MeasuredValue` as an unsigned
  16-bit integer (`flow-measurement-cluster.xml`).
- **A volume consumed:** nothing. Commodity Metering (0x0B07, new in Matter 1.5,
  provisional) holds metered quantities with a `TariffUnit` whose highest value is 1
  (`commodity-metering-cluster.xml`).

So a Matter bridge for a Flume would carry the leak flag and lose the consumption, which
is what the device is bought for.

## What this module would be

Not decided. The options, smallest first:

1. **A client of the cloud API**, a library and a command like the other device modules:
   the token, the devices, the queries, the alerts, the sensor's battery and connection.
   Credentials from the environment. A reading every minute or two fits the hourly
   limit. It stops when Flume's service or its terms do.
2. **A reader of the bridge's MQTT**, in place of flumewatch or beside it: the relay, the
   decryption, the messages as types. It needs the key from the bridge, a redirect on
   the network, and a pulse-to-volume factor measured on the meter. It gives up the
   bridge's firmware updates.
3. **The radio, or a Matter device.** Neither is proposed: the first rests on a weakness
   being closed, the second cannot carry the data.

## Not verified

- Nothing above was observed on a device. The bridge's LAN side was not examined: which
  ports it listens on, if any, is not known. A client-only ESP8266 would listen on none.
- Which hardware generation the MQTT write-up's bridge is. The FCC filings read here are
  the Flume 2's (F2100, F2200); a later sensor (F2600) and a newer product exist.
- The unit of Matter's flow `MeasuredValue`. From memory of the specification it is
  tenths of a cubic metre an hour, steps of 100 litres an hour, too coarse for a
  household leak; the cluster's definition in the SDK does not state the unit.
- What `TariffUnit`'s two values are. They are taken to be units of electrical energy;
  the enum itself was not read.
- How late the cloud API's readings are. One Home Assistant user's automation allows two
  and a half minutes; Flume states no figure in what was read.
- The full list of API routes and their fields. The reference's introduction was read,
  not each route; the routes named above are from the guides and from PyFlume's modules.
- Whether a paid Flume plan is needed for any route.
- The radio details beyond the filing and one summary of the original article; the
  original was not read.

## Sources

- Flume: [the personal API](https://help.flumewater.com/en/articles/3108017-flume-personal-api),
  [querying samples](https://flumetech.readme.io/docs/querying-samples),
  [API restrictions](https://flumetech.readme.io/docs/rate-limiting),
  [the API's introduction](https://flumetech.readme.io/reference/introduction).
- FCC filings: [the bridge, 2AOX8-F2200](https://fccid.io/2AOX8-F2200),
  [the sensor, 2AOX8-F2100](https://fccid.io/2AOX8-F2100).
- The bridge's traffic: [Decrypting Flume water monitor traffic][mqtt],
  [flumewatch](https://github.com/stevecrozz/flumewatch),
  [Hackaday, 2026](https://hackaday.com/2026/08/14/snooping-flume-water-monitor-data-on-the-wire/),
  [Hackaday, 2024](https://hackaday.com/2024/12/18/learning-about-the-flume-water-monitor/).
- The radio: [Decrypting a water monitor's radio signal][radio].
- Other clients: [PyFlume](https://pypi.org/project/pyflume),
  [flume-mqtt-bridge](https://github.com/geoffmyers/flume-mqtt-bridge).
- Matter: [connectedhomeip](https://github.com/project-chip/connectedhomeip),
  [matterbridge's notes on Matter 1.5.1](https://github.com/Luligu/matterbridge/blob/2327c88be31a54c77b012047db599197542b20b4/Matter-1.5.1.md).

[mqtt]: https://lithostech.com/2026/08/decrypting-flume-water-monitor-traffic/
[radio]: https://joshuamorris.info/notes/2026/08/02/decrypting-a-water-monitors-radio-signal/
