# Writing from a microcontroller

How an ESP32, an Arduino-class board, or anything else with a few tens of
kilobytes of RAM writes to a Samyama Edge node.

Reference sketch: [`examples/esp32/samyama_esp32.ino`](../examples/esp32/samyama_esp32.ino).

---

## Use RESP, not HTTP

| | RESP | HTTP |
|---|---|---|
| to send a write | one TCP write, ~80 bytes | request line, headers, JSON body |
| to read the reply | look at the first byte | parse status, headers, JSON |
| on the device | a socket | TLS stack, JSON encoder, chunked reader |
| authentication | **none available** | bearer token |

On a constrained board the second column is most of the flash budget and all of
the RAM headroom. The first column is a `snprintf` and a `write`.

The exception is the last row, and it is not a small one. Read the security
section before choosing.

## The frame

`GRAPH.QUERY <graph> <statement>` as a RESP array of bulk strings:

```
*3\r\n$11\r\nGRAPH.QUERY\r\n$7\r\ndefault\r\n$52\r\nCREATE (:Reading {sensor:'esp32-01', celsius:21.50})\r\n
```

- `*3` — three arguments follow.
- `$11` then `GRAPH.QUERY` — each argument is a byte count, CRLF, the bytes, CRLF.
- Lengths are **bytes, not characters**. `strlen` is right; anything that
  counts characters is wrong the first time a statement carries non-ASCII.
- Every terminator is CRLF. A bare LF is not accepted.

An overstated length leaves the socket mid-frame and every later reply arrives
one command behind, which looks like the server answering the wrong question.

## The reply

The first byte decides it: `-` is an error, anything else is a result.

Read to the end of the line even when you do not care about the content. A
client that reads one byte and moves on leaves the rest in the socket and
desynchronises the next command.

## Security: the RESP port has no authentication

There is no `AUTH` command. The server accepts `GRAPH.QUERY`, `GRAPH.RO_QUERY`,
`GRAPH.DELETE`, `GRAPH.LIST`, `PING`, `ECHO` and `INFO` from anyone who can
open the socket, and `GRAPH.QUERY` writes.

So:

- **Do not expose the RESP port to a network the sensors do not already
  trust.** Bind it to the sensor VLAN, not to `0.0.0.0` on a routable
  interface.
- Use `GRAPH.RO_QUERY` for anything that only reads. It refuses writes, so a
  compromised read-only device cannot modify the graph.
- If you need authentication, use `POST /api/query` with a bearer token and
  accept the TLS and JSON cost on the device. That is a real trade, not a
  formality: on an ESP32 it is the difference between a sketch that fits
  comfortably and one that does not.

## Escaping — or, better, binding

`GRAPH.QUERY` takes optional trailing `key value` pairs, and their values are
**bound**: they reach the engine as values and are never re-read as Cypher
(#1463). So a value that arrived from somewhere else goes in a pair, not in the
statement text:

```text
*7\r\n$11\r\nGRAPH.QUERY\r\n$7\r\ndefault\r\n$41\r\nCREATE (:Reading {sensor:$s, celsius:$c})\r\n$1\r\ns\r\n$8\r\nesp32-01\r\n$1\r\nc\r\n$5\r\n21.50\r\n
```

A value is read as JSON when it parses as JSON (`21.5` a float, `true` a
boolean, `[1,2]` a list) and as a plain string when it does not, so `s esp32-01`
needs no quoting and `s "12"` is the way to bind the *string* `12`. A key the
statement never mentions is refused rather than dropped, because a dropped key
is a typo nobody sees.

The old advice, for a statement you still build by hand: format numbers you
produced yourself, and do not interpolate a string that arrived from anywhere
else — another device, a configuration server, a received packet. The sketch
formats only its own floats for that reason. With bound pairs available, a
statement that interpolates a received string is now a choice rather than a
constraint.

## What is verified, and what is not

[`tests/esp32_sketch_speaks_resp.rs`](../tests/esp32_sketch_speaks_resp.rs)
reads the sketch, rebuilds the frame from the format string in it, and pushes
the bytes through the server's own RESP decoder and query engine. Editing the
sketch's wire format without editing the protocol fails that test.

It has **not** been run on an ESP32. It says the bytes are right; it does not
say the sketch fits in flash, links against a particular core version, or
holds up over a flaky radio link. API-15's next stage asks for a real device
and that is still outstanding.
