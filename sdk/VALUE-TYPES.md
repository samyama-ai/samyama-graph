# Value types across the SDKs

What each engine value arrives as in Python, TypeScript and Rust (API-04).

Every SDK reads query results as JSON, in remote mode and in embedded mode
alike, so a value's JSON form decides what each SDK can hold. The table below
is checked, case by case, by one shared fixture,
[`fixtures/value_types.json`](fixtures/value_types.json). Each value is read
back as a column, inside a list, inside a map and as a node property:

| SDK | Suite | Runs |
|---|---|---|
| Rust | [`crates/samyama-sdk/tests/value_types_round_trip.rs`](../crates/samyama-sdk/tests/value_types_round_trip.rs) | remote (buffered and streamed, against the engine's own HTTP router) and embedded, in every `cargo test` |
| TypeScript | [`typescript/tests/value-types.test.ts`](typescript/tests/value-types.test.ts) | decoding, always; against a server when `SAMYAMA_URL` is set |
| Python | [`python/tests/test_value_types.py`](python/tests/test_value_types.py) | embedded, once the extension is built; remote when `SAMYAMA_URL` is set |

```bash
# A server for the remote halves
cargo run -- --ephemeral --port 16379 --http-port 18080
export SAMYAMA_URL=http://127.0.0.1:18080

cargo test -p samyama-sdk --test value_types_round_trip
(cd sdk/typescript && npm run build && node --test dist/tests/value-types.test.js)
(cd sdk/python && maturin develop && pytest tests/test_value_types.py)
```

## The mapping

| Engine value | JSON | Python | TypeScript | Rust (`serde_json::Value`) |
|---|---|---|---|---|
| Integer | number | `int` | `number`, **rounded beyond ±2^53** | `Number`, exact |
| Float | number, written with a fraction or exponent | `float` | `number`; **`1.0` arrives as `1`** | `Number`, kept apart from an integer |
| Float NaN, ±infinity | **`null`** | `None` | `null` | `Null` |
| Boolean | `true`/`false` | `bool` | `boolean` | `Bool` |
| String | string, any Unicode | `str` | `string` | `String` |
| Null | `null` | `None` | `null` | `Null` |
| Date, LocalTime, Time, LocalDateTime, DateTime | ISO-8601 string, as Cypher renders it: `"2024-02-29"`, `"12:31:14.645876123+05:30"`, `"2024-07-01T12:00+02:00[Europe/Stockholm]"` | `str` | `string` | `String` |
| Duration | `{"months", "days", "seconds", "nanos"}` | `dict` | object | `Object` |
| Point (`point()`) | `{"x", "y", "srid", "crs"}`, plus `latitude`/`longitude` for a geographic point | `dict` | object | `Object` |
| Vector | list of numbers, each `f32` written out exactly as a double (`0.1` → `0.10000000149011612`) | `list[float]` | `number[]` | `Array` |
| List | array, element by element | `list` | array | `Array` |
| Map | object, value by value | `dict` | object | `Object` |

## What does not survive, and why

These are the mapping, not bugs in one SDK. The suites check that each is still
true, so changing one shows up as a test to update.

1. **A non-finite float becomes `null`.** JSON has no NaN or infinity, so
   `0.0 / 0.0` and `1.0 / 0.0` both arrive as null, in every SDK.
2. **A temporal value arrives as a string.** The text is exact, to the
   nanosecond and the zone, but its type is not carried: a `date` and a string
   holding the same text decode to the same value. Parse it where the type is
   known.
3. **A duration and a point arrive as maps.** Nothing marks
   `{"months": 14, "days": 3, "seconds": 14706, "nanos": 500000000}` as a
   duration rather than a map with those keys. The engine has no point type:
   `point()` returns a map.
4. **A vector arrives as a list.** It cannot be told from a list of floats.
5. **TypeScript holds every number as a double.** An integer beyond 2^53
   (`9007199254740993`) is rounded by `JSON.parse`, and a whole float cannot be
   told from an integer. The fixture marks these `lossy.typescript`.
6. **A legacy `DateTime` arrives as an integer.** Values loaded from a
   snapshot written before the five temporal types existed are Unix
   milliseconds, and are sent as that number. Nothing new produces one.

## Fixed while writing this

Over HTTP, a list or map built from an expression (`[n.name]`,
`{k: n.age}`, `[x]`) sent each item as Rust debug text: `[n.name]` arrived as
`["Property(String(\"a\"))"]`, and a date inside one as `"Property(Date(19782))"`.
A list or map written only of literals did not show it, which is why it went
unseen. This affected every remote SDK; the embedded clients were not affected.
Lists and maps are now rendered value by value, the same way as a column
(`src/http/handler.rs`, `render_value`).

## Not covered yet

- **Parameters.** No SDK's public API takes `$name` parameters, so values go
  in as Cypher expressions here. Sending each type as a parameter is a second
  half of this suite, once the SDKs accept parameters.
- **CI for the TypeScript and Python halves against a server.** The CI SDK job
  does not start an engine, so those halves run where `SAMYAMA_URL` is set.
  The Rust suite and the TypeScript decoding half run in CI as they are.
