// Write to a Samyama Edge node from an ESP32 over RESP (API-15).
//
// Why RESP and not HTTP: a RESP command is one TCP write of a few dozen bytes
// and the reply is parsed by looking at the first byte. HTTP on a
// microcontroller costs a TLS stack, a JSON encoder and a chunked-transfer
// reader, which on an ESP32 is most of the flash budget and all of the RAM
// headroom. Nothing here allocates on the heap.
//
// READ THIS BEFORE DEPLOYING: the RESP surface has no AUTH command. Anything
// that can open a socket to this port can write to the graph. Put the Edge
// node on a segment the sensors already trust, or use HTTP with a bearer token
// and pay for the TLS stack. See docs/CONSTRAINED-CLIENTS.md.
//
// Verified by tests/esp32_sketch_speaks_resp.rs, which reads THIS FILE, rebuilds
// the frame from the format string below, and pushes it through the server's
// own decoder and query engine. Editing the format string without editing the
// protocol breaks that test. It does not verify anything about the board.

#include <WiFi.h>

static const char* WIFI_SSID = "your-ssid";
static const char* WIFI_PASS = "your-password";
static const char* HOST      = "192.168.1.10";  // the Edge node
static const uint16_t PORT   = 6379;            // RESP

static const char* GRAPH = "default";

// RESP array of bulk strings: GRAPH.QUERY <graph> <cypher>
//
// *3            three arguments follow
// $11 ...       each argument is a bulk string: byte length, CRLF, bytes, CRLF
//
// Lengths are byte counts, not character counts. Keep the Cypher ASCII and the
// two are the same; send UTF-8 and strlen() is still correct because it counts
// bytes, but do not compute the length any other way.
#define SAMYAMA_RESP_FMT \
  "*3\r\n$11\r\nGRAPH.QUERY\r\n$%u\r\n%s\r\n$%u\r\n%s\r\n"

static WiFiClient client;

// One reading, as a parameterless Cypher write.
//
// The value is formatted into the statement rather than sent as a parameter:
// RESP GRAPH.QUERY takes no parameter list, so there is nothing to bind to.
// That makes escaping the caller's problem, which is why this only ever
// formats numbers it produced itself. Never interpolate a string you received
// from somewhere else into this buffer.
static size_t buildFrame(char* out, size_t cap, const char* cypher) {
  return snprintf(out, cap, SAMYAMA_RESP_FMT,
                  (unsigned)strlen(GRAPH), GRAPH,
                  (unsigned)strlen(cypher), cypher);
}

static bool sendReading(float celsius) {
  char cypher[192];
  snprintf(cypher, sizeof(cypher),
           "CREATE (:Reading {sensor:'esp32-01', celsius:%.2f})", celsius);

  char frame[320];
  size_t n = buildFrame(frame, sizeof(frame), cypher);
  if (n == 0 || n >= sizeof(frame)) return false;   // truncated; do not send

  if (!client.connected() && !client.connect(HOST, PORT)) return false;
  if (client.write((const uint8_t*)frame, n) != n) return false;

  // The reply's first byte is the whole verdict: '-' is an error, anything
  // else is a result. Reading only the first byte and moving on would leave
  // the rest of the reply in the socket and put the next command out of step
  // with its own answer, so drain to the end of the line.
  unsigned long deadline = millis() + 3000;
  int first = -1;
  while (millis() < deadline) {
    if (client.available()) { first = client.read(); break; }
    delay(5);
  }
  if (first < 0) return false;                       // timed out

  while (millis() < deadline && client.available()) {
    if (client.read() == '\n') break;
  }
  return first != '-';
}

void setup() {
  Serial.begin(115200);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) { delay(250); Serial.print('.'); }
  Serial.println(WiFi.localIP());
}

void loop() {
  float celsius = 20.0 + (float)(millis() % 1000) / 100.0;  // stand-in sensor
  Serial.println(sendReading(celsius) ? "ok" : "failed");
  delay(10000);
}
