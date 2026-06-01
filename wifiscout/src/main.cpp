// Snoopy WiFi Scout — sonda diagnostica WiFi su ESP B (WROOM-32).
//
// Cosa fa:
//  1) PASSIVO: ogni SCAN_INTERVAL fa uno scan completo e pubblica, per OGNI
//     rete vista (anche quelle a cui non è connesso), SSID/BSSID/RSSI/canale.
//     -> risponde a "di chi è il segnale e quanto è forte" (router vs repeater).
//  2) ATTIVO: ogni PROBE_INTERVAL prova ad agganciarsi al repeater (PROBE_SSID)
//     e riporta l'esito: connesso, oppure il MOTIVO del rifiuto (AUTH_FAIL,
//     NO_AP_FOUND, HANDSHAKE_TIMEOUT, ...). Poi torna sul router e pubblica.
//     -> risponde a "il repeater non mi lascia entrare".
//
// Sta normalmente connesso al router (HOME_*) per pubblicare su MQTT; i topic
// finiscono in InfluxDB via Telegraf (consumer wifi_scan / wifi_probe).

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include "secrets.h"

// ---- parametri ----
static const uint32_t SCAN_INTERVAL_MS  = 20000;   // scan passivo
static const uint32_t PROBE_INTERVAL_MS = 300000;  // sonda attiva al repeater (5 min)
static const uint32_t PROBE_TIMEOUT_MS  = 15000;   // attesa aggancio repeater
static const uint32_t HOME_TIMEOUT_MS   = 20000;   // attesa ritorno sul router
static const uint32_t MQTT_TIMEOUT_MS   = 8000;

static const char* TOPIC_AP     = "snoopy/wifiscan/ap";
static const char* TOPIC_PROBE  = "snoopy/wifiscan/probe";
static const char* TOPIC_STATUS = "snoopy/wifiscan/status";
static const char* CLIENT_ID    = "snoopy-wifiscout";

WiFiClient   net;
PubSubClient mqtt(net);

volatile uint8_t lastDiscReason = 0;   // motivo dell'ultimo disconnect (event handler)
uint32_t lastScan = 0, lastProbe = 0;
long     repeaterRssiSeen = 0;         // ultimo RSSI del repeater visto in scan
bool     repeaterVisible  = false;

// Mappa numerica dei reason code WiFi (stabile tra versioni del core).
const char* reasonStr(uint8_t r) {
  switch (r) {
    case 1:   return "UNSPECIFIED";
    case 2:   return "AUTH_EXPIRE";
    case 4:   return "ASSOC_EXPIRE";
    case 5:   return "ASSOC_TOOMANY";
    case 6:   return "NOT_AUTHED";
    case 7:   return "NOT_ASSOCED";
    case 8:   return "ASSOC_LEAVE";
    case 15:  return "4WAY_HANDSHAKE_TIMEOUT";
    case 200: return "BEACON_TIMEOUT";
    case 201: return "NO_AP_FOUND";
    case 202: return "AUTH_FAIL";
    case 203: return "ASSOC_FAIL";
    case 204: return "HANDSHAKE_TIMEOUT";
    case 205: return "CONNECTION_FAIL";
    default:  return "OTHER";
  }
}

void onWiFiEvent(WiFiEvent_t event, WiFiEventInfo_t info) {
  if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
    lastDiscReason = info.wifi_sta_disconnected.reason;
  }
}

bool connectHome() {
  WiFi.disconnect(true, true);
  delay(150);
  WiFi.begin(HOME_SSID, HOME_PASS);
  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < HOME_TIMEOUT_MS) delay(200);
  return WiFi.status() == WL_CONNECTED;
}

void ensureMqtt() {
  if (WiFi.status() != WL_CONNECTED || mqtt.connected()) return;
  uint32_t t0 = millis();
  while (!mqtt.connected() && millis() - t0 < MQTT_TIMEOUT_MS) {
    mqtt.connect(CLIENT_ID);
    if (!mqtt.connected()) delay(500);
  }
}

void publishStatus(const char* state) {
  if (!mqtt.connected()) return;
  JsonDocument d;
  d["state"]     = state;
  d["ip"]        = WiFi.localIP().toString();
  d["home_rssi"] = WiFi.RSSI();
  char buf[180];
  size_t n = serializeJson(d, buf);
  mqtt.publish(TOPIC_STATUS, (const uint8_t*)buf, n, true);
}

void doScan() {
  int n = WiFi.scanNetworks(false /*sync*/, true /*show hidden*/);
  repeaterVisible = false;
  for (int i = 0; i < n; i++) {
    JsonDocument d;
    d["ssid"]  = WiFi.SSID(i);
    d["bssid"] = WiFi.BSSIDstr(i);
    d["rssi"]  = WiFi.RSSI(i);
    d["ch"]    = WiFi.channel(i);
    if (WiFi.SSID(i) == String(PROBE_SSID)) {
      repeaterVisible   = true;
      repeaterRssiSeen  = WiFi.RSSI(i);
    }
    char buf[200];
    size_t len = serializeJson(d, buf);
    mqtt.publish(TOPIC_AP, (const uint8_t*)buf, len, false);
    mqtt.loop();
    delay(10);
  }
  WiFi.scanDelete();
}

void doProbe() {
  publishStatus("probe_start");

  lastDiscReason = 0;
  WiFi.disconnect(true, true);
  delay(200);
  WiFi.begin(PROBE_SSID, PROBE_PASS);
  uint32_t t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < PROBE_TIMEOUT_MS) delay(200);

  bool     ok     = (WiFi.status() == WL_CONNECTED);
  long     prssi  = ok ? WiFi.RSSI() : repeaterRssiSeen;  // se fallisce, usa l'RSSI visto in scan
  uint8_t  reason = lastDiscReason;
  uint32_t took   = millis() - t0;

  // torna sul router per poter pubblicare
  bool home = connectHome();
  ensureMqtt();

  JsonDocument d;
  d["target"]      = PROBE_SSID;
  d["result"]      = ok ? "ok" : "fail";
  d["reason"]      = ok ? "CONNECTED" : reasonStr(reason);
  d["reason_code"] = ok ? 0 : reason;
  d["rssi"]        = prssi;            // RSSI del repeater (anche se l'aggancio è fallito)
  d["visible"]     = repeaterVisible ? 1 : 0;
  d["ms"]          = took;
  char buf[256];
  size_t len = serializeJson(d, buf);
  if (home && mqtt.connected())
    mqtt.publish(TOPIC_PROBE, (const uint8_t*)buf, len, false);

  if (!home) ESP.restart();  // se non si riaggancia a casa, riparte pulito
}

void setup() {
  Serial.begin(115200);
  WiFi.persistent(false);
  WiFi.mode(WIFI_STA);
  WiFi.setHostname("snoopy-wifiscout");
  WiFi.onEvent(onWiFiEvent);
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setBufferSize(512);
  connectHome();
  ensureMqtt();
  publishStatus("boot");
  lastProbe = millis();  // non sondare subito al boot: prima un po' di scan
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) connectHome();
  ensureMqtt();
  mqtt.loop();

  uint32_t now = millis();
  if (now - lastScan >= SCAN_INTERVAL_MS) { lastScan = now; doScan(); }
  if (now - lastProbe >= PROBE_INTERVAL_MS) { lastProbe = now; doProbe(); }

  delay(50);
}
