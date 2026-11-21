// =============================================================================
//  TANK SIEGE - Authoritative Multiplayer Tank Battle Server (single file, zero deps)
//  Build : g++ -std=c++17 -O2 -Wall -o server server.cpp
//  Run   : ./server           then open the printed URL on 2+ devices (same Wi-Fi)
//  Target: Linux / macOS / WSL (POSIX sockets + poll()).
//
//  Transport note: browsers cannot send raw UDP, so the wire is a hand-written
//  RFC 6455 WebSocket (own SHA-1 + Base64, no libraries). To get UDP *semantics*
//  on top of it, every input carries (epoch, seq, timestamp) and the server
//  drops anything stale / duplicated / out of order, exactly as a UDP game
//  protocol must, because the game never relies on ordered delivery.
//
//  Threading: one thread, non-blocking sockets, poll() event loop + fixed 60 Hz
//  tick. No locks, no races, deterministic ordering of all state mutation.
// =============================================================================
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>                   
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

// ----------------------------- Tunable constants -----------------------------
constexpr int    PORT              = 8080;
constexpr int    TICK_HZ           = 60;       // authoritative simulation + broadcast rate
constexpr double ARENA_W           = 800.0;
constexpr double ARENA_H           = 600.0;
constexpr double PLAYER_R          = 16.0;     // tank collision radius (shared with client)
constexpr double MAX_SPEED         = 250.0;    // px/s the server tolerates (legit client = 220)
constexpr double MAX_ALLOWED_DRIFT = 60.0;     // px of jitter slack (cumulative "bank", see handleInput)
constexpr double MAX_DELTA_SEC     = 0.5;      // cap so "idle then teleport" cannot farm allowance
constexpr double BULLET_SPEED      = 560.0;
constexpr double BULLET_R          = 4.0;
constexpr double BULLET_LIFE_MS    = 1400.0;
constexpr double FIRE_COOLDOWN_MS  = 280.0;    // enforced server-side: clients cannot rapid-fire
constexpr double RESPAWN_MS        = 2000.0;
constexpr int    MAX_HP            = 100;
constexpr int    BULLET_DMG        = 25;       // 4 hits to destroy a tank
constexpr int    WIN_KILLS         = 8;        // first to this many kills wins
constexpr size_t MAX_BULLETS       = 96;
constexpr size_t ROOM_CAP          = 4;        // lobby capacity
constexpr size_t MIN_PLAYERS       = 2;        // match auto-starts when >= this many are all ready
constexpr int    MAX_VIOLATIONS    = 15;       // kick persistent cheaters
constexpr int    MAX_MSGS_PER_SEC  = 400;      // per-client flood protection
constexpr double kPi               = 3.14159265358979323846;

using Clock = std::chrono::steady_clock;
static double nowMs() {  // monotonic server clock in milliseconds
    return std::chrono::duration<double, std::milli>(Clock::now().time_since_epoch()).count();
}

// Static map geometry. MUST match WALLS in index.html: both sides run identical collision.
struct Wall { double x, y, w, h; };
static const Wall WALLS[] = {{350, 250, 100, 100}, {160, 140, 140, 20}, {500, 440, 140, 20},
                             {150, 330, 20, 120},  {630, 150, 20, 120}};
static const double SPAWNS[4][2] = {{70, 70}, {730, 530}, {730, 70}, {70, 530}};

// ------------------------------- Data model ----------------------------------
struct Client {
    int fd = -1;
    uint32_t id = 0;                         // unique player id shown to everyone
    bool handshaken = false, dead = false, closeAfterFlush = false;   // dead = connection closed
    std::string in, out;                     // raw receive / send buffers
    std::string room;                        // "" = not in a room
    bool ready = false;
    // authoritative simulation state
    double x = 0, y = 0, aim = 0;
    int score = 0, hp = MAX_HP;              // score = kills
    bool alive = true, dirty = false;        // dirty = changed since last snapshot (delta compression)
    double respawnAt = 0, lastFire = 0;
    // anti-cheat / ordering state
    uint32_t epoch = 0;                      // bumped on rollback / respawn / match start; stale-epoch inputs ignored
    uint32_t lastSeq = 0, ackSeq = 0;        // highest seen / highest *accepted* sequence number
    double lastTs = -1;                      // last client timestamp (must never go backwards)
    double lastValidMs = 0;                  // server time of last VALID input
    double driftBank = 0;                    // cumulative excess distance (see handleInput)
    int violations = 0;
    double rateWin = 0; int rateCnt = 0;
};
struct Bullet { uint32_t id, owner; double x, y, vx, vy, born; };
struct Room {
    std::string id;
    std::vector<int> members;                // client fds
    bool inMatch = false, forceFull = false;
    uint32_t tick = 0, nextBullet = 1;
    std::vector<Bullet> bullets;
    std::vector<std::pair<uint32_t, uint32_t>> events;   // (killer, victim) kill feed for next snapshot
};

static std::unordered_map<int, Client>        g_clients;   // fd -> client
static std::unordered_map<std::string, Room>  g_rooms;     // roomId -> room  (Module 2)
static std::vector<int>                       g_dead;      // fds scheduled for removal
static std::mt19937                           g_rng{std::random_device{}()};
static uint32_t g_nextId = 1;
static int      g_nextRoom = 1;
static struct { uint64_t accepted = 0, rejectedCheat = 0, droppedStale = 0, ticks = 0; } g_stats;

// ------------------------------ Small utilities ------------------------------
static std::string fmt(double v) { char b[32]; snprintf(b, sizeof b, "%.2f", v); return b; }
static double clampd(double v, double lo, double hi) { return v < lo ? lo : (v > hi ? hi : v); }
static double rnd(double lo, double hi) { return std::uniform_real_distribution<double>(lo, hi)(g_rng); }
static std::vector<std::string> split(const std::string& s, char d) {
    std::vector<std::string> o; std::string cur;
    for (char c : s) { if (c == d) { o.push_back(cur); cur.clear(); } else cur += c; }
    o.push_back(cur); return o;
}
static bool parseD(const std::string& s, double& o) {
    char* e; o = strtod(s.c_str(), &e);
    return e != s.c_str() && *e == 0 && std::isfinite(o);
}
static bool parseU(const std::string& s, uint32_t& o) {
    char* e; unsigned long long v = strtoull(s.c_str(), &e, 10);
    if (e == s.c_str() || *e != 0 || v > 0xFFFFFFFFull) return false;
    o = (uint32_t)v; return true;
}
static std::string errJson(const std::string& m) { return "{\"t\":\"err\",\"m\":\"" + m + "\"}"; }

// --------------------- SHA-1 + Base64 (needed for WS handshake) --------------
static std::string sha1(const std::string& s) {
    uint32_t h0 = 0x67452301, h1 = 0xEFCDAB89, h2 = 0x98BADCFE, h3 = 0x10325476, h4 = 0xC3D2E1F0;
    std::string m = s; uint64_t bits = (uint64_t)s.size() * 8;
    m.push_back((char)0x80);
    while (m.size() % 64 != 56) m.push_back(0);
    for (int i = 7; i >= 0; --i) m.push_back((char)((bits >> (i * 8)) & 0xFF));
    auto rol = [](uint32_t v, int n) { return (v << n) | (v >> (32 - n)); };
    for (size_t o = 0; o < m.size(); o += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i)
            w[i] = ((uint32_t)(uint8_t)m[o + 4 * i] << 24) | ((uint32_t)(uint8_t)m[o + 4 * i + 1] << 16) |
                   ((uint32_t)(uint8_t)m[o + 4 * i + 2] << 8) | (uint32_t)(uint8_t)m[o + 4 * i + 3];
        for (int i = 16; i < 80; ++i) w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20)      { f = (b & c) | (~b & d);          k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ d;                   k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
            else             { f = b ^ c ^ d;                   k = 0xCA62C1D6; }
            uint32_t t = rol(a, 5) + f + e + k + w[i];
            e = d; d = c; c = rol(b, 30); b = a; a = t;
        }
        h0 += a; h1 += b; h2 += c; h3 += d; h4 += e;
    }
    std::string out; uint32_t hs[5] = {h0, h1, h2, h3, h4};
    for (uint32_t h : hs) for (int i = 3; i >= 0; --i) out.push_back((char)((h >> (i * 8)) & 0xFF));
    return out;
}
static std::string base64(const std::string& in) {
    static const char* T = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string o; size_t i = 0;
    for (; i + 2 < in.size(); i += 3) {
        uint32_t v = ((uint8_t)in[i] << 16) | ((uint8_t)in[i + 1] << 8) | (uint8_t)in[i + 2];
        o += T[v >> 18]; o += T[(v >> 12) & 63]; o += T[(v >> 6) & 63]; o += T[v & 63];
    }
    if (i + 1 == in.size()) {
        uint32_t v = (uint8_t)in[i] << 16;
        o += T[v >> 18]; o += T[(v >> 12) & 63]; o += "==";
    } else if (i + 2 == in.size()) {
        uint32_t v = ((uint8_t)in[i] << 16) | ((uint8_t)in[i + 1] << 8);
        o += T[v >> 18]; o += T[(v >> 12) & 63]; o += T[(v >> 6) & 63]; o += '=';
    }
    return o;
}

// ------------------------------ Socket output --------------------------------
static void markDead(Client& c) { if (!c.dead) { c.dead = true; g_dead.push_back(c.fd); } }

static void flushClient(Client& c) {
    while (!c.out.empty() && !c.dead) {
        ssize_t n = send(c.fd, c.out.data(), c.out.size(), 0);
        if (n > 0) c.out.erase(0, (size_t)n);
        else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        else { markDead(c); return; }
    }
    if (c.out.empty() && c.closeAfterFlush) markDead(c);
}
// Encode one unmasked server->client WebSocket frame.
static void sendFrame(Client& c, uint8_t op, const std::string& p) {
    if (c.dead) return;
    c.out.push_back((char)(0x80 | op));
    if (p.size() < 126) c.out.push_back((char)p.size());
    else if (p.size() < 65536) { c.out.push_back(126); c.out.push_back((char)(p.size() >> 8)); c.out.push_back((char)(p.size() & 0xFF)); }
    else { c.out.push_back(127); for (int i = 7; i >= 0; --i) c.out.push_back((char)(((uint64_t)p.size() >> (i * 8)) & 0xFF)); }
    c.out += p;
    if (c.out.size() > (1u << 20)) { markDead(c); return; }   // back-pressure: drop hopelessly slow client
    flushClient(c);
}
static void sendText(Client& c, const std::string& s) { sendFrame(c, 0x1, s); }

// ----------------------- Module 2: rooms & matchmaking -----------------------
// Circle-vs-wall test (shared rule with the client). Bullets use r = 0.
static bool hitsWall(double x, double y, double r) {
    for (const Wall& w : WALLS) {
        double cx = clampd(x, w.x, w.x + w.w), cy = clampd(y, w.y, w.y + w.h);
        if ((x - cx) * (x - cx) + (y - cy) * (y - cy) <= r * r) return true;
    }
    return false;
}
static std::string lobbyJson(const Room& r) {
    std::string s = "{\"t\":\"lobby\",\"room\":\"" + r.id + "\",\"cap\":" + std::to_string(ROOM_CAP) +
                    ",\"min\":" + std::to_string(MIN_PLAYERS) + ",\"state\":\"" + (r.inMatch ? "match" : "lobby") + "\",\"players\":[";
    bool first = true;
    for (int fd : r.members) {
        const Client& c = g_clients.at(fd);
        if (c.dead) continue;
        if (!first) s += ",";
        first = false;
        s += "{\"id\":" + std::to_string(c.id) + ",\"ready\":" + (c.ready ? "true" : "false") +
             ",\"score\":" + std::to_string(c.score) + "}";
    }
    return s + "]}";
}
static void broadcastLobby(Room& r) {
    std::string j = lobbyJson(r);
    for (int fd : r.members) sendText(g_clients[fd], j);
}
// Room browser: lets a second device click a room instead of typing its name.
static void sendRooms(Client& c) {
    std::string s = "{\"t\":\"rooms\",\"list\":[";
    bool first = true;
    for (auto& kv : g_rooms) {
        if (!first) s += ",";
        first = false;
        s += "{\"id\":\"" + kv.first + "\",\"n\":" + std::to_string(kv.second.members.size()) +
             ",\"cap\":" + std::to_string(ROOM_CAP) + ",\"m\":" + (kv.second.inMatch ? "1" : "0") + "}";
    }
    sendText(c, s + "]}");
}
static void respawn(Client& c, double now, const char* why) {
    int k = (int)(c.id + (uint32_t)(rnd(0, 4))) % 4;
    c.x = SPAWNS[k][0]; c.y = SPAWNS[k][1]; c.hp = MAX_HP; c.alive = true; c.dirty = true;
    c.epoch++; c.lastValidMs = now; c.driftBank = 0;
    sendText(c, "{\"t\":\"rb\",\"epoch\":" + std::to_string(c.epoch) + ",\"x\":" + fmt(c.x) + ",\"y\":" + fmt(c.y) +
                ",\"seq\":" + std::to_string(c.lastSeq) + ",\"why\":\"" + why + "\"}");
}
static void startMatch(Room& r) {
    r.inMatch = true; r.tick = 0; r.forceFull = true; r.bullets.clear(); r.events.clear();
    double t = nowMs(); size_t n = r.members.size();
    for (size_t i = 0; i < n; ++i) {
        Client& c = g_clients[r.members[i]];
        c.x = SPAWNS[i % 4][0]; c.y = SPAWNS[i % 4][1]; c.aim = 0;
        c.score = 0; c.hp = MAX_HP; c.alive = true; c.dirty = true; c.epoch++; c.lastSeq = 0; c.ackSeq = 0;
        c.lastTs = -1; c.lastValidMs = t; c.driftBank = 0; c.violations = 0; c.lastFire = 0;
        sendText(c, "{\"t\":\"start\",\"room\":\"" + r.id + "\",\"epoch\":" + std::to_string(c.epoch) +
                    ",\"x\":" + fmt(c.x) + ",\"y\":" + fmt(c.y) + "}");
    }
    broadcastLobby(r);
    printf("[room %s] match started with %zu players\n", r.id.c_str(), n);
}
static void endMatch(Room& r, uint32_t winner) {
    r.inMatch = false; r.bullets.clear();
    std::string sc = "[";
    for (size_t i = 0; i < r.members.size(); ++i) {
        Client& c = g_clients[r.members[i]];
        sc += (i ? "," : "") + std::string("[") + std::to_string(c.id) + "," + std::to_string(c.score) + "]";
        c.ready = false;
    }
    sc += "]";
    std::string j = "{\"t\":\"end\",\"winner\":" + std::to_string(winner) + ",\"scores\":" + sc + "}";
    for (int fd : r.members) sendText(g_clients[fd], j);
    broadcastLobby(r);
    printf("[room %s] match ended, winner=%u\n", r.id.c_str(), winner);
}
// Auto-start: enough connected players and everyone ready.
static void tryStart(Room& r) {
    if (r.inMatch || r.members.size() < MIN_PLAYERS) return;
    for (int fd : r.members) if (!g_clients[fd].ready) return;
    startMatch(r);
}
static void joinRoom(Client& c, const std::string& rid) {
    if (!c.room.empty()) { sendText(c, errJson("already in a room")); return; }
    std::string clean;
    for (char ch : rid) if (isalnum((unsigned char)ch) || ch == '-' || ch == '_') clean += ch;
    if (clean.size() > 16) clean.resize(16);
    if (clean.empty()) {   // quick-match: first open lobby, else create one
        for (auto& kv : g_rooms)
            if (!kv.second.inMatch && kv.second.members.size() < ROOM_CAP) { clean = kv.first; break; }
        if (clean.empty()) clean = "ARENA-" + std::to_string(g_nextRoom++);
    }
    auto it = g_rooms.find(clean);
    if (it == g_rooms.end()) { Room nr; nr.id = clean; it = g_rooms.emplace(clean, nr).first; }
    Room& r = it->second;
    if (r.inMatch || r.members.size() >= ROOM_CAP) {
        sendText(c, errJson("room full or match in progress"));
        if (r.members.empty()) g_rooms.erase(it);
        return;
    }
    r.members.push_back(c.fd); c.room = clean; c.ready = false; c.score = 0;
    printf("[room %s] player %u joined (%zu/%zu)\n", clean.c_str(), c.id, r.members.size(), ROOM_CAP);
    broadcastLobby(r);
}
static void leaveRoom(Client& c) {
    if (c.room.empty()) return;
    auto it = g_rooms.find(c.room);
    c.room.clear(); c.ready = false;
    if (it == g_rooms.end()) return;
    Room& r = it->second;
    r.members.erase(std::remove(r.members.begin(), r.members.end(), c.fd), r.members.end());
    if (r.members.empty()) { printf("[room %s] closed\n", r.id.c_str()); g_rooms.erase(it); return; }
    if (r.inMatch && r.members.size() < MIN_PLAYERS) endMatch(r, 0);
    broadcastLobby(r);
}

// ---------- Module 1: packet validation + Anti-Cheat Validation Gate ---------
// Input wire format:  I|epoch|seq|clientTimestampMs|targetX|targetY
static void handleInput(Client& c, const std::vector<std::string>& f) {
    auto it = g_rooms.find(c.room);
    if (it == g_rooms.end() || !it->second.inMatch || f.size() != 6 || !c.alive) return;
    uint32_t epoch, seq; double ts, tx, ty;
    if (!parseU(f[1], epoch) || !parseU(f[2], seq) || !parseD(f[3], ts) || !parseD(f[4], tx) || !parseD(f[5], ty)) return;

    // (a) Epoch gate: inputs sent before the client processed our last rollback/respawn are dropped.
    if (epoch != c.epoch) { g_stats.droppedStale++; return; }
    // (b) Absolute sequence number + timestamp: drop duplicates / out-of-order / stale.
    if (seq <= c.lastSeq || ts < c.lastTs) { g_stats.droppedStale++; return; }
    c.lastSeq = seq; c.lastTs = ts;

    // (c) Hard speed / teleport gate + wall check (no walking through concrete).
    double now = nowMs();
    double deltaSec = clampd((now - c.lastValidMs) / 1000.0, 0.0, MAX_DELTA_SEC);
    double dist = std::hypot(tx - c.x, ty - c.y);
    double maxMove = MAX_SPEED * deltaSec;
    // Spec formula:  dist > MAX_SPEED*deltaSec + MAX_ALLOWED_DRIFT  => cheat.
    // Hardening: MAX_ALLOWED_DRIFT is a *cumulative bank*, not a per-packet freebie. Without it a
    // cheater could move at 5x speed by staying under the slack on every packet. The bank
    // accumulates distance beyond the legal speed budget and drains while the player is slow.
    double remainingSlack = MAX_ALLOWED_DRIFT - c.driftBank;
    bool tooFast = dist > maxMove + remainingSlack;
    if (tooFast || hitsWall(tx, ty, PLAYER_R - 2.0)) {
        g_stats.rejectedCheat++; c.violations++;
        c.epoch++;                                   // invalidate everything still in flight
        printf("[ANTI-CHEAT] player %u rejected (%s): dist=%.1f allowed=%.1f dt=%.3fs violations=%d\n",
               c.id, tooFast ? "speed/teleport" : "wall", dist, maxMove + remainingSlack, deltaSec, c.violations);
        // Authoritative rollback: force client back to the last verified position.
        sendText(c, "{\"t\":\"rb\",\"epoch\":" + std::to_string(c.epoch) + ",\"x\":" + fmt(c.x) +
                    ",\"y\":" + fmt(c.y) + ",\"seq\":" + std::to_string(seq) + ",\"why\":\"" +
                    (tooFast ? "speed/teleport" : "wall") + "\"}");
        if (c.violations >= MAX_VIOLATIONS) { printf("[ANTI-CHEAT] kicking player %u\n", c.id); markDead(c); }
        return;                                      // lastValidMs deliberately NOT advanced
    }
    c.driftBank = std::max(0.0, c.driftBank + dist - maxMove);
    double nx = clampd(tx, PLAYER_R, ARENA_W - PLAYER_R), ny = clampd(ty, PLAYER_R, ARENA_H - PLAYER_R);
    if (nx != c.x || ny != c.y) c.dirty = true;
    c.x = nx; c.y = ny; c.ackSeq = seq; c.lastValidMs = now;
    g_stats.accepted++;
}
// Fire command: F|angle. The server spawns the bullet from the AUTHORITATIVE position and enforces cooldown.
static void handleFire(Client& c, const std::vector<std::string>& f) {
    auto it = g_rooms.find(c.room);
    if (it == g_rooms.end() || !it->second.inMatch || !c.alive || f.size() != 2) return;
    double a; if (!parseD(f[1], a)) return;
    double now = nowMs(); Room& r = it->second;
    if (now - c.lastFire < FIRE_COOLDOWN_MS * 0.9 || r.bullets.size() >= MAX_BULLETS) return;
    c.lastFire = now; c.aim = a; c.dirty = true;
    Bullet b{r.nextBullet++, c.id, c.x + std::cos(a) * (PLAYER_R + 8), c.y + std::sin(a) * (PLAYER_R + 8),
             std::cos(a) * BULLET_SPEED, std::sin(a) * BULLET_SPEED, now};
    r.bullets.push_back(b);
}
static void handleMessage(Client& c, const std::string& msg) {
    double now = nowMs();                            // simple flood protection
    if (now - c.rateWin > 1000) { c.rateWin = now; c.rateCnt = 0; }
    if (++c.rateCnt > MAX_MSGS_PER_SEC) return;
    std::vector<std::string> f = split(msg, '|');
    const std::string& cmd = f[0];
    if (cmd == "I") handleInput(c, f);
    else if (cmd == "F") handleFire(c, f);
    else if (cmd == "A" && f.size() == 2) {          // turret aim for other players' rendering
        double a; if (parseD(f[1], a) && a != c.aim) { c.aim = a; c.dirty = true; }
    }
    else if (cmd == "P" && f.size() == 2) {          // RTT probe: echo client stamp + server clock
        double t; if (parseD(f[1], t)) sendText(c, "{\"t\":\"pong\",\"c\":" + fmt(t) + ",\"s\":" + fmt(now) + "}");
    }
    else if (cmd == "L") sendRooms(c);
    else if (cmd == "J") joinRoom(c, f.size() > 1 ? f[1] : "");
    else if (cmd == "R") {
        auto it = g_rooms.find(c.room);
        if (it == g_rooms.end() || it->second.inMatch) return;
        c.ready = !c.ready; broadcastLobby(it->second); tryStart(it->second);
    }
}

// ------------------- Module 3: 60 Hz authoritative simulation ----------------
static void tickRoom(Room& r) {
    r.tick++;
    const double now = nowMs(), dt = 1.0 / TICK_HZ;
    uint32_t winner = 0;
    for (int fd : r.members) {                       // respawn timers
        Client& c = g_clients[fd];
        if (!c.dead && !c.alive && now >= c.respawnAt) respawn(c, now, "respawn");
    }
    // Bullets: integrate, collide with walls / arena edge / tanks. All hit detection is server-side.
    for (size_t i = 0; i < r.bullets.size();) {
        Bullet& b = r.bullets[i];
        b.x += b.vx * dt; b.y += b.vy * dt;
        bool gone = now - b.born > BULLET_LIFE_MS || b.x < 0 || b.x > ARENA_W || b.y < 0 || b.y > ARENA_H || hitsWall(b.x, b.y, 0);
        if (!gone) {
            for (int fd : r.members) {
                Client& t = g_clients[fd];
                if (t.dead || !t.alive || t.id == b.owner) continue;
                if (std::hypot(b.x - t.x, b.y - t.y) >= PLAYER_R + BULLET_R) continue;
                gone = true; t.hp -= BULLET_DMG; t.dirty = true;
                if (t.hp <= 0) {                     // kill
                    t.hp = 0; t.alive = false; t.respawnAt = now + RESPAWN_MS;
                    r.events.push_back({b.owner, t.id});
                    for (int f2 : r.members) {
                        Client& o = g_clients[f2];
                        if (o.id == b.owner) { o.score++; o.dirty = true; if (o.score >= WIN_KILLS && !winner) winner = o.id; }
                    }
                }
                break;
            }
        }
        if (gone) r.bullets.erase(r.bullets.begin() + (long)i); else ++i;
    }
    // Delta snapshot: only tanks that changed; full resync every second or on demand.
    bool full = r.forceFull || (r.tick % TICK_HZ == 0);
    std::string body = ",\"p\":["; bool first = true;
    for (int fd : r.members) {
        Client& c = g_clients[fd]; if (c.dead) continue;
        if (full || c.dirty) {
            if (!first) body += ",";
            first = false;
            body += "[" + std::to_string(c.id) + "," + fmt(c.x) + "," + fmt(c.y) + "," + std::to_string(c.score) + "," +
                    std::to_string(c.hp) + "," + fmt(c.aim) + "]";
            c.dirty = false;
        }
    }
    body += "],\"b\":[";
    for (size_t i = 0; i < r.bullets.size(); ++i) {
        const Bullet& b = r.bullets[i];
        body += (i ? "," : "") + std::string("[") + std::to_string(b.id) + "," + fmt(b.x) + "," + fmt(b.y) + "," + std::to_string(b.owner) + "]";
    }
    body += "]";
    if (!r.events.empty()) {
        body += ",\"k\":[";
        for (size_t i = 0; i < r.events.size(); ++i)
            body += (i ? "," : "") + std::string("[") + std::to_string(r.events[i].first) + "," + std::to_string(r.events[i].second) + "]";
        body += "]";
    }
    body += ",\"srv\":" + fmt(now) + "}";
    // Per-client header carries that client's acknowledged input sequence for reconciliation.
    for (int fd : r.members) {
        Client& c = g_clients[fd];
        sendText(c, "{\"t\":\"snap\",\"tick\":" + std::to_string(r.tick) + ",\"ack\":" + std::to_string(c.ackSeq) +
                    ",\"full\":" + (full ? "1" : "0") + body);
    }
    r.forceFull = false; r.events.clear();
    if (winner) endMatch(r, winner);
}
static void runTick() {
    g_stats.ticks++;
    for (auto& kv : g_rooms) if (kv.second.inMatch) tickRoom(kv.second);
}
// Print every LAN address so a second device knows exactly what to type.
static void printLanAddresses() {
    ifaddrs* ifa = nullptr;
    if (getifaddrs(&ifa) != 0) return;
    printf("  This computer : http://localhost:%d\n", PORT);
    for (ifaddrs* p = ifa; p; p = p->ifa_next) {
        if (!p->ifa_addr || p->ifa_addr->sa_family != AF_INET) continue;
        char buf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &((sockaddr_in*)p->ifa_addr)->sin_addr, buf, sizeof buf);
        if (strncmp(buf, "127.", 4) == 0) continue;
        printf("  Other devices : http://%s:%d   (interface %s)\n", buf, PORT, p->ifa_name);
    }
    freeifaddrs(ifa);
    printf("  Run from the folder that contains index.html. Devices must be on the same Wi-Fi/LAN.\n");
}

// ------------------------- HTTP + WebSocket protocol -------------------------
static void processInbound(Client& c) {
    if (!c.handshaken) {
        size_t end = c.in.find("\r\n\r\n");
        if (end == std::string::npos) { if (c.in.size() > 8192) markDead(c); return; }
        std::string head = c.in.substr(0, end); c.in.erase(0, end + 4);
        std::string low = head; for (char& ch : low) ch = (char)tolower((unsigned char)ch);
        size_t kp = low.find("sec-websocket-key:");
        if (low.find("upgrade: websocket") != std::string::npos && kp != std::string::npos) {
            size_t s = kp + 18, e = head.find("\r\n", s); if (e == std::string::npos) e = head.size();
            std::string key = head.substr(s, e - s);
            key.erase(0, key.find_first_not_of(" \t")); key.erase(key.find_last_not_of(" \t\r") + 1);
            std::string acc = base64(sha1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"));
            c.out += "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + "\r\n\r\n";
            c.handshaken = true; c.id = g_nextId++;
            flushClient(c);
            sendText(c, "{\"t\":\"welcome\",\"id\":" + std::to_string(c.id) + "}");
            printf("[net] player %u connected (fd %d)\n", c.id, c.fd);
        } else {   // plain HTTP: serve the frontend so the demo needs zero setup
            std::ifstream f("index.html", std::ios::binary);
            std::stringstream ss; ss << f.rdbuf(); std::string body = ss.str();
            std::string st = f ? "200 OK" : "404 Not Found";
            if (!f) body = "index.html not found next to server binary";
            c.out += "HTTP/1.1 " + st + "\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: " +
                     std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
            c.closeAfterFlush = true; flushClient(c); return;
        }
    }
    // Decode as many complete frames as are buffered (clients MUST mask: RFC 6455 5.1).
    while (!c.dead) {
        std::string& b = c.in;
        if (b.size() < 2) return;
        uint8_t op = (uint8_t)b[0] & 0x0F; bool masked = (uint8_t)b[1] & 0x80;
        uint64_t len = (uint8_t)b[1] & 0x7F; size_t off = 2;
        if (len == 126) { if (b.size() < 4) return; len = ((uint8_t)b[2] << 8) | (uint8_t)b[3]; off = 4; }
        else if (len == 127) { if (b.size() < 10) return; len = 0; for (int i = 2; i < 10; ++i) len = (len << 8) | (uint8_t)b[i]; off = 10; }
        if (len > 65536 || !masked) { markDead(c); return; }
        if (b.size() < off + 4 + len) return;
        uint8_t mk[4]; memcpy(mk, &b[off], 4); off += 4;
        std::string payload((size_t)len, '\0');
        for (size_t i = 0; i < len; ++i) payload[i] = (char)((uint8_t)b[off + i] ^ mk[i & 3]);
        b.erase(0, off + (size_t)len);
        if (op == 0x1) handleMessage(c, payload);
        else if (op == 0x8) { markDead(c); return; }
        else if (op == 0x9) sendFrame(c, 0xA, payload);   // ping -> pong
    }
}

static void reapDead() {
    for (int fd : g_dead) {
        auto it = g_clients.find(fd); if (it == g_clients.end()) continue;
        Client& c = it->second;
        if (c.handshaken) printf("[net] player %u disconnected\n", c.id);
        leaveRoom(c); close(fd); g_clients.erase(it);
    }
    g_dead.clear();
}

// ----------------------------------- main ------------------------------------
int main() {
    signal(SIGPIPE, SIG_IGN);
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1; setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_ANY); addr.sin_port = htons(PORT);
    if (bind(ls, (sockaddr*)&addr, sizeof addr) < 0 || listen(ls, 64) < 0) { perror("bind/listen"); return 1; }
    fcntl(ls, F_SETFL, fcntl(ls, F_GETFL, 0) | O_NONBLOCK);
    printf("TANK SIEGE server running (%d Hz authoritative tick)\n", TICK_HZ);
    printLanAddresses();

    const double stepMs = 1000.0 / TICK_HZ;
    double nextTick = nowMs() + stepMs, nextStats = nowMs() + 10000;
    std::vector<pollfd> pfds;
    while (true) {
        pfds.clear(); pfds.push_back({ls, POLLIN, 0});
        for (auto& kv : g_clients) pfds.push_back({kv.first, (short)(POLLIN | (kv.second.out.empty() ? 0 : POLLOUT)), 0});
        double wait = nextTick - nowMs();
        poll(pfds.data(), (nfds_t)pfds.size(), wait > 0 ? (int)std::ceil(wait) : 0);

        if (pfds[0].revents & POLLIN) {
            while (true) {
                int fd = accept(ls, nullptr, nullptr); if (fd < 0) break;
                fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
                setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);   // no Nagle: latency matters
                g_clients[fd].fd = fd;
            }
        }
        for (size_t i = 1; i < pfds.size(); ++i) {
            auto it = g_clients.find(pfds[i].fd); if (it == g_clients.end() || it->second.dead) continue;
            Client& c = it->second;
            if (pfds[i].revents & (POLLERR | POLLNVAL)) { markDead(c); continue; }
            if (pfds[i].revents & (POLLIN | POLLHUP)) {
                char buf[4096];
                while (!c.dead) {
                    ssize_t n = recv(c.fd, buf, sizeof buf, 0);
                    if (n > 0) { c.in.append(buf, (size_t)n); if (c.in.size() > 131072) markDead(c); else processInbound(c); }
                    else if (n == 0) { markDead(c); break; }
                    else { if (errno != EAGAIN && errno != EWOULDBLOCK) markDead(c); break; }
                }
            }
            if (!c.dead && (pfds[i].revents & POLLOUT)) flushClient(c);
        }
        // Fixed-timestep loop with bounded catch-up so a stall never spirals.
        int guard = 0;
        while (nowMs() >= nextTick && guard++ < 5) { runTick(); nextTick += stepMs; }
        if (guard >= 5) nextTick = nowMs() + stepMs;
        reapDead();
        if (nowMs() >= nextStats) {
            nextStats += 10000;
            printf("[stats] clients=%zu rooms=%zu accepted=%llu cheatRejected=%llu staleDropped=%llu\n",
                   g_clients.size(), g_rooms.size(), (unsigned long long)g_stats.accepted,
                   (unsigned long long)g_stats.rejectedCheat, (unsigned long long)g_stats.droppedStale);
        }
    }
}
