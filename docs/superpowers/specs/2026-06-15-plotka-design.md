# plotka - design (as built)

**Date:** 2026-06-15 (design); updated to match the implementation.
**Status:** Implemented (single node + gossip cluster + polish).

> Name: `plotka` (PL *plotka* = gossip/rumor; pun on *płotka* = a small fish / "small fry").
> The service works by gossip, and it is a small, unpretentious daemon for a simple problem.

This document describes the design as built. The phase-by-phase build records live in
`docs/superpowers/plans/` (`plotka-core`, `plotka-cluster`, `plotka-polish`).

## Problem

A small homelab needs dynamic DNS: KVM VMs register their `name -> IP` on boot and
deregister on shutdown, so other hosts can resolve them by name. This already works and
has for ~5 years, implemented as part of `kvm-no-cloud-init`:

- Client: VM does `GET http://<registry>/<hostname>` (register) and `GET .../-<hostname>`
  (deregister). Source-IP based.
- Server: a PHP script writes `ip name` into a hosts file; `dnsmasq` serves it.

### What's wrong with the old setup

- Server is PHP + dnsmasq - two runtimes, awkward to package and operate.
- No replication, no HA. The registry is a single remote node reached over the BGP-routed
  overlay ("cloud/VPN").
- Deregister relies on an explicit shutdown call - a crashed VM leaves a stale record
  forever.

## Goals

- **No-ops replication.** Give a node the IP(s) of its peers and it just works. Records
  written to *any* node replicate to *every* node.
- **Multi-master.** A registration may land on any node.
- **Tiny client.** Zero-dependency: registration is doable from pure `bash` (`/dev/tcp`),
  from `dig`, or from a few-kB C binary.
- **Single static server binary.** One file per node, minimal config.
- **Self-healing lifecycle.** A dead VM's record expires on its own.
- **Location-independent.** Any node serves any request and accepts any registration; the
  design never assumes a client reaches a specific node. This is what makes the service
  safe to put behind anycast - but anycast itself is out of scope (network-layer, BGP).

## Non-goals

A **closed service on a trusted network.** Deliberately not built: IP access lists, client
authentication, PKI, TLS for the client path; anycast/routing (a BGP job). The only secret
is a shared key for server-to-server gossip - replication integrity, not access control.

## Architecture

Each node is one Go binary. Conceptually:

1. **DNS server** - authoritative, non-recursive resolver: answers `A`/`AAAA`/`PTR` from
   the store with TTL 0, `NXDOMAIN` for unknown names.
2. **Registry** - accepts register/deregister over three channels (DNS, tcp-line, HTTP).
3. **Replicated store** - in-memory; per name an optional `A` and `AAAA`, each
   `{ip, ts, deleted, static}`, plus a reverse index for PTR. Replicated across nodes by
   `hashicorp/memberlist` gossip.
4. **Listeners** - each protocol on its own port, or an opt-in mux (see below).

Go is the server language (`memberlist` + `miekg/dns` are mature); the result is a single
static binary of a few MB. The "tiny" constraint applies to the *client*, not the server.

### Listeners (relocate-model ports)

Each protocol runs on a dedicated port, **or** shares an opt-in mux port:

| protocol | flag | default | transport |
|----------|------|---------|-----------|
| DNS      | `--registry-dns-port`  | `53`   | UDP + TCP |
| HTTP     | `--registry-http-port` | `80`   | TCP       |
| tcp-line | `--registry-tcp-port`  | `2000` | TCP       |
| (mux)    | `--registry-port`      | `0` (off) | UDP + TCP |

Rules:
- A protocol with a dedicated port (`>0`) runs there. With a dedicated port set and the mux
  off, setting that port to `0` disables the protocol.
- The mux (`--registry-port`, if `>0`) carries every protocol *without* a dedicated port.
  On a port shared by ≥2 protocols, TCP is split by first byte (`soheilhy/cmux`): a leading
  `:` -> tcp-line, an uppercase method -> HTTP, otherwise -> DNS-over-TCP.
- **UDP serves DNS only.** The tcp-line channel is TCP-only (hence the name); UDP-based
  fire-and-forget registration is done through the DNS channel (`dig :+name`).
- Ports must be distinct and at least one protocol must be enabled, else startup errors.
- All listeners share `--registry-bind` (the service IP). Bind an explicit IP, not
  `0.0.0.0` (which collides with a local resolver such as `systemd-resolved` on
  `127.0.0.53:53`).

Ports 53 and 80 are privileged: run under systemd with
`AmbientCapabilities=CAP_NET_BIND_SERVICE` as an unprivileged (Dynamic) user - no root, no
`setcap` (file capabilities live in an xattr a `.deb` does not carry).

## Registration protocol (client -> server)

One grammar, encoded in the registered name itself, reusing the existing `-` deregister
convention:

```
<op>[<addr>].<name>
  op    = +  register   |  -  deregister
  addr  = optional [IPv4] or [IPv6], brackets literal.
          absent  -> use the connection source IP (default)
          present -> use the given address (NAT case: source IP would be the gateway)
  name  = hostname to register/deregister (may contain dots, e.g. host.lab)
```

Brackets around `addr` are required because IPv6 contains colons.

**The `:` prefix is the universal "registry command" marker** on the DNS and tcp-line
channels: a token starting with `:` means "register/deregister, do not resolve". It tags
the bytes for the mux, tells the DNS handler this is a command, and stops `dig` from eating
a leading `+`/`-` as a CLI flag. HTTP does not need it - the verb disambiguates.

### Channels

- **tcp-line (primary, zero-dep), TCP:** client writes the `:`-prefixed token,
  fire-and-forget, no response.
  - `printf ':+host.a'             > /dev/tcp/SERVER/2000`   (register, source IP)
  - `printf ':+[192.168.1.2].host.a' > /dev/tcp/SERVER/2000` (register, explicit IP, NAT)
  - `printf ':-host.a'            > /dev/tcp/SERVER/2000`    (deregister)

- **DNS query:** the qname is the `:`-prefixed token; the server replies immediately
  (minimal NOERROR/NXDOMAIN) so `dig` does not block.
  - `dig @SERVER :+host.a`               (register, source IP)
  - `dig @SERVER :+[192.168.1.2].host.a` (register, explicit IP)
  - `dig @SERVER :-host.a`               (deregister)

- **HTTP (TCP):** path-segment registration by method; `GET` is a pure query.
  - `POST`/`PUT /name` -> register (source IP); `POST`/`PUT /ip/name` -> register (explicit)
  - `DELETE /name` -> deregister
  - `GET /name` -> forward query: the IP(s), one per line (A then AAAA), 404 if none
  - `GET /ip` -> reverse query (PTR): the name, 404 if none (the segment parses as an IP)

### Fire-and-forget + heartbeat

The tcp-line channel sends no response. That is safe because registration is **soft-state
with a heartbeat**: the client re-registers periodically, so a lost packet is corrected by
the next cycle. No client-side retry logic.

## DNS serving

- **Authoritative, non-recursive, final.** Answers from the store only; unknown -> NXDOMAIN.
- **No fixed zone or suffix.** Any registered name is answerable (`host.a`, `box.lab`, ...).
- **Response TTL = 0.** A local DNS server; answers must not be cached. (Distinct from
  `--purge-ttl`, which governs record purge, not DNS caching.)
- **A / AAAA:** a name holds at most one `A` and one `AAAA`. An IPv4 registration sets the
  `A`, an IPv6 one the `AAAA`; register twice for both.
- **PTR:** answered from the reverse index for `in-addr.arpa` / `ip6.arpa`.
- **Name case** is normalized to lower-case so every channel and DNS lookup agree (DNS is
  case-insensitive, RFC 4343).

## Store and conflict resolution

The store is the single source of truth and the merge core. Every mutation - local
(a channel write) or remote (a replicated delta) - goes through one LWW function.

- **Unit of replication:** a `Delta{Name, V6, IP, TSNanos, Deleted, Static}` (JSON on the
  wire).
- **Last-writer-wins, deterministic and node-independent** so all nodes converge:
  - strictly newer `ts` wins;
  - on equal `ts`: an existing tombstone wins; an incoming tombstone beats a live record
    (delete bias); two live records break the tie by **smaller IP string** (a total order).
- **Tombstones:** a delete is a `deleted=true` record carrying the deletion `ts`. It hides
  the record, replicates via LWW, and is GC'd by purge after `--purge-ttl`. Without it a
  node that missed a delete would re-gossip the stale record and resurrect it.
- **Anti-resurrection age guard:** an incoming *live* (non-static) delta older than
  `--purge-ttl` is rejected, so a record cannot come back from a stale peer's snapshot
  after its tombstone was purged.
- **Static precedence (immutability):** see below.

## Records: dynamic vs static

- **Dynamic** (the normal path): VMs register over a channel; `plotka client set` is the
  same thing by hand. Subject to purge.
- **Static** (`--register ip:name`, repeatable): immutable operator config. The server
  re-asserts each on a timer (`--reassert-interval`, default 1h) so it never expires, and:
  - a **dynamic** register/deregister can never change or delete a static record;
  - static-vs-static is LWW (change a static via config + restart);
  - static is **exempt from purge** and from the age guard (re-asserted by its owner).
  - `plotka client list` marks each record `static` or `dynamic`.

  Static is driven by `--register`, not a separate "pin" verb: a freshly bootstrapped empty
  cluster must not require an operator to remember to hand-register anything. Each node
  carries the same `--register` list and logs `<IP> registered as <name>` at startup.

## Lifecycle

A single mechanism covers "live host stays" and "dead host disappears":

- Every record carries a `ts` (last-seen). Registration just refreshes it - a "heartbeat"
  is the same idempotent register on a timer.
- **`--purge-ttl`** (default 24h) is the age threshold: a sweep drops records not refreshed
  within it. **`--purge-interval`** (default 8h) is how often the sweep runs (`plotka
  client purge` runs it on demand). A stale record is gone within `purge-ttl +
  purge-interval`.
- A live host re-registers within `purge-ttl` -> permanent; a crashed/powered-off host
  stops -> expires. Clean shutdown sends `:-name` for instant removal.
- **Expiry propagates.** A purge does not delete only locally: it converts the expired live
  record to a tombstone (ts=now) and broadcasts it, so a peer with a longer `purge-ttl`
  drops it too. The effective cluster expiry is therefore the most aggressive node
  (`min` of the per-node `purge-ttl`s). A live host whose record was expired re-appears on
  its next heartbeat (the fresh `ts` beats the tombstone) - which is why `purge-ttl` must
  stay above the heartbeat interval.
- **Hard constraint:** `purge-ttl` > heartbeat interval (else a live host is purged between
  beats); `purge-interval` < `purge-ttl`. Defaults: heartbeat 4-8h, purge-ttl 24h,
  purge-interval 8h - comfortable margins.

## Replication (server <-> server)

- **Transport:** `hashicorp/memberlist` - TCP + UDP on one port (default **7946**). UDP
  carries gossip + SWIM ping/ack; TCP carries full-state anti-entropy (push-pull) and is
  the fallback. Both must be open between nodes.
- **Model:** multi-master, eventually consistent; no leader, no external store.
- **Two traffic kinds:** on-change broadcasts (a changed record is gossiped to random peers,
  retransmitted ~log(N) times, sub-second) and the change-independent SWIM ping (~1s) plus
  full-state push-pull (default 1h) as the convergence backstop. A heartbeat (re-register
  with a newer `ts`) does broadcast - negligible at this scale (~150-200 records, 4-8h
  heartbeat); push-pull would carry it anyway.
- **Bootstrap:** start with one or a few seed peer IPs (`--join host-ip1,host-ip2,...`); the
  node learns the rest of the mesh via gossip. The same `--join` list works on every node
  (joining yourself is a no-op). **Join is retried every 10s while the node is alone**, so
  cluster formation does not depend on cold-start order (memberlist does not retry Join).
- **Gossip advertises the host's unicast IP, not the service VIP** (the VIP is anycast, the
  same on every node; gossip must reach a *specific* node). `--bind` may listen on all
  interfaces; the **advertise** address is auto-derived as "a host IP that is not
  `--registry-bind`" (override with `--advertise` if a host has >1 non-VIP IP). `--join`
  takes host IPs, never the VIP.
- **Integrity:** memberlist's symmetric encryption with a shared key - the one secret in
  the system, server-to-server only.
- **Visibility:** node join/leave is logged; `plotka client cluster status` lists members
  (name, advertised `addr:port`, state alive/suspect/dead/left). A failed node is reaped
  from the member list ~30s after it dies (SWIM detection ~5-6s + `GossipToTheDeadTime`).

## Configuration & CLI

All configuration is via CLI flags - no config file (KISS). Every node can run the same
flag set; per-host bits (advertise) are auto-derived.

```
plotka server [flags]
  service:
    --registry-bind 10.53.53.53     service bind IP (shared by all listeners)
    --registry-dns-port 53          dedicated DNS port (0 = on mux / disabled)
    --registry-http-port 80         dedicated HTTP port
    --registry-tcp-port 2000        dedicated tcp-line port
    --registry-port 0               mux port (all protocols on one port); 0 = off
    --admin-socket /run/plotka/admin
  records:
    --register 10.53.53.53:registry.vm   static ip:name, repeatable (immutable)
    --purge-ttl 24h                 remove records not refreshed within this
    --purge-interval 8h             how often the purge sweep runs
    --reassert-interval 1h          how often to refresh --register statics
    --verbose                       log every record change to stderr
  cluster:
    --bind ""                       cluster (gossip) listen IP ("" = all)
    --port 7946                     cluster port (TCP+UDP)
    --advertise ""                  address peers dial (default: host IP != VIP)
    --join ip1,ip2,ip3              seed peers - host IPs, never the VIP
    --cluster-key <b64>             shared secret; also PLOTKA_CLUSTER_KEY / --cluster-key-file
    --node-name <name>              unique node name (default: hostname)

plotka client [--admin-socket PATH] <op>
  list                 records: name, ip, last-seen, static|dynamic
  set <name> <ip>      create/update a (dynamic) record
  delete <name>        remove a record (tombstone)
  purge                run the purge sweep now
  cluster status       cluster members: name, addr, state
plotka version
plotka help
```

`plotka client` is always local: it talks to the server on the same host over a unix
socket (default `/run/plotka/admin`), so admin ops are bounded by filesystem permissions,
not reachable over the network. It is not how VMs register; the server replicates the
change to the mesh.

Prefer the cluster secret via `PLOTKA_CLUSTER_KEY` (e.g. a systemd `EnvironmentFile`) over
`--cluster-key`, so it does not show up in `ps`. `--verbose` logs every record change:
`register`/`deregister` (local), `replicate`/`replicate-delete` (from a peer), `expire`
(purged).

## Client (on the VM)

- **Pure bash:** `/dev/tcp` tcp-line protocol, zero dependencies. Primary path.
- **`dig`:** DNS-query channel (also the UDP fire-and-forget path).
- **C binary:** optional, few kB, minimal-footprint case.
- `kvm-no-cloud-init` integration: register `:+name` at boot, `:-name` at clean shutdown,
  and a systemd timer re-sending `:+name` periodically (lazy, e.g. hourly) as the heartbeat.

## Persistence

**None.** No disk snapshot. A node starts, bootstraps from the network if a mesh exists,
otherwise registers its `--register` statics and waits. The cluster can come up empty and
re-replicate over time. Losing dynamic state is acceptable - it is rebuilt by VM restarts,
`set`, and heartbeats; statics are re-asserted by every node.

## Trust model

Closed service, trusted network (the BGP/VPN overlay). No client auth by design. With an
explicit `[addr]`, any client can register any name to any IP - acceptable and consistent.
The only secret is the cluster key (server-to-server). Licensed into the public domain.
