# plotka

plotka is a dynamic DNS registry for fleets of machines whose addresses come
and go - homelab VMs, containers, anything ephemeral. A host announces its
`name -> IP` when it boots (and refreshes on a timer); plotka then answers DNS
`A` / `AAAA` / `PTR` queries for those names. No zone files, no hand-edited
records.

It runs as a single node, or as a multi-master cluster: register against any
node and the record replicates to every node via gossip - no primary, no single
point of failure (see [Clustering](#clustering)).

Registration is deliberately trivial - a one-line `dig`, `curl`, or even
`echo > /dev/tcp` from the host, no client to install.

## Channels and ports

Three registration channels, each on its own port by default (the mux is opt-in):

| channel | default port | transport | purpose |
|---------|--------------|-----------|---------|
| DNS     | `53`         | UDP + TCP | resolution + `:`-query registration |
| HTTP    | `80`         | TCP       | REST registration + query |
| tcp     | `2000`       | TCP       | zero-dependency line protocol |

Each protocol has a `--registry-<proto>-port`. Set one to `0` to disable that
protocol (with the mux off). The opt-in **mux** (`--registry-port`, default `0`)
carries every protocol that has no dedicated port on a single port.

### DNS

The leading `:` marks a registration query (and stops `dig` eating `+`/`-`):

```sh
dig @SERVER :+box.lab               # register name -> source IP
dig @SERVER :+[10.9.9.9].box.lab    # register name -> explicit IP (NAT)
dig @SERVER :-box.lab               # deregister
dig @SERVER box.lab +short          # resolve
```

### tcp (zero-dependency line protocol)

Same `:`-grammar, fire-and-forget over TCP. Pure bash, no client binary:

```sh
printf ':+box.lab'            > /dev/tcp/SERVER/2000   # source IP
printf ':+[10.9.9.9].box.lab' > /dev/tcp/SERVER/2000   # explicit IP (NAT)
printf ':-box.lab'           > /dev/tcp/SERVER/2000    # deregister
```

(UDP fire-and-forget registration is available via the DNS channel - `dig :+name`.)

### HTTP

Path-segment registration by method; `GET` is a pure query:

```sh
curl -X POST   http://SERVER:80/box.lab          # register, source IP
curl -X POST   http://SERVER:80/10.9.9.9/box.lab # register, explicit IP
curl -X DELETE http://SERVER:80/box.lab          # deregister
curl http://SERVER:80/box.lab                    # forward: IP(s), 404 if none
curl http://SERVER:80/10.9.9.9                   # reverse: name, 404 if none
```

A name may hold both an A and an AAAA - register it twice, once per family;
`GET /name` then returns both, one per line. IPv6 uses brackets in the `:`-grammar
(`:+[2001:db8::1].name`) and bare colons in an HTTP path segment.

## Record lifecycle

A record carries a last-seen timestamp. Registration (the only write path for
hosts) just refreshes it - a "heartbeat" is the same call on a timer. A
background sweep (`--purge-interval`) drops records not refreshed within
`--purge-ttl`. Clean shutdown can send `:-name` for instant removal; otherwise a
crashed/powered-off host's record expires on its own. Keep `--purge-ttl`
comfortably above the heartbeat interval; `--purge-interval` only sets how often
the sweep runs (keep it below `--purge-ttl`). A stale record is gone within
`purge-ttl + purge-interval`.

`--register` entries are **immutable static records**: re-asserted by the server
on a timer, and a dynamic registration/deletion can never change or remove them.
Use them for the registry's own name, routers, NAS, and other non-heartbeating
hosts. `plotka client list` marks each entry `static` or `dynamic`.

## Running

```
plotka server [flags]
  --registry-bind 10.53.53.53   service bind IP (shared by all listeners)
  --registry-dns-port 53        dedicated DNS port (0 = on mux / disabled)
  --registry-http-port 80       dedicated HTTP port (0 = on mux / disabled)
  --registry-tcp-port 2000      dedicated tcp-line port (0 = on mux / disabled)
  --registry-port 0             mux port (all protocols on one port); 0 = off
  --admin-socket /run/plotka/admin
  --purge-ttl 24h               remove records not refreshed within this
  --purge-interval 8h           how often the purge sweep runs
  --reassert-interval 1h        how often to refresh --register statics
  --register 10.53.53.53:registry.vm   static ip:name, repeatable
  --verbose                     log every record change to stderr

plotka client [--admin-socket PATH] [--full] <list | set NAME IP | delete NAME | purge | cluster status>
  --full   list: do not truncate the name column to 64 chars
plotka version
plotka help
```

`plotka client` talks to the local server over the unix admin socket.

Ports 53 and 80 are privileged: run under systemd as an unprivileged user with
`AmbientCapabilities=CAP_NET_BIND_SERVICE` (see `packaging/plotka.service`) - no
root, no `setcap`. Bind an explicit service IP, not `0.0.0.0` (which collides
with a local resolver such as `systemd-resolved` on `127.0.0.53:53`).

## Clustering

Multi-master, eventually consistent: register on any node, it replicates to all
within seconds (gossip broadcast on change; full-state push-pull anti-entropy as
a backstop). Deletes use tombstones so they propagate and self-GC after
`--purge-ttl`. Conflicts resolve last-writer-wins by timestamp.

Cluster flags (the **service** stays on `--registry-bind`; cluster traffic uses
each host's own unicast IP, never the VIP):

```
--bind <host-ip>      cluster listen IP ("" = all interfaces)
--port 7946           cluster port (TCP + UDP, must be open between nodes)
--advertise <host-ip> address peers dial; default: a host IP that is not
                      --registry-bind (set explicitly if the host has >1 non-VIP IP)
--join ip1,ip2,ip3    seed peers - HOST IPs, never the anycast VIP (empty = first node)
--cluster-key <b64>   shared secret; also PLOTKA_CLUSTER_KEY env or --cluster-key-file
--node-name <name>    unique node name (default: hostname)
```

Every node runs the **same** flag set - `--advertise` is auto-derived and `--join`
is the same seed list everywhere. Generate the shared key once:

```sh
head -c 32 /dev/urandom | base64    # 32 bytes -> AES-256 cluster encryption
```

Prefer passing the key via `PLOTKA_CLUSTER_KEY` (e.g. a systemd `EnvironmentFile`)
rather than `--cluster-key`, so it does not show up in `ps`. Node join/leave is
logged to stderr; `plotka client cluster status` lists members (name, advertised
`addr:port`, state). `--verbose` additionally logs every record change -
`register`/`deregister` (local), `replicate`/`replicate-delete` (from a peer),
and `expire` (purged).

Example three-node cluster (same on every host; anycast VIP `10.53.53.53`):

```sh
PLOTKA_CLUSTER_KEY=$(cat /etc/plotka/key) plotka server \
  --register 10.53.53.53:registry.vm \
  --port 7946 --join 10.0.0.11,10.0.0.12,10.0.0.13
```

## Security

plotka is a daemon for an internal, trusted network. **Registration is
unauthenticated by design** - any host that can reach a registration port can
register, deregister, or override any name (and with an explicit address, point
it anywhere). No ACLs, no client auth, no TLS on the client path - that
simplicity is the point. Do not expose the registration ports to an untrusted
network.

**Node-to-node cluster traffic can be secured, and that part is built in.** Set
a shared key (`PLOTKA_CLUSTER_KEY` / `--cluster-key`) and gossip between nodes is
encrypted and authenticated (AES, via memberlist), so a foreign host cannot join
the cluster or read replicated records. This key protects replication between
nodes only; it does not authenticate registrations.

## Build and release

`make build` produces a stripped binary in `bin/plotka`. Releases are built by
GoReleaser on a pushed `v*` tag (linux amd64/arm64); the version is taken from
the git tag and embedded via `-X main.version`.

## License

Public domain. No rights reserved.

## Design

Full design and decisions: `docs/superpowers/specs/2026-06-15-plotka-design.md`.
Implementation plans under `docs/superpowers/plans/`: `plotka-core` (single node),
`plotka-cluster` (gossip HA), `plotka-polish` (ports, HTTP, cluster status).
