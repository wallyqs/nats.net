# Gap audit: nats.net Core, connection level, against NQ C# (`csharp.async`)

Scope: `NatsConnection` lifecycle, `NatsOpts` (every property) and its nested option types, events, ping and RTT, server info, stats, transport (TCP, TLS modes, WebSocket, custom sockets), auth, pooling, logging, write buffering and backpressure.

Versions compared:
- nats.net at HEAD `15bd0bf` (v3.3.0 plus #1266). The fork point is `8bb0fb7`.
- NQ at `/home/user/nq.dev/packages/csharp/src/NQ`.
- The nats.go oracle at `7a8404a` (v1.54.0).

Every item was checked with grep or by reading the source. Status column:
- "N/A" means the nats.net option is dead code, or is test-only.
- "PRESENT*" means present, but with a behaviour divergence that comes from following nats.go.

Class column:
- **oracle-backed**: nats.go has an equivalent. The status of that symbol in `csharp.async.json` is given.
- **nats.net-only**: no nats.go equivalent. nats.net or nats-server would be the reference.
- **.NET idiom**: no protocol behaviour, only API or hosting shape.

Tag: **[new]** means the item was introduced by a commit in `8bb0fb7..HEAD`.

Path abbreviations:
- `NO` = `src/NATS.Client.Core/NatsOpts.cs`
- `NC` = `src/NATS.Client.Core/NatsConnection.cs`
- `qNO` = NQ `NatsOpts.cs`
- `qNC` = NQ `NatsConnection.cs`
- `go` = `/home/user/oracle/nats.go/nats.go`
- caps = `ir/capabilities/csharp.async.json`

## 1. Lifecycle

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|---|
| `ConnectAsync` | NC:232 | PRESENT `ConnectAsync` qNC:166 | `Connect` go:1115; caps `Connect` implemented | oracle-backed | nats.net's call is idempotent: concurrent callers await `_waitForOpenConnection`. NQ runs compiled `start_connection`. |
| Lazy connect: publish, subscribe, ping and request call `ConnectAsync` when the connection is not open | `NatsConnection.Publish.cs`:28-46,120-131; `LowLevelApi.cs`:7-14; `Ping.cs`:14-17 | MISSING. NQ calls `ConnectAsync` only from its own definition (qNC:166); operations never connect implicitly. | none (a nats.go `Conn` only exists after `Connect`) | nats.net-only (API shape) | A common nats.net usage pattern. |
| `RetryOnInitialConnect`: `ConnectAsync` loops with backoff until connected | NO:336; NC:271-285 | PARTIAL `RetryOnFailedConnect` qNO:124 | `RetryOnFailedConnect` go:1696 / field go:573; caps implemented → `NatsOpts.RetryOnFailedConnect` | oracle-backed (semantics differ) | nats.go returns at once in the RECONNECTING state and connects in the background (`ReconnectHandler` fires later). nats.net's `ConnectAsync` blocks until connected. A nats.net user porting code gets a different await contract. |
| `ConnectRetryAsync` | `tests/NATS.Client.TestUtilities/NatsUtils.cs`:10 | N/A | none | none | Test helper only, not library API. #1198 (new) gates nats.net tests with it. |
| `ReconnectAsync`: force reconnect | `NatsConnection.Reconnect.cs`:8 | PRESENT `ReconnectAsync` qNC:178 | `ForceReconnect` go:2622; caps implemented | oracle-backed | nats.net returns without effect unless the connection is Open. NQ follows nats.go `force_reconnect`. |
| `DisposeAsync` / close | NC:338 | PRESENT `DisposeAsync`→`CloseAsync` qNC:378,161, `Closed` task qNC:296 | `Close` go:6258; caps implemented | oracle-backed | |
| Connection-level drain | nats.net has no connection `DrainAsync` (grep `public.*Drain` in Core finds only `INatsSub.DrainAsync`) | NQ has more: `DrainAsync` qNC:156, `IsDraining` | `Drain` go:6400; implemented | — | Not a gap. |
| `DrainSubscriptionsOnDispose`: dispose drains subscriptions and flushes the writer | NO:196; NC:357-370 | PARTIAL. There is no dispose-time toggle (grep `DrainSubscriptionsOnDispose`: no match); `DisposeAsync` = `CloseAsync`. Explicit `DrainAsync` gives the same result. | `Drain` go:6400 (implemented) | oracle-backed through `Drain`; the toggle itself is nats.net-only | **[new]** #1085 |
| `DrainPingTimeout`: per-subscription PING/PONG bound during drain | NO:222 | PARTIAL. Only an overall `DrainTimeout` (qNO:120). | `DrainTimeout` go:1402; implemented | oracle-backed (different granularity) | **[new]** #1085 |
| `ConsumerDrainOnDisposeTimeout` | NO:209 | MISSING (no match) | none | nats.net-only | **[new]** #1085. JetStream consume-loop wait; out of the core area. |
| Explicit subscription drain | `NatsSubBase.cs`:314 | PRESENT `NatsSubscription.DrainAsync` | `Subscription.Drain` go:5276; implemented | oracle-backed | **[new]** #1177 on the nats.net side; NQ already had it. |

## 2. Connection state and events

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|---|
| `ConnectionState` {Closed, Open, Connecting, Reconnecting, Failed} | NC:14-21,138 | PRESENT* `Status()` → `Core.ClientStatus` {disconnected, connected, closed, reconnecting, connecting, draining_subs, draining_pubs} (Core.cs:906), plus `IsConnected`/`IsClosed`/`IsReconnecting`/`IsDraining` | `Status` go:187, `Conn.Status` go:6463; implemented | oracle-backed | NQ has no `Failed` state: auth abort or exhausted retries end as `closed` plus `LastError`. It is a method, not a property, and the enum names are snake_case. |
| `ConnectionOpened` (connect and reconnect) | NC:122 | PRESENT `ConnectionOpened` qNC:255 (fires for both, as in nats.net), plus `ConnectedHandler`/`ReconnectHandler` | `ConnectHandler` go:1427, `ReconnectHandler` go:1435; implemented | oracle-backed | |
| `ConnectionDisconnected` | NC:120 | PRESENT qNC:257 (args carry `Error`) | `DisconnectErrHandler` go:1410; implemented | oracle-backed | |
| `ReconnectFailed` | NC:124 | PRESENT qNC:261 | `Options.ReconnectErrCB` go:488; implemented | oracle-backed | |
| Closed event | — (nats.net has none) | NQ has more: `ConnectionClosed` qNC:259 | `ClosedHandler` go:1451 | — | Not a gap. |
| `MessageDropped`: per dropped message, with subscription, pending count, subject, reply, headers and data | NC:126,292-303; `NatsEventArgs.cs`:25-49 | PARTIAL. NQ `MessageDropped` (qNC:263) fires once per slow-consumer episode, on the rising edge, with `NatsErrorEventArgs(error, subscription)`. Per-message drops show only as the `NatsSubscription.Dropped` counter. NQ README.md documents this. | `ErrSlowConsumer` via `AsyncErrorCB`; `Subscription.Dropped` go:5920; implemented | NQ follows nats.go. The per-message event carrying the message is nats.net-only. | |
| `SlowConsumerDetected` (once per episode, re-armed when the subscription recovers) | NC:128,305-313 | PRESENT* under another name: NQ's `MessageDropped` plus `Error` events and `NatsSubStatus.SlowConsumer` | `ErrSlowConsumer` go (`ErrSlowConsumer` implemented) | oracle-backed | There is no event named `SlowConsumerDetected`. The event arguments carry only the subscription. |
| `SuppressSlowConsumerWarnings` | NO:389 | N/A. NQ has no logger. | none | .NET idiom (logging) | |
| `LameDuckModeActivated` (args: server `Uri`) | NC:130,164-166 | PRESENT qNC:269 | `LameDuckModeHandler` go:1686; implemented | oracle-backed | NQ args are `NatsEventArgs("lame duck mode")` with no server URI (qNC:1121). |
| `ServerError` event for every `-ERR`, with `NatsServerErrorKind` classification | NC:132; `NatsEventArgs.cs`:4-16,70-110; `Internal/NatsReadProtocolProcessor.cs`:119-138 | PARTIAL. The NQ `Error` event (qNC:265) carries `NatsException.Code` (`Core.ErrorCode`), mapped by compiled `classify_server_error` (qNC:1580-1594) into the same 9 kinds. NQ follows nats.go: a terminal server error (one that closes the connection) is recorded in `LastError` without raising the event (qNC:1724-1725). No raw `Error` string property. | `ErrorHandler` go:1467, `processErr` go:4385; caps `ErrorHandler`→`NatsConnection.Error` implemented | oracle-backed (event-coverage divergence) | **[new]** #745, #1148 |
| `ServersDiscovered` | — | NQ has more: qNC:267 | `DiscoveredServersHandler` go:1459 | — | Not a gap. |
| `OnSubscribed` (`NatsSubOpts.Events`): callback once SUB is queued, for the lazy `SubscribeAsync` enumerable | `NatsSubEvents.cs`:31; `NatsConnection.LowLevelApi.cs`:6-39 | MISSING (grep `OnSubscribed`: no match). NQ `SubscribeAsync` (qNC:366) is also lazy; `SubscribeCoreAsync` is eager and is the workaround. | none (nats.go subscribe is synchronous) | nats.net-only (.NET async-enumerable idiom) | **[new]** #1217 |
| Async event dispatcher (ordered, never blocks the reader) | NC:113-114,994-1029 | PRESENT `callbacks` channel qNC:226 | `asyncCallbacksHandler` | oracle-backed | |

## 3. NatsOpts, field by field (NO = nats.net NatsOpts.cs)

| nats.net property (line, default) | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|
| `Url` (69, `nats://localhost:4222`, comma list) | PRESENT `Url` qNO:77, plus `PreferredUrl`:79 and `Servers`:81 | `Connect(url)` go:1115, `Options.Url`/`Servers` go:314,323; implemented | oracle-backed | |
| URL credentials (`user:pass@`, `token@`) | PRESENT* via the compiled `select_connect_credentials` (Core.cs:6037) | `connectProto` go:3089-3114 | oracle-backed (precedence differs) | In nats.net, AuthOpts override URL credentials, and only the first URL's credentials are used (NO:405-442). NQ and nats.go use the current server's URL credentials, which beat the options. |
| `Name` (71, `"NATS .NET Client"`) | PRESENT `Name` qNO:82 (default "") | `Name` go:1131; implemented | oracle-backed | Default differs. |
| `Echo` (73) | PRESENT `Echo` qNO:95 | `NoEcho` go:1273; implemented | oracle-backed | |
| `Verbose` (75) | PRESENT `Verbose` qNO:96 | `Options.Verbose` go:340; implemented | oracle-backed | |
| `Headers` (77): CONNECT `headers:false` opt-out | MISSING. NQ always negotiates headers (`negotiate_connect_features`). | none (go:3089+ always sends headers:true) | nats.net-only | Low value. |
| `AuthOpts` (79) | see §5 | | | |
| `TlsOpts` (91) | see §4 | | | |
| `WebSocketOpts` (93) | see §4 | | | |
| `SerializerRegistry` (95) | MISSING (byte[]/string payloads only) | none | .NET idiom | Serialization area. |
| `LoggerFactory` (97) | MISSING. No `ILogger`/`LoggerFactory` anywhere in NQ (grep: 0). | none (nats.go has no logger) | .NET idiom | NQ surfaces state only through events, `LastError` and `StatusChanged`. |
| `WriterBufferSize` (99, 64 KiB): pipe pause threshold | PRESENT* `WriteBufferSize` qNO:139 (default 2 MiB; batch written inline once it holds this many bytes) | `WriteBufferSize` go:1365; implemented | oracle-backed | Default and semantics differ, but both give writer backpressure. |
| `ReaderBufferSize` (101, 64 KiB) | MISSING as an option. Fixed `ReadBufferSize = 256 KiB` (`Transport.cs`:24). | none (nats.go defaultBufSize is internal) | nats.net-only (tuning) | |
| `MaxPayloadHardCap` (108, 64 MiB): inbound MSG/HMSG size guard, drops the connection on violation | PARTIAL. The parser is hard-wired to `Parser(67108864, 1048576)` (`Parser.cs`:50,176,215); not configurable. | none (nats.go `parser.go` caps only the control line, `MAX_CONTROL_LINE_SIZE` parser.go:28) | nats.net-only | **[new]** #1095. The same 64 MiB value is already enforced; only the knob and nats.net's "violation means no reconnect" behaviour are missing. |
| `UseThreadPoolCallback` (110) | N/A | none | — | Dead in nats.net: no use outside NatsOpts.cs. |
| `InboxPrefix` (112) | PRESENT `InboxPrefix` qNO:115 | `CustomInboxPrefix` go:1726; implemented | oracle-backed | |
| `NoRandomize` (114) | PRESENT qNO:98 | `DontRandomize` go:1264; implemented | oracle-backed | |
| `PingInterval` (116) | PRESENT qNO:118 | `PingInterval` go:1333; implemented | oracle-backed | |
| `MaxPingOut` (118) | PRESENT `MaxPingsOut` qNO:119 | `MaxPingsOutstanding` go:1343; implemented | oracle-backed | |
| `ReconnectWaitMin` (123, 2 s) + `ReconnectWaitMax` (240, 5 s): exponential backoff (NC:1061-1117) | PARTIAL. Fixed `ReconnectWait` qNO:128 plus `CustomReconnectDelay(attempts)` qNO:132 (exponential backoff can be written there). No `ReconnectWaitMax` (no match). | `ReconnectWait` go:1282, `CustomReconnectDelay` go:1311 (type go:258); both implemented | the exponential policy is nats.net-only; reachable through the oracle-backed callback | |
| `ReconnectJitter` (128) | PRESENT qNO:129, plus `ReconnectJitterTls`:130 | `ReconnectJitter` go:1301; implemented | oracle-backed | |
| `ConnectTimeout` (130) | PRESENT qNO:116 | `Timeout` go:1374; implemented | oracle-backed | |
| `ObjectPoolSize` (132) | MISSING | none | .NET idiom (allocation tuning) | |
| `RequestTimeout` (134) | PRESENT qNO:122 | `Options.Timeout`-style default | oracle-backed | |
| `CommandTimeout` (136, 5 s): a publish blocked on a full writer throws `NatsTimeoutException` (`Commands/CommandWriter.cs`:746-770) | PARTIAL. No command timeout. A full batch is written inline under `WriteTimeout` (qNO:117, default 60 s, Core.cs:4022). | `FlusherTimeout` go:1382; implemented → `NatsOpts.WriteTimeout` | oracle-backed (different mechanism) | |
| `SubscriptionCleanUpInterval` (138) | MISSING | none | .NET idiom (weak-reference cleanup of abandoned subscriptions) | |
| `HeaderEncoding` (148, ASCII) | MISSING (fixed encoding) | none | nats.net-only | Headers area. |
| `CaseSensitiveHeaders` (160) | PRESENT* `NatsHeaders` is always ordinal (case-sensitive) (`NatsMsg.cs`:10) | nats.go `Header` map, case-sensitive | oracle-backed | **[new]** #1258. NQ behaves like nats.net with the option set to true. There is no case-insensitive mode. |
| `SubjectEncoding` (170, UTF-8) | MISSING as an option (UTF-8/WTF-8 fixed, qNC:620) | none | nats.net-only | |
| `WaitUntilSent` (172) | N/A | none | — | Dead in nats.net: declared, never read. |
| `DrainSubscriptionsOnDispose` / `ConsumerDrainOnDisposeTimeout` / `DrainPingTimeout` | see §1 | | | **[new]** |
| `MaxReconnectRetry` (230, −1 means unlimited) | PRESENT* `MaxReconnectRetry` qNO:127 (default 60, per-server attempts as in nats.go) | `MaxReconnects` go:1292; implemented | oracle-backed | Defaults and counting differ: nats.net counts wait cycles globally. |
| `IgnoreAuthErrorAbort` (246) | PRESENT qNO:126; per-server `record_auth_failure` (qNC:1709-1714) | `IgnoreAuthErrorAbort` go:1738, go:4164-4165; implemented | oracle-backed | nats.net stops after the same auth error twice in a row globally (NC:770-776), then enters `Failed`. NQ tracks it per server, as nats.go does. |
| `SubPendingChannelCapacity` (252, 16384) | PRESENT `SubPendingMessages` qNO:141 (default 65536, Core.cs:4205) | `SyncQueueLen` go:1625 / `SubChanLen`; implemented | oracle-backed | nats.net raised its default from 1024 in #1181 **[new]**. |
| `SubPendingChannelFullMode` (265, DropNewest; also Wait and DropOldest) | PARTIAL. Drop-newest only (nats.go slow consumer); no Wait or DropOldest mode. | none for Wait or DropOldest | nats.net-only | #1181 **[new]** unified the defaults. |
| `RequestReplyMode` (292, Direct by default) and its no-responders nuance (`DirectSetIntentionally`) | PARTIAL / different model: the nats.go response mux (≈ SharedInbox), plus `UseOldRequestStyle` qNO:103 | `UseOldRequestStyle` go:1665; implemented | nats.net-only (Direct is an implementation detail) | **[new]** #1182 changed the default. Request-reply area. |
| `SocketConnectionFactory` (313) | PARTIAL `CustomDialer` (`NatsDialer` returns a `Socket`, qNO:57,105) | `CustomDialer` interface go:301 returns any `net.Conn`; `SetCustomDialer` go:1657; implemented | oracle-backed (NQ is narrower than the oracle) | Neither nats.net's factory nor nats.go's `net.Conn` is limited to a socket. NQ's `Socket` return cannot carry an in-memory pipe, a QUIC stream or a proxy-wrapped stream. |
| `RetryOnInitialConnect` (336) | see §1 | | | |
| `PublishTimeoutOnDisconnected` (344): while disconnected, publish waits for reconnect, or throws after `CommandTimeout` | PARTIAL / different model. NQ buffers in the reconnect buffer (`ReconnectBufferSize` qNO:137, 8 MiB) and fails with `ErrReconnectBufExceeded`. | `ReconnectBufSize` go:1352; implemented | oracle-backed (semantics differ) | There is no "await reconnect" publish mode. |
| `SkipSubjectValidation` (372, obsolete) | PRESENT qNO:101 | `SkipSubjectValidation` go:1808; implemented | oracle-backed | **[new]** #1180 deprecated it in nats.net (default false). NQ's default is also false. |
| `BackoffWithJitterAsync` helper (454) | MISSING | none | nats.net-only helper | Trivial. |

NQ options that nats.net lacks:
- `Pedantic`, `AllowReconnect`, `ReconnectOnFlusherError`, `ReconnectToServerHandler`, `ReconnectJitterTls`, `IgnoreDiscoveredServers`, `SkipHostLookup`, `PermissionErrOnSubscribe`, `NoCallbacksAfterClose`.
- `Compression` (WebSocket permessage-deflate), `ProxyPath`, `DrainTimeout`, `PreferredUrl`/`Servers`.

## 4. Transport: TCP, TLS and WebSocket

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|---|
| `TlsMode.Auto` (default): upgrades whenever the server advertises `tls_available` | `NatsTlsOpts.cs`:29,161-171; NC:682 | PRESENT* follows nats.go. Upgrades only when the scheme is `tls://`, `Tls` is set, or the server sends `tls_required` (`resolve_tls_requirement`, qNC:279). | `checkForSecure` go:3018-3041 | oracle-backed (divergent default) | nats.net is opportunistic: it upgrades on `tls_available`. nats.go and NQ do not. |
| `TlsMode.Prefer` | `NatsTlsOpts.cs`:49, NC:682 | MISSING (no match for `Prefer`/`TlsMode`) | none (nats.go never upgrades on `tls_available` alone) | nats.net-only | |
| `TlsMode.Require` | `NatsTlsOpts.cs`:54, NC:676 | PRESENT `NatsOpts.Tls` / `tls://` → secure-wanted check | `Secure` go:1150, `ErrSecureConnWanted` go:3023; implemented | oracle-backed | |
| `TlsMode.Implicit` (TLS before INFO) | `NatsTlsOpts.cs`:59, NC:632-639 | PRESENT `NatsTlsOpts.HandshakeFirst` qNO:15 | `TLSHandshakeFirst` go:1763; implemented | oracle-backed | |
| `TlsMode.Disable`: never upgrade, and fail if the server requires TLS (TLS-terminating proxy) | `NatsTlsOpts.cs`:64, NC:670-674 | MISSING | none (go:3025 always switches to Secure when the server requires it) | nats.net-only | Needed behind a TLS-terminating proxy. NQ already never upgrades on `tls_available` alone, so only the "server requires but don't" case is uncovered. |
| `CertFile`/`KeyFile` (PEM) | `NatsTlsOpts.cs`:83,92 | PRESENT qNO:28-29 | `ClientCert` go:1230; implemented | oracle-backed | |
| `KeyFilePassword` (encrypted PEM key) | `NatsTlsOpts.cs`:97 | MISSING (no match) | none (`tls.LoadX509KeyPair`, no password) | nats.net-only | |
| `CertBundleFile` + `CertBundleFilePassword` (PFX/PKCS#12) | `NatsTlsOpts.cs`:106,111 | PARTIAL: no file option; the user can load a PFX into `ClientCertificates` (qNO:19) or `ClientCertificateHandler` (qNO:35) | `ClientTLSConfig`/`TLSCertCB` go:1169,355; implemented | nats.net-only convenience | |
| `CaFile` | `NatsTlsOpts.cs`:121 | PRESENT `CaFiles` (a list) qNO:32 | `RootCAs` go:1200; implemented | oracle-backed | |
| `InsecureSkipVerify` | `NatsTlsOpts.cs`:124 | PRESENT qNO:25 | via `Secure(tls.Config)` go:1150 | oracle-backed | |
| `ConfigureClientAuthentication`: async hook over `SslClientAuthenticationOptions` (TargetHost, EnabledSslProtocols, revocation mode, remote validation, local cert selection) | `NatsTlsOpts.cs`:116,241-254 | PARTIAL. Covered: `ServerName`, `CertificateValidation`, `ClientCertificates`/`RootCertificates` and the handlers. Not covered: `EnabledSslProtocols` (hard-coded `SslProtocols.None`, `Transport.cs`:156), revocation (hard-coded `NoCheck`, :157), `LocalCertificateSelectionCallback`, and async. | `Options.TLSConfig` go:352 / `Secure` go:1150; caps `Options.TLSConfig`→`NatsOpts.Tls` implemented | oracle-backed (`tls.Config` has MinVersion and VerifyConnection) | |
| TLS host for discovered IP-only servers | `FixTlsHost` NC:1031 | PRESENT through the compiled pool's `tls_name` (Core.cs, 7 hits) | `tlsName` go:2248-2264,2568 | oracle-backed | |
| WebSocket `ws://`/`wss://` | `Internal/WebSocketConnection` via `ClientWebSocket` | PRESENT `WebSocket.cs` / `Transport.UpgradeWebSocketAsync` (`Transport.cs`:171) | websocket.go; caps `Compression`/`ProxyPath` implemented | oracle-backed | nats.net ignores TlsOpts for `wss`. NQ applies NatsTlsOpts to `wss`. |
| `NatsWebSocketOpts.RequestHeaders` | `NatsWebSocketOpts.cs`:22 | PRESENT `WebSocketHeaders` qNO:111 | `WebSocketConnectionHeaders` go:1776; implemented | oracle-backed | |
| `ConfigureClientWebSocketOptions`: async per-URI hook over `ClientWebSocketOptions` (HTTP proxy, subprotocols, client certs, keep-alive, cookies, credentials) | `NatsWebSocketOpts.cs`:28 | PARTIAL. Dynamic headers via `WebSocketHeadersHandler` qNO:114. No HTTP proxy, keep-alive, cookies or async; the hand-rolled upgrade has no `ClientWebSocket`. | `WebSocketConnectionHeadersHandler` go:1791; implemented | headers are oracle-backed; proxy and the rest are nats.net-only | |
| `INatsSocketConnection` / `INatsSocketConnectionFactory` (custom transport: Send/Receive) | `NATS.Client.Abstractions/INatsSocketConnection.cs`:13; `INatsSocketConnectionFactory.cs` | MISSING (no match). Nearest is `CustomDialer` (socket only). | `CustomDialer` go:301 (any `net.Conn`) | oracle-backed (the oracle allows any conn) | **[new]** #1192 moved it to the Abstractions package. |
| `INatsTlsUpgradeableSocketConnection` (library performs TLS on a custom socket) | `INatsSocketConnection.cs`:37 | PRESENT* implicitly: a `CustomDialer` socket goes through the normal TLS upgrade | `CustomDialer` + `Secure` | oracle-backed | **[new]** location (#1192). |
| `OnConnectingAsync`: rewrite host:port before each dial | NC:155,604-615 | PARTIAL. `CustomDialer(network, address)` can dial elsewhere; `ReconnectToServerHandler` picks a pool member. Neither is async per-URI host rewriting that also changes the TLS target host. | `CustomDialer` go:301, `ReconnectToServerCB` go:495; implemented | oracle-backed | |
| `OnSocketAvailableAsync`: wrap the connected socket | NC:157,641-645 | MISSING for users. Only the internal `Transport.StreamFilter` test seam exists (`Transport.cs`:39). | `CustomDialer` returning a wrapping `net.Conn` go:301 | oracle-backed | |

## 5. Auth (`NatsAuthOpts`, `NatsAuthOpts.cs`)

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|---|
| `Username`/`Password` | :50,52 | PRESENT qNO:83-84 | `UserInfo` go:1476; implemented | oracle-backed | |
| `Token` | :54 | PRESENT qNO:85 | `Token` go:1494; implemented | oracle-backed | |
| `Jwt` + `Seed` | :56,60 | PRESENT `WithUserJwtAndSeed` qNO:151 | `UserJWTAndSeed` go:1566; implemented | oracle-backed | |
| `NKey` + `Seed` (in-memory nkey seed) | :58,60 | PARTIAL. `Nkey` + `SignatureHandler` (qNO:92-93) need a caller-supplied Ed25519 signer; NQ's Ed25519 and NKey codec are `internal` (`Ed25519.cs`:20, `Credentials.cs`:16). The only seed helper is file-based. | `Nkey(pub, sigCB)` go:1611; implemented | oracle-backed (the convenience is nats.net-only) | A `WithNkeySeed(string)` helper would be cheap. |
| `NKeyFile` | :66 | PRESENT `WithNkeyFromSeedFile` qNO:154 | `NkeyOptionFromSeed` go:6808; implemented | oracle-backed | |
| `Creds` (content) | :62 | PRESENT `WithUserCredentialBytes` qNO:149 | `UserCredentialBytes` go:1538; implemented | oracle-backed | |
| `CredsFile` | :64 | PRESENT `WithUserCredentials` qNO:146 | `UserCredentials` go:1519; implemented | oracle-backed | nats.net reads the file once; NQ and nats.go re-read it on each connect. |
| `AuthCredCallback`: async `Func<Uri, CancellationToken, ValueTask<NatsAuthCred>>`, called on each connect with the target server URI; the result may be any credential kind | :74; `Internal/UserCredentials.cs` AuthenticateAsync | PARTIAL. Synchronous, per-kind handlers: `UserInfoHandler`, `TokenHandler`, `UserJwtHandler`+`SignatureHandler` (qNO:87-92). Not provided: async, the server URI argument, or choosing the credential kind at runtime. | `UserInfoHandler` go:1484, `TokenHandler` go:1507, `UserJWT` go:1589; implemented | the handlers are oracle-backed; the async and per-URI parts are nats.net-only | |
| Sign the nonce regardless of `auth_required` | #1109 **[new]** | PRESENT (nats.go behaviour) | `connectProto` go:3128+ | oracle-backed | |

## 6. Ping, server info, statistics and pooling

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | Class | Notes |
|---|---|---|---|---|---|
| `PingAsync(CancellationToken)` → RTT | `NatsConnection.Ping.cs`:12 | PARTIAL `PingAsync()` qNC:147 takes no `CancellationToken` | `RTT` go:6058; implemented | oracle-backed | Small API gap. nats.net also auto-connects here. |
| Ping write timeout counted as a missed PONG | NC:1143-1152 **[new]** #1266 | PRESENT by construction. Compiled `ping_loop` checks staleness and increments `outstanding_pings` *before* `leaf_ping_write` (Core.cs:21606-21621). The write only appends to the outbound pipe (qNC:679), so a stuck write cannot hide a missed PONG. | `processPingTimer` go:6002-6011 (`pout++`, then `sendPing`) | oracle-backed | No action needed. A vector or DST scenario for "socket write stalls with pings outstanding → stale" would document it. |
| `ServerInfo` (`INatsServerInfo`) | `INatsServerInfo.cs` | PARTIAL `ServerInfo`/`ServerMetadata` → `Core.InfoValues` (Core.cs:3204). Missing: `git_commit`, `go`, `tls_verify`, `cluster_dynamic`, `ws_connect_urls`. | `ServerInfo` go:1048-1075 has none of those five | nats.net-only (nats-server INFO fields) | NQ has more: `domain`, `api_lvl`, `acc_is_sys`. |
| Statistics | `NatsStats.cs`; `GetStats()` is **internal** (NC:415) | NQ has more: public `Stats()` qNC:183 (in/out msgs and bytes, reconnects), plus `Buffered` and `NumSubscriptions` | `Stats` go:6500, `Statistics` go:1020; implemented | — | nats.net exposes counters only through OTel metrics. |
| `NatsConnectionPool` / `INatsConnectionPool` / `NatsPooledConnection` | `NatsConnectionPool.cs`:3-61 | MISSING (no match for `ConnectionPool`) | none | .NET idiom | Round-robin pool, also used by DI `WithPoolSize`. |
| `INatsConnection` / `INatsClient` interfaces | `INatsConnection.cs`, `INatsClient.cs` | MISSING. NQ `NatsConnection` is `sealed`, with no interface. | none | .NET idiom (mocking, DI) | |
| Extension points: `AddSubAsync(NatsSubBase)`, `SubscriptionManager`, `HeaderParser`, `GetBoundedChannelOpts`, `OnMessageDropped` | `INatsConnection.cs` | MISSING | none | nats.net-only | |
| DI (`NATS.Extensions.Microsoft.DependencyInjection` `NatsBuilder`) / `NatsClient` | `src/NATS.Extensions.Microsoft.DependencyInjection/NatsBuilder.cs` | MISSING | none | .NET idiom | Outside the core package. |

## Prioritized significant gaps

1. **Lazy connect, and `RetryOnInitialConnect` blocking semantics** (nats.net-only / semantics differ).
   - nats.net code commonly never calls `ConnectAsync`, or relies on it blocking until a server is up.
   - NQ follows nats.go: an explicit `ConnectAsync`, and `RetryOnFailedConnect` returns while still reconnecting.
   - This is the biggest drop-in-compatibility gap. Behaviour sits in the shell API shape, not on the wire.
2. **TLS modes Prefer and Disable** (nats.net-only).
   - NQ follows nats.go's `checkForSecure` (go:3018).
   - nats.net's default `Auto` upgrades on `tls_available`, so the TLS default diverges. `Disable` (TLS-terminating proxies) has no NQ equivalent.
   - Needs nats.net or a live server as the reference. Recording it in `natsserver-deferred.md`-style docs would be the minimum.
3. **Custom transport** (`INatsSocketConnectionFactory`, `OnSocketAvailableAsync`) (oracle-backed: nats.go `CustomDialer` returns any `net.Conn`).
   - NQ's `NatsDialer` returns a `Socket` only.
   - Widening it to return a `Stream` would meet both the oracle and nats.net.
4. **`ServerError` event coverage and shape** **[new]**.
   - The NQ `Error` event carries an `ErrorCode` but, per nats.go, skips terminal server errors and keeps no raw error text property.
   - nats.net raises an event for every `-ERR`, with a Kind.
5. **Per-message `MessageDropped` with the message**, and **`SubPendingChannelFullMode.Wait`/`DropOldest`** (nats.net-only).
   - NQ gives one event per slow-consumer episode plus the `Dropped()` counter, and drop-newest only.
6. **Auth callback model** (`AuthCredCallback`: async, per-server URI, any credential kind) and an **in-memory NKey seed**.
   - The in-memory seed is oracle-backed through `Nkey`+`SignatureHandler`, but NQ's signer is internal.
   - Possible fixes: a public `WithNkeySeed`, or async handler overloads.
7. **Reconnect backoff** (`ReconnectWaitMin`/`ReconnectWaitMax`, exponential).
   - Expressible today through `CustomReconnectDelay`.
   - Defaults differ: `MaxReconnectRetry` is −1 in nats.net and 60 in NQ, so nats.net users get unlimited retries and NQ users do not.
8. **Write and backpressure knobs**: `CommandTimeout`, `PublishTimeoutOnDisconnected`, `ReaderBufferSize`, `MaxPayloadHardCap` **[new]**.
   - NQ has a different (nats.go) model: inline write under `WriteTimeout` (60 s), the reconnect buffer, and a fixed 64 MiB parser cap.
   - `MaxPayloadHardCap` could become a configurable parser limit cheaply.
9. **TLS knobs**: `KeyFilePassword`, `CertBundleFile` (PFX), and from `ConfigureClientAuthentication`: `EnabledSslProtocols`, revocation mode and local certificate selection.
   - NQ hard-codes `SslProtocols.None` and `X509RevocationMode.NoCheck` (`Transport.cs`:156-157).
10. **.NET idioms, low protocol value**: `ILoggerFactory` logging, `NatsConnectionPool`, `INatsConnection`/`INatsClient` interfaces, DI, `SerializerRegistry`, `OnSubscribed` **[new]**, `PingAsync(CancellationToken)`, `ServerInfo` fields `git_commit`/`go`/`tls_verify`/`cluster_dynamic`/`ws_connect_urls`, the `Headers` CONNECT opt-out, and `HeaderEncoding`/`SubjectEncoding`.

Not gaps:
- Ping write timeout as a missed PONG (#1266) is already NQ behaviour through the oracle's `pout++`-before-send ordering.
- Statistics, connection drain, the Closed event and discovered-servers events are areas where NQ exceeds nats.net.
- nats.net's `ConnectRetryAsync` is a test helper, and `UseThreadPoolCallback` and `WaitUntilSent` are dead options.
