# nats.net features missing from NQ's C# client

This audit compares three things:

- nats.net at `15bd0bf`: this branch, synced to nats-io/nats.net main, v3.3.0.
- NQ's generated C# client, `csharp.async`, at wallyqs/nq.dev `78d38eb` (`packages/csharp`, `NQ.Nats`).
- NQ's behavioural oracles:
  - nats.go `7a8404ab` (v1.54.0), including `jetstream/` and `micro/`;
  - orbit.go `8898afea` (`jetstreamext`, `natsext`).

Each gap is classed as one of:

- **oracle-backed**: nats.go or orbit.go has an equivalent. NQ can implement it under its normal rule: IR plus measured oracle evidence.
- **nats.net-only**: Go has no equivalent. Adopting it means using nats.net or nats-server as the reference, or recording a divergence.
- **idiom**: a .NET packaging or ergonomics feature with no protocol behaviour, such as DI, `ILogger` or generics.

**[new]** marks a gap that comes from nats.net commits after `8bb0fb7`, the fork's previous head. That range is 144 commits, 3.0.0-preview through 3.3.0.

The detailed reports, with `file:line` cites in all three codebases:

| Area | Report |
| --- | --- |
| Connection, options, transport, auth, events | [gap-core-connection.md](gap-core-connection.md) |
| Publish/subscribe/request, serializers, headers, OTel, DI, Simplified | [gap-core-messaging.md](gap-core-messaging.md) |
| JetStream (streams, consumers, consume, ack, publish, batch) | [gap-jetstream.md](gap-jetstream.md) |
| KeyValue, Object Store, Services | [gap-kv-obj-svc.md](gap-kv-obj-svc.md) |

## Bottom line

NQ C# already implements almost every nats.net feature that has a nats.go or orbit.go equivalent. Its records show:

- core: 269 implemented, 13 reviewed unsupported;
- services: 120 of 121;
- JetStream: 394 implemented;
- orbit: 63 implemented.

What remains splits into four groups:

1. A short list of **oracle-backed** gaps.
2. A larger set of **nats.net-only** behaviours. In most of these NQ deliberately follows nats.go semantics.
3. The .NET **idiom** layer: typed serializers, OTel, DI, logging, interfaces.
4. **Capability-record debt**: most of JetStream's 510 "planned" symbols already have C# bindings.

## 1. Oracle-backed gaps (implementable under NQ's normal rules)

| Gap | Oracle | Notes |
| --- | --- | --- |
| `RequestManyAsync` (sentinel, max msgs, stall/idle timeout) | orbit.go `natsext.RequestMany`, `requestmany.go:37-101` | NQ has no capability contract for `natsext`. This is the best candidate. |
| Batch direct get: `GetBatch`, `GetLastMsgsFor` | orbit.go `jetstreamext/getbatch.go:128,191` | 15 of the 40 planned orbit symbols. nats.net lacks it too. |
| Custom transport that returns a stream, not just a `Socket` (nats.net `INatsSocketConnectionFactory`, `OnSocketAvailableAsync`) [new: moved to Abstractions in #1192] | nats.go `CustomDialer` returns any `net.Conn` (`nats.go:301`) | NQ's `NatsDialer` returns only a `Socket`. |
| Public in-memory NKey seed | nats.go `Nkey` + `SignatureHandler` | NQ's Ed25519 is internal; only `WithNkeyFromSeedFile` exists. |
| Unbound names: `ErrEndOfData`, `ErrConsumerHasActiveSubscription`, `MigrationStatus*`, `DefaultAPIPrefix`, direct-get header constants | nats.go `jetstream/errors.go:312,320`, `stream_config.go:548-576`, `message.go:270-292` | Small. |

## 2. nats.net-only behaviour (no Go oracle; NQ follows nats.go)

### Connection

- **Lazy connect.** nats.net's publish, subscribe and request call `ConnectAsync` themselves; NQ requires an explicit `ConnectAsync`.
- **`RetryOnInitialConnect` blocks until connected.** NQ's `RetryOnFailedConnect` returns at once, in the reconnecting state.
- **TLS `Prefer`/`Disable`, and `Auto` resolving to Prefer.** nats.net upgrades opportunistically on `tls_available` and can disable TLS for terminating proxies. NQ follows nats.go `checkForSecure` (`nats.go:3018-3041`).
- **Auth.** nats.net has `AuthCredCallback`: async, per server URI, returning any credential kind. NQ has only the sync per-kind handlers. Precedence also differs: nats.net's AuthOpts override URL credentials, while NQ follows nats.go, where the URL wins.
- **Events.**
  - **`ServerError` [new].** nats.net raises it for every `-ERR`, with a kind and the raw text. NQ's `Error` event, like nats.go, skips terminal server errors and has no raw-text property.
  - **`MessageDropped` and full-channel modes.** nats.net raises `MessageDropped` per dropped message and offers `SubPendingChannelFullMode.Wait` and `DropOldest` [new #1181]. NQ fires once per slow-consumer episode, plus a counter, and only drops the newest.
  - **`OnSubscribed` [new #1217].**
- **Options.**
  - **Different model:** `ReconnectWaitMin`/`Max` exponential backoff (NQ: only through `CustomReconnectDelay`) and `MaxReconnectRetry` (nats.net −1, NQ 60, counted per server).
  - **No NQ equivalent:** `CommandTimeout`, `PublishTimeoutOnDisconnected`, `ReaderBufferSize` (NQ is fixed at 256 KiB), `MaxPayloadHardCap` [new #1095] (NQ is fixed at 64 MiB), `DrainSubscriptionsOnDispose`/`DrainPingTimeout` [new #1085], the `Headers` CONNECT opt-out, `HeaderEncoding`/`SubjectEncoding`.
  - **TLS:** `KeyFilePassword`, `CertBundleFile` (PFX), SSL protocol and revocation settings (NQ hard-codes `SslProtocols.None` and `NoCheck`). WebSocket proxy and keep-alive.

### Messaging

- **No-responders is a divergence.** NQ always throws `NatsNoRespondersException`, as nats.go does; nats.net delivers a 503 message unless `ThrowIfNoResponders` is set.
- **`NatsSubOpts`:** `Timeout`, `IdleTimeout`, `StartUpTimeout` [new #1134], `MaxMsgs` inline (NQ: `AutoUnsubscribeAsync`), `NatsSubEndReason`.
- **`RequestReplyMode`** Direct versus SharedInbox [new #1182].
- **`NatsPubOpts`** and `CreateRequestSubAsync`, plus the low-level `AddSubAsync` and custom `NatsSubBase` extension points.
- **Headers.** nats.net defaults to case-insensitive keys, with case-sensitive mode as an opt-in [new #1258]. NQ is case-sensitive only, as nats.go is. NQ also lacks `TryGetLastValue`.
- **`NatsMsg`.** NQ has no status `Code` enum, `HasNoResponders`, `EnsureSuccess` or per-message deserialize `Error`.

### JetStream

- **Unmodelled server fields.**
  - Consumer config: `ConsumerConfig.Sourcing` [new #1249] and `ConsumerConfig.Direct`.
  - List responses: `missing`/`offline` on stream and consumer lists [new #1248].
  - Stream info: `StreamInfo.Alternates`, `StreamState.Lost`, `StreamSourceInfo.External`/`Error`.
- **Ordered push consumer.** NQ has no `CreateOrderedPushConsumerAsync`; the push API itself is present, which covers #1231.
- **Convenience methods on objects.**
  - Consumer objects lack `ResetAsync`, `UnpinAsync` and `DeleteAsync` [new #1250].
  - Streams lack `NatsJSStream.DeleteAsync`/`UpdateAsync`.
  - Purge returns no purged count.
- **Ergonomics.**
  - `TryPublishAsync`, `EnsureSuccess` and `NatsJSDuplicateMessageException`.
  - `MaxConsecutive503Errors`, context-wide `DoubleAck` (NQ double-acks only +ACK, as nats.go does), and `DefaultConsumeOpts`/`DefaultNextOpts`.
  - Consume as an `IAsyncEnumerable` (NQ uses a callback plus a `MessagesAsync` iterator), `DrainOnCancel` [new #1177], non-error consume notifications, and an opt-in for list cancellation [new #1214].
- **Raw `JSRequestResponseAsync`** and the admin models behind it: snapshot [new #1088], restore, stepdown, peer-remove.
- **`$JS.FC` reply subjects in the metadata parser** [new #1127]. nats.go rejects these too (`internal/parser/parse.go:88`), so adopting them would be a recorded divergence.

### KeyValue, Object Store, Services

- **KV:**
  - `PurgeDeletes` `RetainRecentlyDeletedKeyHistory` [new #1252];
  - `Try*`/`NatsResult` variants;
  - watch `IdleHeartbeat` and `OnNoData`;
  - the `UseDirectGetApiWithKeysInSubject` toggle;
  - a public `IsValidKey`;
  - `WatcherThrowOnCancellation` [new; mostly not applicable to NQ's channel watchers].
- **Object Store:**
  - `GetAsync(key, Stream)` and `PutAsync(key, Stream)` overloads;
  - `AddLinkAsync(string, string)`;
  - watch `InitialSetOnly` and `OnNoData`;
  - chunk-count and size checks on Get (nats.go checks only the digest).
- **Services:**
  - `RemoveEndpointAsync`, stopping one endpoint at a time [new #1255];
  - an automatic error reply when a handler throws (`NatsSvcEndpointException` or 999);
  - requester-side `IsServiceSuccess`, `EnsureServiceSuccess`, `GetServiceStatus` [new];
  - the `AddServiceAsync(name, version, queueGroup)` overload;
  - endpoints named by subject only;
  - `IAsyncDisposable`.

## 3. .NET idiom layer (entirely absent in NQ)

- **Typed serialization.** nats.net has `INatsSerialize`/`INatsDeserialize`/`INatsSerializerRegistry` and the raw, UTF-8 and default serializers. It also has `NatsMsg<T>`, `PublishAsync<T>`, `SubscribeAsync<T>`, `RequestAsync<TReq,TRes>`, `NatsJsonSerializer` (with `JsonWriterOptions` [new #1253]) and source-generated JSON contexts. The same applies to typed KV, Object Store, JetStream and services endpoints. NQ is `byte[]`/`string` only.
- **OpenTelemetry [new: #1154, #1172, #1194, #1195, #1201, #1208, #1229, #1236].** ActivitySource tracing injects `traceparent` headers, which reach the wire. There are also Meter metrics, filter, enrich and span-name formatting, baggage, and JetStream, KV-watch and service endpoint spans. NQ has none of this.
- **Logging.** NQ has no `ILoggerFactory` and no logging at all.
- **Packaging and hosting.** NQ has none of these: `INatsConnection`/`INatsClient` interfaces and `NATS.Client.Abstractions` [new #1192]; `NatsClient` (Simplified) and `NATS.Net`; DI `AddNatsClient`/`NatsBuilder` and Hosting `AddNats`; `NatsConnectionPool`; `INatsJSContextFactory`. NQ's `NatsConnection` is sealed.
- **Zero-copy buffers.** NQ copies payloads into `byte[]`; it has no `NatsMemoryOwner`, `NatsBufferWriter`, `IBufferWriter` serialization, or `ReadOnlySequence`/`ReadOnlyMemory` publish.

## 4. NQ record and IR debt found along the way

- **JetStream "planned" symbols are mostly evidence gaps.** About 16 of the 439 planned non-KV symbols have no C# surface. The rest (records, fields, enums, `JSErrCode*`, listers) are bound but cannot be measured by the evidence gate. All 71 planned KV and Object Store symbols have C# counterparts, and 25 of the 40 planned orbit symbols are bound batch-publish types.
- **Stale records.** In `ir/capabilities/jetstream/csharp.async.json`, `jetstream.ConsumerManager.PauseConsumer`, `ResumeConsumer`, `ResetConsumer` and `ResetConsumerToSequence` are "planned", but `NatsJSContext.PauseConsumerAsync`/`ResetConsumerAsync(stream, name, …)` exist (`JetStream.cs:2366,2378`).
- **Stale header annotation.** `ir/headers.nqir:163,167` say "nats.go writes keys unchecked". The pinned nats.go rejects invalid header keys with `ErrBadHeaderMsg` (`nats.go:884-887`, `isHeaderKeyValid`) and replaces CR/LF in values with spaces (`nats.go:996-1011`). NQ instead throws its own "invalid header key/value" error. That agrees with nats.net (`HeaderWriter.cs:91,106`) but not with the oracle, so it should be re-adjudicated.
- **KV with AllowDirect disabled (nats.net #1240).** NQ handles it, following nats.go, but has no end-to-end test against such a bucket.

## Already covered in NQ (no action)

- **Recent nats.net fixes NQ already matches:**
  - #1266: a ping write timeout counts as a missed pong. NQ's compiled `ping_loop` increments `outstanding_pings` before writing, matching nats.go `processPingTimer`.
  - #1240: KV non-direct get.
  - #1149: KV duplicate-window cap.
  - #1265: v2 ack metadata.
- **nats.net features NQ already has:** consumer reset; priority groups with pinning, overflow and prioritized; every nats.go `StreamConfig`/`ConsumerConfig` field; ordered pull consumers; term with a reason.
- **NQ is ahead of nats.net on:**
  - per-message TTL, schedules, and atomic and fast batch publish (orbit);
  - public `Stats()`, connection `DrainAsync`, `ServersDiscovered`;
  - KV mirror buckets;
  - Object Store update, list and names.

## Suggested order

1. `RequestManyAsync` (orbit `natsext`) and batch direct get (orbit `jetstreamext`). Both are oracle-backed and close real gaps.
2. A stream-returning custom dialer and a public NKey-seed signer. Both are oracle-backed.
3. Record the nats.go-versus-nats.net divergences: TLS Prefer/Auto, lazy connect, no-responders as an exception, header case, `MaxReconnectRetry` default, and URL versus AuthOpts precedence. Then decide case by case whether to add opt-in nats.net modes.
4. Model the missing server fields (`ConsumerConfig.Sourcing`/`Direct`, list `offline`, `StreamState.Lost`) and add the object convenience methods. These are cheap and decision-free.
5. Fix the record debt: stale `ConsumerManager.*` records, the header annotation, and type/field evidence.
6. The idiom layer, if NQ C# aims to be a drop-in for nats.net: serializers and generics, OTel (which affects the wire), logging and the interfaces.
