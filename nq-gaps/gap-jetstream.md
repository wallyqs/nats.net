# Gap audit: nats.net `NATS.Client.JetStream` (not KV/ObjectStore) vs NQ C# (`packages/csharp`)

Sources compared:
- **nats.net** v3.3.0, `/home/user/nats.net/src/NATS.Client.JetStream`. Paths below are relative to that directory.
- **NQ C#** `/home/user/nq.dev/packages/csharp/src/NQ/JetStream.cs`. "JS:NNNN" means `JetStream.cs:NNNN`, and "Batch:NNN" means `JetStreamBatch.cs:NNN`.
- **Go oracle** nats.go `7a8404a` (`/home/user/oracle/nats.go/jetstream`) and orbit.go `jetstreamext`.
- **Capabilities** `ir/capabilities/jetstream/csharp.async.json` (394 implemented, 510 planned) and `ir/capabilities/orbit/csharp.async.json` (63 implemented, 40 planned).

"New since 8bb0fb7" means the item comes from the 31 commits in `git log 8bb0fb7..HEAD -- src/NATS.Client.JetStream`.

Class column:
- **oracle-backed**: nats.go or orbit.go has an equivalent.
- **nats.net-only**: there is no Go equivalent.
- **NQ ahead**: NQ has it and nats.net does not.

## 1. Feature table

### Context, options, account

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| Create context | NatsClientExtensions.cs:14-40, NatsJSContext.cs:20,30 | PRESENT: `NatsJS.CreateJetStreamContext` JS:826 | `jetstream.New` | oracle-backed (implemented) | |
| API prefix / domain | NatsJSOpts.cs:8-24 (`ApiPrefix`, `Domain`, `Prefix`) | PRESENT: `CreateJetStreamContextWithApiPrefix` JS:832 and `...WithDomain` JS:837. `js.Options` (`NatsJSOptions` record) JS:437 | `NewWithAPIPrefix`, `NewWithDomain` | implemented | `jetstream.DefaultAPIPrefix` (api.go:39) is not a public C# constant (planned) |
| Request timeout | NatsJSOpts.cs:26 `RequestTimeout` | PRESENT: `NatsJSOpts.DefaultTimeout` JS:431-434 and `js.WithTimeout` JS:2136 | `WithDefaultTimeout` | oracle-backed | |
| Client trace | none | PRESENT: `NatsJSClientTrace` JS:423 | `WithClientTrace` | NQ ahead | |
| Context-wide `DoubleAck` default | NatsJSOpts.cs:49 | MISSING. Only per-call `DoubleAckAsync` JS:2952 | none (Go has only `Msg.DoubleAck`) | nats.net-only | |
| `DefaultConsumeOpts` / `DefaultNextOpts` | NatsJSOpts.cs:57,62 | MISSING. Defaults are fixed at nats.go's `DefaultMaxMessages` and `DefaultExpires` (JS:991-993) | none | nats.net-only | nats.net's default MaxMsgs is 1000; nats.go's is 500 |
| `ThrowOnListCancellation` (#1214) | NatsJSOpts.cs:73, NatsJSContext.Streams.cs:189 | PARTIAL. The listers (JS:2293, 2315, 4854, 4875) pass the token into every page request, so a cancel throws `OperationCanceledException` when the next page is requested. There is no opt-in switch. A cancel while an already-fetched last page is being yielded completes silently. | lister `Err()` returns `ctx.Err()` | toggle is nats.net-only | **new since 8bb0fb7**. NQ behaves like nats.net's throw mode, except on the last page |
| Context factory (DI) | INatsJSContextFactory.cs, NatsJSContextFactory.cs:7,14 | MISSING | none | nats.net-only | |
| Raw API request `JSRequestResponseAsync<TReq,TRes>` | NatsJSContext.cs:366 | MISSING (the API plane `NatsConnection.JetStreamContext` is internal) | none public | nats.net-only | This is how nats.net users reach the model-only APIs (snapshot, restore, stepdown, templates and others; see Streams) |
| `NewBaseInbox` | NatsJSContext.cs:363 | MISSING | none | nats.net-only | trivial |
| Account info | NatsJSContext.cs:47, Models/AccountStats.cs, Tier.cs, AccountLimits.cs, ApiStats.cs | PRESENT: `GetAccountInfoAsync` JS:2217. `AccountInfo` JS:4961 has every nats.net field plus `ReservedMemory`/`ReservedStore` | `JetStream.AccountInfo` jetstream.go:51 | implemented | the AccountInfo record and its fields are "planned" in caps (records are never evidenced) |

### Streams

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| Create / Update / CreateOrUpdate stream | NatsJSContext.Streams.cs:18,162,41 | PRESENT: JS:2225, 2228, 2231 | `StreamManager.*` | implemented | |
| Delete stream | Streams.cs:68 | PRESENT: `DeleteStreamAsync` JS:2280 | jetstream.go | implemented | returns void (nats.net returns bool) |
| Get stream with `StreamInfoRequest` (deleted details, subjects filter, offset) | Streams.cs:139, Models/StreamInfoRequest.cs | PRESENT, different shape: `GetStreamAsync(name)` JS:2257, then `NatsJSStream.InfoAsync(StreamInfoOptions{DeletedDetails, SubjectFilter})` JS:440, 4690. Subjects are fetched page by page | `Stream.Info(opts)` stream.go:36. `WithDeletedDetails`/`WithSubjectFilter` jetstream_options.go:583,595 | oracle-backed. The ctors are "planned" but bound as `StreamInfoOptions` | |
| `StreamNameBySubject` | none | PRESENT JS:2265 | jetstream.go:153 | NQ ahead | |
| Purge (filter / seq / keep) | Streams.cs:91 `PurgeStreamAsync`, NatsJSStream.cs:59, Models/StreamPurgeRequest.cs | PRESENT: `NatsJSStream.PurgeAsync(PurgeOptions)` JS:452, 4704, including the seq+keep check. PARTIAL: it returns no `Purged` count, and there is no purge on the context | `Stream.Purge` stream.go:45 (returns only an error). `WithPurge*` jetstream_options.go:94-117 | the count and the context-level call are nats.net-only | |
| Delete message (erase / no_erase) | Streams.cs:115 (context), NatsJSStream.cs:73 | PRESENT on the stream only: `DeleteMsgAsync` JS:4777, `SecureDeleteMsgAsync` JS:4780 | stream.go:56,61 | context-level call is nats.net-only | |
| List streams (subject filter) | Streams.cs:182 | PRESENT `ListStreamsAsync(subject)` JS:2293 | jetstream.go:161 | implemented | yields `StreamInfo`; nats.net yields stream objects |
| List stream names | Streams.cs:219 | PRESENT JS:2315 | jetstream.go:165 | implemented | |
| Offline / missing assets in list responses (#1248) | Models/StreamListResponse.cs:22,30, ConsumerListResponse.cs:22,30 | MISSING. `grep Offline` matches only PeerInfo (JS:4978, 4988) | none. nats.go list responses have no `missing`/`offline`; only `PeerInfo.Offline` (stream_config.go:370, 406) | nats.net-only | **new since 8bb0fb7**. nats.net only adds them to the models; its `ListStreamsAsync` doesn't expose them either |
| Stream object Delete / Update / Refresh | NatsJSStream.cs:44, 86, 184 | PARTIAL: `InfoAsync` acts as Refresh (JS:4690). No `DeleteAsync` or `UpdateAsync` on `NatsJSStream` (grep `DeleteAsync\(`, `UpdateAsync\(`: no match) | Go's `Stream` interface (stream.go:31-61) has neither | nats.net-only | |
| StreamConfig, field by field | Models/StreamConfig.cs (37 json fields) | PRESENT. All 37 are in `StreamConfig` JS:5261-5301. Name mapping: `NumReplicas`→`Replicas`, `DuplicateWindow`→`Duplicates`, `AllowRollupHdrs`→`AllowRollup`, `TemplateOwner`→`Template`, `AllowMsgTTL`→`AllowMsgTtl`. `AllowBatchPublish` (#1120) JS:5300. NQ also has `FirstSeq` and `ConsumerLimits`, which nats.net lacks | stream_config.go:64-219 | oracle-backed (all 113 config/record fields "planned", see §2) | durations are `long` ns (nats.net uses `TimeSpan`) |
| Mirroring / sourcing (`StreamSource`, `External`, `Consumer` #1128) | Models/StreamSource.cs:64, StreamConsumerSource.cs, ExternalStreamSource.cs | PRESENT: `StreamSource` JS:5231 (includes `Consumer` → `StreamConsumerSource` JS:5225, plus `Domain`). `ExternalStream` JS:5157 | stream_config.go StreamSource | oracle-backed | #1128 is new since 8bb0fb7 and present |
| Subject transforms (stream and per source) | StreamConfig.cs:46, SubjectTransform.cs, StreamSource `subject_transforms` | PRESENT: `SubjectTransformConfig` JS:5219 | stream_config.go:170 | oracle-backed | |
| `StreamInfo.Alternates` | Models/StreamInfo.cs:56 | MISSING (StreamInfo JS:5329) | none (stream_config.go:29-54) | nats.net-only | |
| `StreamState.Lost` | Models/StreamState.cs:83 | MISSING (StreamState JS:5304) | none (stream_config.go:250-290) | nats.net-only | |
| `StreamSourceInfo.External` / `.Error` | Models/StreamSourceInfo.cs:57,61 | MISSING (JS:5319) | none (stream_config.go:224-246) | nats.net-only | NQ has `Seq` and `SubjectTransforms`, which nats.net lacks (nats.net still uses the legacy `subject_transform_dest`, StreamSourceInfo.cs:35) |
| Snapshot / restore / leader stepdown / peer remove / templates / account purge / meta server remove | Models only: StreamSnapshotRequest.cs:55 (#1088 `window_size`), StreamRestore*, *LeaderStepdown*, StreamRemovePeer*, StreamTemplate*, AccountPurgeResponse, MetaServerRemove*. No public methods; reachable only through `JSRequestResponseAsync` | MISSING (grep `Restore`, `stepdown`, `RemovePeer`, `StreamTemplate`, `AccountPurge`: no match) | none in nats.go/jetstream | nats.net-only | #1088 is new since 8bb0fb7 |

### Stored message get

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| Get by seq / `next_by_subj` / `last_by_subj` | NatsJSStream.cs:200, Models/StreamMsgGetRequest.cs | PRESENT: `GetMsgAsync(seq)` JS:4726, `GetMsgAsync(seq, subject)` JS:4733 (next_by_subj), `GetLastMsgForSubjectAsync` JS:4741 | stream.go:48,52. `WithGetMsgSubject` jetstream_options.go:130 ("planned", bound as an overload) | implemented | |
| Direct get | NatsJSStream.cs:190 `GetDirectAsync<T>` | PRESENT, implicit: DIRECT.GET is chosen when `CachedInfo.Config.AllowDirect` (`js_msg_get_direct` JS:4751). You cannot force direct or non-direct, and there is no typed deserializer | same automatic choice as nats.go | oracle-backed | |
| Batch get / last-msgs-for (multi-subject) | none in nats.net | MISSING (grep `GetBatch`, `GetLastMsgsFor`: no match) | orbit `jetstreamext.GetBatch` getbatch.go:128, `GetLastMsgsFor` getbatch.go:191 | oracle-backed (orbit caps: 15 symbols planned) | not a nats.net gap; nats.net has no batch get either |
| Direct-get reply header constants (`Nats-Stream`, `Nats-Sequence`, `Nats-Time-Stamp`, `Nats-Subject`, `Nats-Last-Sequence`) | none public | MISSING as public constants (handled inside the IR) | message.go:270-292 (`StreamHeader`, `SequenceHeader`, `TimeStampHeaer`, `SubjectHeader`, `LastSequenceHeader`) | oracle-backed, planned | |

### Consumer management

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| Create / Update / CreateOrUpdate consumer (context) | NatsJSContext.Consumers.cs:29-47 | PRESENT JS:2336, 2342, 2348 | `StreamConsumerManager.*` | implemented | |
| Same on a stream object | INatsJSStream.cs:63 (only CreateOrUpdate) | PRESENT JS:4792-4800 (all three) | stream.go ConsumerManager | NQ ahead | |
| Get / Delete consumer | Consumers.cs:57,138 | PRESENT JS:2354, 2360, 4804, 4808 | implemented | | Delete returns void (nats.net returns bool) |
| Durable-only create (#1150) | Consumers.cs | PRESENT: follows nats.go (consumer.go:304, name = Durable) | consumer.go:304 | oracle-backed | #1150 is new since 8bb0fb7 |
| List consumers / names | Consumers.cs:68,99 (context, by stream) | PARTIAL: only on `NatsJSStream` (JS:4854, 4875). The context has neither | stream.go:114,118 (stream only) | context-level is nats.net-only | |
| Pause / resume | Consumers.cs:160,181 | PRESENT: `PauseConsumerAsync(…, NatsJSTime)` JS:2366, 4811 and `ResumeConsumerAsync` JS:2372, 4815 | jetstream.go:211,214; stream.go:107,110 | oracle-backed | No `DateTimeOffset` overload (use `NatsJSTime.FromDateTimeOffset`). Resume returns `ConsumerPauseResponse`, not bool. Caps lists `ConsumerManager.PauseConsumer`/`ResumeConsumer` as planned although they are bound on NatsJSStream: a stale record |
| Consumer reset (#1126 API, #1250 on stream objects) | Consumers.cs:202, NatsJSStream.cs:172 | PRESENT: `ResetConsumerAsync` / `ResetConsumerToSequenceAsync` JS:2378, 2384 (context) and JS:4819, 4823 (stream). `ConsumerResetResponse` JS:5151 | jetstream.go:218,224; stream.go:127,134 | oracle-backed | Caps lists `ConsumerManager.ResetConsumer*` as planned although bound: stale. **New since 8bb0fb7** |
| Consumer object Reset / Unpin / Delete / Refresh (#1250) | NatsJSConsumer.cs:46,370,377,385; INatsJSConsumer.cs:112-133; NatsJSPushConsumer.cs:55,232 | PARTIAL: `INatsJSConsumer.InfoAsync` (acts as Refresh) and `CachedInfo` JS:2554-2556. No `ResetAsync`, `UnpinAsync` or `DeleteAsync` on consumer objects (grep `UnpinAsync`, `DeleteAsync\(`: no match) | Go's Consumer interface (consumer.go:75-162) has none of them | nats.net-only | `ResetAsync` on objects is **new since 8bb0fb7** |
| Unpin (context level) | Consumers.cs:192 | PARTIAL: only `NatsJSStream.UnpinConsumerAsync` JS:4847 | stream.go:122 (stream only) | context-level is nats.net-only | |
| ConsumerConfig, field by field | Models/ConsumerConfig.cs | PRESENT: every nats.go field is in `ConsumerConfig` JS:5073-5108. MISSING: `direct` (ConsumerConfig.cs:183) and `sourcing` (ConsumerConfig.cs:190, #1249) | none: nats.go consumer_config.go:109-248 has neither | nats.net-only | `Sourcing` is **new since 8bb0fb7**. Name mapping: `DurableName`→`Durable`, `SampleFreq`→`SampleFrequency`, `RateLimitBps`→`RateLimit`, `MaxBatch`→`MaxRequestBatch`, `MaxExpires`→`MaxRequestExpires`, `MaxBytes`→`MaxRequestMaxBytes`, `NumReplicas`→`Replicas`, `MemStorage`→`MemoryStorage` |
| Priority groups: policy (pinned_client / overflow / prioritized), groups, PinnedTTL | ConsumerConfig.cs:275-304, ConsumerConfigPriorityPolicy.cs | PRESENT: `ConsumerConfig.PriorityPolicy`/`PinnedTtl`/`PriorityGroups` JS:5100-5102; `PriorityPolicy` enum JS:5065; `PriorityGroupState` JS:5117 | consumer_config.go:242-244 | oracle-backed | |
| Pin ID tracking (#1099, #1116) | Internal/NatsJSConsume.cs, NatsJSFetch.cs | PRESENT: `PullRequest.pinId` (JS:2985 region) and `NatsJSErrorKind.PinIDMismatch` | pull.go | oracle-backed | new since 8bb0fb7; present |

### Consumption

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| FetchAsync (MaxMsgs / MaxBytes / Expires / IdleHeartbeat) | NatsJSConsumer.cs:236; NatsJSOpts.cs `NatsJSFetchOpts` | PRESENT: `FetchAsync` JS:2528, `FetchBytesAsync` JS:2530, `FetchMaxWait` JS:1064, `FetchHeartbeat` JS:1066, `FetchContext` JS:1068-1074 | consumer.go:75,101 | implemented | nats.net can combine MaxMsgs and MaxBytes in one fetch; NQ follows nats.go (`FetchBytes` uses the default batch) |
| FetchNoWaitAsync | NatsJSConsumer.cs:341 | PRESENT JS:2532 | consumer.go:120 | implemented | batch only; nats.net also accepts MaxBytes with NoWait |
| NextAsync | NatsJSConsumer.cs:200 | PRESENT JS:2538 (takes `NatsJSFetchOpt`) | consumer.go:154 | implemented | |
| ConsumeAsync | NatsJSConsumer.cs:63 (`IAsyncEnumerable<INatsJSMsg<T>>`) | PRESENT, different shape: callback `ConsumeAsync(handler, …)` JS:2545 and iterator `MessagesAsync()` JS:2552 (`INatsJSMessagesContext.NextAsync` JS:3295). No `IAsyncEnumerable` over a consume | consumer.go:137,149 | oracle-backed | |
| Consume opts: MaxMsgs / MaxBytes / Expires / IdleHeartbeat / ThresholdMsgs / ThresholdBytes | NatsJSOpts.cs:103-186 | PRESENT: `PullMaxMessages` JS:1095, `PullMaxMessagesWithBytesLimit` JS:1097, `PullExpiry` JS:1100, `PullMaxBytes` JS:1102, `PullThresholdMessages`/`Bytes` JS:1104/1106, `PullHeartbeat` JS:1116 | jetstream_options.go | implemented | NQ also has `StopAfter` JS:1118, which nats.net lacks |
| Priority-group pull opts (Group / MinPending / MinAckPending / Priority) | NatsJSOpts.cs:303ff `NatsJSPriorityGroupOpts` | PRESENT: `FetchMinPending`/`FetchMinAckPending`/`FetchPrioritized`/`FetchPriorityGroup` JS:1056-1062 and `PullMinPending`/`PullMinAckPending`/`PullPrioritized`/`PullPriorityGroup` JS:1108-1114 | jetstream_options.go:478ff | implemented | |
| `MaxConsecutive503Errors` | NatsJSOpts.cs:173 | MISSING | none (pull.go has no 503 counter) | nats.net-only | |
| `DrainOnCancel` (#1177) | NatsJSOpts.cs:186 | PARTIAL: explicit `INatsJSConsumeContext.Drain()`/`Stop()` JS:3280-3284 and `INatsJSMessagesContext.Drain` JS:3299. No switch that drains on cancel | `ConsumeContext.Drain` pull.go:52 | the switch is nats.net-only | **new since 8bb0fb7** |
| Notification handler (`INatsJSNotification`: Timeout, NoResponders, LeadershipChange, MessageSizeExceedsMaxBytes, PinIdMismatch, Protocol) on Consume / Fetch / Next | INatsJSNotification.cs; NatsJSOpts.cs:160, 204, 238 | PARTIAL: `ConsumeErrHandler` JS:1123 (consume and push) receives these conditions as `NatsJSErrorKind` (NoHeartbeat, ConsumerLeadershipChanged, PinIDMismatch, MaxBytesExceeded, …). Fetch errors come through `NatsJSMessageBatch.Error()` JS:3272. No per-fetch or per-next handler, and no non-error notifications | `ConsumeErrHandler` | per-fetch notifications are nats.net-only | |
| Ordered (pull) consumer | NatsJSContext.Consumers.cs:18; NatsJSOrderedConsumer.cs; NatsJSOpts.cs:76 | PRESENT: `CreateOrderedConsumerAsync` JS:2395, 4827. `NatsJSOrderedConsumerConfig` JS:3869 has every nats.net option plus `Metadata` and `NamePrefix` | jetstream.go:199 | implemented | |
| Ordered **push** consumer (`CreateOrderedPushConsumerAsync`; teardown fixes #1188, #1191) | NatsJSContext.PushConsumers.cs:60; NatsJSOrderedPushConsumer.cs | MISSING (grep `CreateOrderedPushConsumer`: no match) | none in nats.go/jetstream | nats.net-only | the teardown fixes are **new since 8bb0fb7** |
| Push consumer public API (#1231) | NatsJSContext.PushConsumers.cs:9-40; NatsJSPushConsumer.cs:77; NatsJSPushConsumerOpts.cs | PRESENT: `CreatePushConsumerAsync` JS:2416, `CreateOrUpdatePushConsumerAsync` JS:2422, `UpdatePushConsumerAsync` JS:2428 (NQ ahead), `GetPushConsumerAsync` JS:2434, stream versions JS:4831-4843. `NatsJSPushConsumer.ConsumeAsync` JS:4494 with flow control, heartbeats and `ConsumeErrHandler`. PARTIAL: no `SubOpts` (subscription options), no `DeleteAsync`/`ResetAsync` on the push object, and consume is a callback | jetstream.go:237,249; consumer.go:171-177 | oracle-backed core; extras nats.net-only | **new since 8bb0fb7**. nats.net's push `Next`/`Fetch`/`Unpin` only throw |

### Message acknowledgement and metadata

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| AckAsync | NatsJSMsg.cs:182 | PRESENT JS:2950 | message.go | implemented | NQ acks take no CancellationToken (only `DoubleAckAsync` does) |
| Double ack | `AckOpts.DoubleAck` NatsJSMsg.cs:304, context default NatsJSOpts.cs:49 | PARTIAL: `DoubleAckAsync` JS:2952 covers +ACK only. nats.net can double-ack Nak, Progress and Term too | `Msg.DoubleAck` only | nats.net-only extension | |
| Nak / Nak with delay | NatsJSMsg.cs:185, NatsJSExtensions.cs:42 | PRESENT: `NakAsync` JS:2954, `NakWithDelayAsync` JS:2956 | `Msg.Nak`, `NakWithDelay` | implemented | |
| AckProgress | NatsJSMsg.cs:197 | PRESENT `InProgressAsync` JS:2958 | `InProgress` | implemented | |
| AckTerminate with reason (#1048, #1081) | NatsJSMsg.cs:200-230, NatsJSExtensions.cs:45 | PRESENT: `TermAsync` JS:2960, `TermWithReasonAsync` JS:2962 | `Term`, `TermWithReason` | implemented | |
| Tracking "already acked" | none | PRESENT (`ackd`, `NatsJSErrorKind.MsgAlreadyAckd`) JS:2924, 2968-2976 | `ErrMsgAlreadyAckd` | NQ ahead | |
| Metadata: v1 (9 tokens) and v2 (11-12 tokens, domain `_`, account hash; #1265 is a test only) | Internal/ReplyToDateTimeAndSeq.cs | PRESENT: `NatsJSMsg.Metadata()` JS:2929 via `Core.js_parse_metadata` (Core.cs:54697; accepts 9 or ≥11 tokens) | internal/parser/parse.go:57 | implemented | |
| `$JS.FC.` reply subjects in the metadata parser (#1127) | ReplyToDateTimeAndSeq.cs:37 | MISSING: `js_parse_metadata` requires token[1] == `ACK` (Core.cs:54721-54733, bytes `{65,67,75}`) | nats.go also rejects it: internal/parser/parse.go:88 requires `"ACK"` | nats.net-only (conflicts with the oracle) | **new since 8bb0fb7**. Adopting it would be a recorded divergence |

### Publish, batch, schedules, counters

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| PublishAsync + `NatsJSPubOpts` (MsgId, ExpectedLastMsgId, ExpectedStream, ExpectedLastSequence, ExpectedLastSubjectSequence(+Subject), RetryWaitBetweenAttempts, RetryAttempts) | NatsJSContext.cs:80; NatsJSOpts.cs:252ff | PRESENT: `PublishAsync` JS:2139, `PublishMsgAsync` (headers via NatsMsg) JS:2154. Options: `WithMsgID` JS:916, `WithExpectStream` JS:920, `WithExpectLastSequence` JS:922, `WithExpectLastSequencePerSubject` JS:924, `WithExpectLastSequenceForSubject(seq, subj)` JS:926, `WithExpectLastMsgID` JS:929, `WithRetryWait` JS:931, `WithRetryAttempts` JS:933 | jetstream_options.go:6xx | implemented | |
| Per-message TTL | none (grep `Nats-TTL`/`MsgTTL` in nats.net JS: no match) | PRESENT `WithMsgTTL` JS:918 | jetstream_options.go:622 | NQ ahead | |
| `TryPublishAsync` (non-throwing `NatsResult`) | NatsJSContext.cs:127 | MISSING | none | nats.net-only | |
| `PubAckResponse.EnsureSuccess` / `IsSuccess` / `NatsJSDuplicateMessageException` | NatsJSExtensions.cs:14-39; NatsJSException.cs:66 | PARTIAL: `PubAck.Duplicate` JS:5194 (as in nats.go). No helper that throws | none | nats.net-only | |
| PublishConcurrentAsync | NatsJSContext.cs:281; NatsJSPublishConcurrentFuture.cs | PRESENT: `PublishConcurrentAsync`/`PublishMsgConcurrentAsync` JS:2194/2198 returning `NatsJSPubAckFuture` (JS:1580, `Ok()`/`Err`/`Msg`), plus `PublishConcurrentPending`/`Complete`/`CleanupPublisherAsync` and async handlers | `Publisher.PublishAsync` | implemented | NQ has more (max pending, stall wait) |
| Generic `T` serializers (`INatsSerialize<T>`/`INatsDeserialize<T>`) | throughout | MISSING (byte[] only) | Go uses []byte | nats.net-only (design) | |
| Atomic batch publish | none (only flags: StreamConfig.cs:292 `allow_atomic`, :311 `allow_batched` #1120) | PRESENT: `NatsJSBatchPublisher` Batch:150, `PublishMsgBatchAsync`, `NatsJSFastPublisher` Batch:287, `NewBatchPublisher`/`NewFastPublisher` Batch:695/698 | orbit publishbatch.go:178,422; fastpublish.go:169 | NQ ahead | |
| Message schedules | flag only (StreamConfig.cs:284) | PRESENT: `WithScheduleAt/Every/Cron/Target/Source/TTL/TTLNever/TimeZone/Rollup` JS:937-955, plus the header constants | jetstream_options.go | NQ ahead | |
| Counters | flag only (`allow_msg_counter`, StreamConfig.cs:276) | flag only (`AllowMsgCounter` JS:5296). No counter API | orbit.go `counters/` (not in NQ's contracts) | neither has it | |

### Errors, docs, telemetry

| Feature | nats.net ref | NQ C# status + ref | Go oracle | class | notes |
|---|---|---|---|---|---|
| API exception with ApiError (Code / ErrCode / Description) | NatsJSException.cs:85; Models/ApiError.cs | PRESENT: `NatsJSException.ApiError` → `NatsJSApiError` (Code, ErrorCode, Description) JS:728-805, plus `Kind`, `Matches`, `Is` | errors.go:35 | oracle-backed. APIError symbols are planned (types never evidenced) | |
| Specialised exceptions (Protocol, Duplicate, PublishNoResponse, ApiNoResponse, Timeout, Connection) | NatsJSException.cs:34-134 | PRESENT, different shape: one `NatsJSException` with `NatsJSErrorKind` (JS:483), plus `NatsException` with `Core.ErrorCode.timeout`/`no_responders` | errors.go sentinels | oracle-backed | |
| Error code enum | none (raw int only) | PRESENT `NatsJSErrorCode` JS:5347 | errors.go `JSErrCode*` | NQ ahead | |
| `ErrEndOfData`, `ErrConsumerHasActiveSubscription` | none | MISSING from `NatsJSErrorKind` | errors.go:312, 320 | oracle-backed, planned | not a nats.net gap |
| `MigrationStatusType` constants (Meta, Membership, Snapshot, Catchup, Quorum, Blocked, Unavailable) | none | MISSING. `DesiredClusterInfoStatus.Type` is a plain string (JS:5015) | stream_config.go:548-576 | oracle-backed, planned | not a nats.net gap |
| Consumer-info usage warnings (#1079, a94b9eb, before 8bb0fb7) | INatsJSConsumer.cs and INatsJSContext.cs remarks | MISSING (docs only; grep `frequent`/`performance` in JetStream.cs: no match) | none | nats.net-only (docs) | |
| JetStream OpenTelemetry (activities; #1194 ack/dropped metrics; #1208 baggage; #1229, #1236 receive span) | NatsJSTelemetryExtensions.cs | MISSING (no `ActivitySource` or `Meter` in NQ) | none in nats.go/jetstream | nats.net-only | **new since 8bb0fb7** (strictly a core/OTel area) |

## 2. The "planned" capability symbols

### jetstream/csharp.async.json: 510 planned

Every one has the reason "Awaiting a C# binding and oracle conformance evidence". The JETSTREAM-PLAN (lines 873-874, 1084-1088) explains that struct types, fields, listers and interfaces stay planned in every target because the Go evidence gate cannot measure a type or field. So "planned" is mostly an **evidence** gap, not a **binding** gap. A name check against `JetStream*.cs` (script at `scratchpad/tools/classify.py`, output in `scratchpad/js-planned-classified.txt`) gives these groups:

| Group | Count | Bound in C#? |
|---|---|---|
| Stream records and fields (StreamConfig 37 fields, StreamInfo, StreamState, StreamSource(Info), SubjectTransformConfig, RePublish, ExternalStream, Placement, RawStreamMsg, StreamPurgeRequest, StreamConsumerLimits/Source) | 113 | Yes (JS:5157-5345, JS:802) |
| Consumer records and fields (ConsumerConfig 33, ConsumerInfo 16, SequenceInfo, SequencePair, PriorityGroupState, Pause/Reset responses, OrderedConsumerConfig 10) | 80 | Yes (JS:5073-5155, 2888, 3869) |
| KV / ObjectStore | 71 | out of this area (bound as `NatsKV*`/`NatsObj*`) |
| Account and cluster records (AccountInfo, AccountLimits, APIStats, Tier, ClusterInfo, PeerInfo, Desired*) | 62 | Yes (JS:4930-5039). `AccountInfo.Tier` (Go's embedded struct) is flattened into `AccountInfo` fields |
| `JSErrCode*` constants and the `ErrorCode` type | 28 | Yes, as the `NatsJSErrorCode` enum (JS:5347) |
| Enum constants (AckPolicy, DeliverPolicy, ReplayPolicy, RetentionPolicy, Discard*, Storage, Compression, PersistMode, PriorityPolicy*) | 27 | Yes, as C# enum members (JS:4999-5255) |
| Enum `MarshalJSON`/`UnmarshalJSON`/`String` | 26 | Not applicable (generated converters inside `NatsJS`) |
| Message and publish records/interfaces (PubAck, PubAckFuture, Msg, MsgMetadata, MessageBatch, MessageHandler, MsgAck/ErrHandler, Publisher) | 21 | Yes (`PubAck` JS:5191, `NatsJSMsg` 2919, `NatsJSMsgMetadata` 2895, delegates 1547/1553/2910) |
| Interfaces (JetStream, Stream, Consumer, PushConsumer, ConsumeContext, MessagesContext, `JetStream.Conn`/`.Options`, …) | 17 | Yes (`NatsJSContext`, `NatsJSStream`, `INatsJSConsumer`, …; `Connection` JS:2134, `Options` JS:2132) |
| Option types and JetStreamOptions fields (FetchOpt, PullConsumeOpt, PullMessagesOpt, PushConsumeOpt, PublishOpt, StreamInfoOpt, StreamListOpt, StreamPurgeOpt, GetMsgOpt, JetStreamOpt, ClientTrace) | 16 | Yes (`NatsJSFetchOpt`, `NatsJSPullOpt`, `NatsJSPublishOpt`, `StreamInfoOptions`, `PurgeOptions`, `NatsJSOpt`, `NatsJSOptions`, `NatsJSClientTrace`) |
| Lister and Manager interfaces and methods (StreamInfoLister, StreamNameLister, ConsumerInfoLister, ConsumerNameLister, StreamManager, ConsumerManager, StreamConsumerManager) | 15 | Listers are `IAsyncEnumerable` (Info/Name/Err → enumerate/throw). **`ConsumerManager.PauseConsumer`/`ResumeConsumer`/`ResetConsumer`/`ResetConsumerToSequence` are bound at JS:4811-4825 but still listed as planned (stale records)** |
| Error types and sentinels (APIError 7, JetStreamError 2, ErrEndOfData, ErrConsumerHasActiveSubscription) | 11 | APIError/JetStreamError are bound (`NatsJSApiError`, `NatsJSException`). **`ErrEndOfData` and `ErrConsumerHasActiveSubscription` are unbound** |
| Functional-option constructors (WithClientTrace, WithDefaultTimeout, WithDeletedDetails, WithSubjectFilter, WithStreamListSubject, WithPurgeSubject/Sequence/Keep, WithGetMsgSubject) | 9 | Bound idiomatically (`NatsJSOpts` props, `StreamInfoOptions`, list `subject` param, `PurgeOptions`, the `GetMsgAsync(seq, subject)` overload) |
| `MigrationStatus*` constants and type | 8 | **Unbound** |
| Other constants: `DefaultAPIPrefix` and the direct-get headers `StreamHeader`, `SequenceHeader`, `TimeStampHeaer`, `SubjectHeader`, `LastSequenceHeader` | 6 | **Unbound** |

Only about 16 of the 439 non-KV/Obj planned symbols have no C# surface at all:
- `ErrEndOfData`, `ErrConsumerHasActiveSubscription`
- 8 MigrationStatus symbols
- `DefaultAPIPrefix` and the 5 direct-get header constants

A further 4 are bound but their capability records are stale (the ConsumerManager pause/resume/reset methods).

### orbit/csharp.async.json: 40 planned

| Group | Count | Bound in C#? |
|---|---|---|
| Batch publish types and fields: `BatchAck` (+6 fields), `BatchFlowControl` (+3), `BatchMsgOpt`, `BatchPublisher`, `BatchPublisherOpt`, `PublishMsgBatchOpt` | 15 | Yes: `NatsJSBatchAck` Batch:56, `NatsJSBatchFlowControl` Batch:43, `NatsJSBatchMsgOpt` Batch:30, `NatsJSBatchPublisher` Batch:150. The opt-func types are folded into parameters |
| Fast publish types and fields: `FastPubAck` (+2), `FastPublishFlowControl` (+3), `FastPublishErrHandler`, `FastPublisher`, `FastPublisherOpt` | 10 | Yes: Batch:70, 98, 77, 287, 80 |
| **Batch get**: `GetBatch`, `GetLastMsgsFor`, `GetBatchSeq`/`Subject`/`MaxBytes`/`StartTime`, `GetLastMsgsUpToSeq`/`UpToTime`/`BatchSize`, the `GetBatchOpt`/`GetLastForOpt` types, and errors `ErrBatchUnsupported`/`ErrInvalidResponse`/`ErrNoMessages`/`ErrSubjectRequired` (getbatch.go:34-191) | 15 | **Unbound** (grep `GetBatch`/`GetLastMsgsFor`: no match). JETSTREAM-PLAN:2905 marks batch get as outside J8 |

## 3. Most significant gaps, in priority order

1. **Batch direct get (orbit `GetBatch`, `GetLastMsgsFor`), oracle-backed.** This is the only functional oracle-backed gap in this area: 15 planned orbit symbols, getbatch.go:128 and :191. nats.net lacks it too, so it is not needed for nats.net parity, but it is the next real API to build.
2. **Server fields nats.net decodes and NQ drops (nats.net-only, no Go field).**
   - `ConsumerConfig.Sourcing` (#1249, new) and `Direct`.
   - `missing`/`offline` on the stream and consumer LIST responses (#1248, new).
   - `StreamSourceInfo.External` and `.Error`, `StreamInfo.Alternates`, `StreamState.Lost`.

   With round-trip decoding, NQ silently drops these. Adding them needs either an upstream nats.go change or a recorded divergence; `Sourcing` matters most for server 2.14 consumer sourcing.
3. **Object-level convenience methods (nats.net-only).**
   - On consumer objects: `ResetAsync` (#1250, new), `UnpinAsync`, `DeleteAsync`.
   - On `NatsJSStream`: `DeleteAsync`, `UpdateAsync`.
   - On the context: `ListConsumersAsync`/`ListConsumerNamesAsync`/`UnpinConsumerAsync`/`PurgeStreamAsync`/`DeleteMessageAsync`.
   - The purged count from purge.

   These are thin wrappers, but nats.net users porting code will hit them first.
4. **Ordered push consumer (nats.net-only).** `CreateOrderedPushConsumerAsync`, with teardown fixes #1188 and #1191 since 8bb0fb7. nats.go has no equivalent.
5. **Consume ergonomics (nats.net-only).**
   - No `IAsyncEnumerable` consume.
   - No per-fetch/next `NotificationHandler` (non-error notifications).
   - No `MaxConsecutive503Errors`.
   - No `DrainOnCancel` (#1177, new) or `ThrowOnListCancellation` (#1214, new) switches. NQ effectively throws on list cancel, except while the last page is being yielded.
   - No context-wide `DoubleAck` or `DefaultConsumeOpts`, and no CancellationToken on the ack calls.
6. **Publish ergonomics (nats.net-only).** `TryPublishAsync`, `EnsureSuccess` and `NatsJSDuplicateMessageException`, and typed serializers.
7. **Raw `JSRequestResponseAsync` and the admin models (nats.net-only).** Snapshot (#1088 `window_size`, new), restore, stepdown, peer remove, templates, account purge.
8. **`$JS.FC` metadata parsing (#1127, new, nats.net-only).** This contradicts the oracle (nats.go parse.go:88 rejects it), so it should only be adopted as a divergence recorded in the deferred docs.
9. **Small oracle-backed constants and sentinels.** `ErrEndOfData`, `ErrConsumerHasActiveSubscription`, the direct-get header constants, `MigrationStatusType`, `DefaultAPIPrefix`.
10. **Capability hygiene.** Flip `jetstream.ConsumerManager.{PauseConsumer, ResumeConsumer, ResetConsumer, ResetConsumerToSequence}` to implemented: they are bound at `NatsJSStream` JS:4811-4825.
11. **Telemetry and docs (nats.net-only).** JetStream OTel (#1194, #1208, #1229, #1236, new) and the consumer-info usage warnings (#1079).

### Where NQ C# goes beyond nats.net in this area
- `StreamNameBySubjectAsync`, stream-level Create/Update consumer and Pause/Resume, `UpdatePushConsumerAsync`.
- `StreamConfig.FirstSeq`/`ConsumerLimits`, `StreamSourceInfo.Seq`/`SubjectTransforms`, `StreamSource.Domain`.
- Per-message TTL, schedules, atomic and fast batch publish (orbit), and the async-publisher controls (max pending, stall wait, handlers).
- `StopAfter`, `ClientTrace`, `NatsJSErrorCode`, and "already acked" tracking.
