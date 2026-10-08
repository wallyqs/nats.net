# Feature-gap audit: nats.net KV / ObjectStore / Services vs NQ C#

Scope: nats.net `src/NATS.Client.{KeyValueStore,ObjectStore,Services}` (upstream main v3.3.0) compared with the NQ C# client
(`/home/user/nq.dev/packages/csharp/src/NQ/{JetStreamKv.cs,JetStream.cs,NatsSvc.cs}`). The oracle is nats.go at pinned
`7a8404ab` (v1.54.0): `jetstream/kv.go`, `kv_options.go`, `object.go`, `object_options.go`, `micro/`.

Paths are shortened: `net:` = `/home/user/nats.net/src/NATS.Client.*`, `nq:` = `/home/user/nq.dev/packages/csharp/src/NQ/`,
`go:` = `/home/user/oracle/nats.go/`. Capability records: `ir/capabilities/jetstream/csharp.async.json` (jetstream) and
`ir/capabilities/services/csharp.async.json` (micro).

Labels: **PRESENT** (NQ has it), **PARTIAL** (part is missing), **MISSING**. **oracle-backed** means nats.go has an equivalent
public API. **nats.net-only** means nats.go has no public equivalent. `[new]` marks gaps that come from nats.net commits in
`8bb0fb7..HEAD` (f635660 #1084, 14374ff #1149, 7d64ac7 #1152, f9576b1 #1199, d47c5d9 #1235, 0033cfd #1236, 7635056 #1252,
13313a8 #1240, 18ce034 #1255; 03c0240 #1103 only changes csproj/README).

How NQ's shape differs overall. NQ C# follows nats.go's API (methods on `NatsJSContext`, `byte[]` values, channel-based
watchers and listers, `params` option objects). nats.net instead has a separate `INatsKVContext` / `INatsObjContext` /
`INatsSvcContext` created by `CreateKeyValueStoreContext()` and friends (`net:KeyValueStore/NatsClientExtensions.cs:15-58`),
generic `T` with serializers, `IAsyncEnumerable` results and `Try*`/`NatsResult` variants. Where the only difference is
shape, the item is marked PRESENT and the difference goes in notes.

---

## 1. KeyValueStore

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | class | notes |
|---|---|---|---|---|---|
| KV context object (`INatsKVContext`, `CreateKeyValueStoreContext`) | `INatsKVContext.cs:5`, `NatsClientExtensions.cs:15-58` | PRESENT (shape): the methods are on `NatsJSContext` (`nq:JetStreamKv.cs:945-1076`) | `KeyValueManager` `jetstream/kv.go:35` (cap: **planned**, type) | oracle-backed | no separate context type; NQ follows the nats.go manager-on-JetStream shape |
| `NatsKVOpts.UseDirectGetApiWithKeysInSubject` | `NatsKVContext.cs:302` | PARTIAL: NQ always uses `DIRECT.GET.<stream>.<subject>` for a last-by-subject get (`ir/jetstream-api.nqir:627`) and has no toggle | nats.go always does this (`jetstream/stream.go:577-583`), no option | nats.net-only (toggle) | NQ behaves as nats.go does, i.e. as nats.net with the option on |
| `NatsKVOpts.WatcherThrowOnCancellation` (#1084) | `NatsKVContext.cs:308`, `NatsKVStore.cs:449-498` | MISSING (not applicable): NQ watchers are channels stopped with `StopAsync`, and there is no IAsyncEnumerable cancellation path | none | nats.net-only `[new]` | f635660 |
| CreateStore(config) | `NatsKVContext.cs:57` | PRESENT `NatsJSContext.CreateKeyValueAsync` `nq:JetStreamKv.cs:962` (includes nats.go's "exists with the same config, so update" path) | `CreateKeyValue` `kv.go:47`/`534` (cap: implemented) | — | |
| CreateStore(string bucket) overload | `NatsKVContext.cs:53` | PARTIAL: only a config form | none (config only) | nats.net-only | trivial convenience |
| UpdateStore | `NatsKVContext.cs:99` | PRESENT `UpdateKeyValueAsync` `nq:JetStreamKv.cs:999` | `kv.go:54`/`575` (impl) | — | |
| CreateOrUpdateStore | `NatsKVContext.cs:111` | PRESENT `CreateOrUpdateKeyValueAsync` `nq:JetStreamKv.cs:1011` | `kv.go:59`/`596` (impl) | — | |
| GetStore (+ MaxMsgsPerSubject check) | `NatsKVContext.cs:83-96` | PRESENT `GetKeyValueAsync` `nq:JetStreamKv.cs:947` (`js_kv_bucket_check`) | `kv.go:40`/`509` (impl) | — | NQ also maps mirror buckets (`js_kv_map_handle`, `nq:JetStreamKv.cs:932`), where nats.net has `// TODO: KV mirror` (`NatsKVContext.cs:94`). NQ covers more here |
| DeleteStore | `NatsKVContext.cs:123` | PRESENT `DeleteKeyValueAsync` `nq:JetStreamKv.cs:1015` | `kv.go:65`/`734` (impl) | — | nats.net returns `bool`; NQ returns void, as nats.go does |
| Bucket names | `GetBucketNamesAsync` `NatsKVContext.cs:130` | PRESENT `KeyValueStoreNames` (channel lister) `nq:JetStreamKv.cs:1025` | `kv.go:72`/`749` (impl) | — | |
| List bucket statuses | `GetStatusesAsync` `NatsKVContext.cs:144` | PRESENT `KeyValueStores` `nq:JetStreamKv.cs:1047` | `kv.go:79`/`780` (impl) | — | nats.net does not filter out non-`KV_` streams and reports the stream name as `Bucket` (`NatsKVContext.cs:146-151`). NQ filters via `js_kv_list`, as nats.go does |
| Bucket-name validation | `NatsKVContext.cs:24,155` | PRESENT `KvNames` → `js_kv_bucket` `nq:JetStreamKv.cs:888` | `bucketValid` `kv.go:904` | — | |
| **NatsKVConfig: Bucket, Description, MaxValueSize, History (max 64), MaxAge/TTL, MaxBytes, Storage, Replicas, Placement, Republish, Compression, Mirror, Sources, Metadata, LimitMarkerTTL** | `NatsKVConfig.cs:19-90` | PRESENT, all fields in `NatsKVConfig` `nq:JetStreamKv.cs:53-68` (`TTL`=MaxAge, `RePublish`, `Replicas`, `History` is `byte`); derived via `js_kv_stream_config` `ir/jetstream-kv.nqir:173` | `KeyValueConfig` `kv.go:213-277` (cap: type + 15 fields **planned**) | — | the fields exist and are used in the prepare path; the capability records lag (see §4) |
| Mirror/Sources mapping (KV_ prefix, `$KV.<src>.>` → `$KV.<dst>.>` transform) | `NatsKVContext.cs:210-256` | PRESENT `js_kv_map_source` `nq:JetStreamKv.cs:1124-1138` | `kv.go:697-725` | — | NQ keeps a source that already has its own transforms unchanged (nats.go `kv.go:704`); nats.net always overwrites the transforms |
| duplicate_window capped at MaxAge (#1149) | `NatsKVContext.cs:198-203` | PRESENT `ir/jetstream-kv.nqir:191-194` (`%window`) | `kv.go:647-654` | — | 14374ff: covered |
| LimitMarkerTTL / per-key TTL API-level check | `NatsKVContext.cs:61-73` | PRESENT (`check_api_level` in `js_kv_stream_settings`, `ir/jetstream-kv.nqir:171`) | `kv.go:660-670` | — | nats.net also refuses 0 < TTL < 1s on the client; nats.go does not (nats.net-only check) |
| Put | `NatsKVStore.cs:101` | PRESENT `PutAsync(byte[])`, `PutStringAsync` `nq:JetStreamKv.cs:258,264` | `kv.go:108,116` (impl) | — | |
| Create (fails if the key exists; replaces a deleted marker) | `NatsKVStore.cs:146-200` | PRESENT `CreateAsync` `nq:JetStreamKv.cs:271-290` | `kv.go:123`/`1062` (impl) | — | |
| Create with TTL | `NatsKVStore.cs:146` (`TimeSpan ttl`) | PRESENT `CreateAsync(..., NatsJS.KeyTTL(ttl))` `nq:JetStreamKv.cs:785` | `KeyTTL` `kv_options.go:126` (impl) | — | nats.net refuses on the client when the store has no TTL support (`NatsKVStore.cs:164`); NQ leaves that to the server, as nats.go does |
| Update (CAS on revision) | `NatsKVStore.cs:202` | PRESENT `UpdateAsync` `nq:JetStreamKv.cs:292` | `kv.go:129`/`1117` (impl) | — | |
| Delete with expected revision | `NatsKVDeleteOpts.Revision` `NatsKVOpts.cs:85` | PRESENT `DeleteAsync(key, NatsJS.LastRevision(rev))` `nq:JetStreamKv.cs:309,781` | `LastRevision` `kv_options.go:98` (impl) | — | |
| Purge (+ revision) | `NatsKVStore.cs:232` | PRESENT `PurgeAsync` `nq:JetStreamKv.cs:313` | `kv.go:149` (impl) | — | |
| Purge with TTL | `NatsKVStore.cs:236` | PRESENT `PurgeAsync(key, NatsJS.PurgeTTL(ttl))` `nq:JetStreamKv.cs:783` | `PurgeTTL` `kv_options.go:110` (impl) | — | |
| `Try*` variants returning `NatsResult` (TryPut/TryCreate/TryUpdate/TryDelete/TryPurge/TryGetEntry) | `INatsKVStore.cs:45-212` | MISSING | none (Go returns errors) | nats.net-only | API style difference |
| Get latest (GetEntryAsync, revision=0) | `NatsKVStore.cs:254-290` | PRESENT `GetAsync` `nq:JetStreamKv.cs:252` | `Get` `kv.go:95`/`1007` (impl) | — | a deleted marker gives KeyNotFound in NQ (as nats.go, `kv.go:1010`); nats.net throws `NatsKVKeyDeletedException(revision)` |
| Get by revision | `GetEntryAsync(key, revision)` `NatsKVStore.cs:283-289` | PRESENT `GetRevisionAsync` `nq:JetStreamKv.cs:255` | `GetRevision` `kv.go:100`/`1020` (impl) | — | nats.net sends `Seq`+`NextBySubj`; NQ/nats.go send `GetMsg(seq)` and then check the subject (`kv.go:938-942`). A gap in a key's revisions gives different results |
| Get on a bucket with AllowDirect disabled (#1240) | `NatsKVStore.cs:356-441` (non-direct path decodes the base64 headers and classifies DEL/PURGE/marker-reason) | PRESENT: `NatsJSStream.FetchMsgAsync` takes MSG.GET when `!AllowDirect` (`nq:JetStream.cs:4748-4772`) and decodes the headers into `RawStreamMsg.Header`; `EntryAsync` runs `js_kv_entry_op` on them (`nq:JetStreamKv.cs:225-243`) | `kv.go:925-990` via `stream.getMsg` `stream.go:564-600` | — `[new]` (13313a8 fixed in nats.net; NQ already correct) | NQ maps the non-direct "no message found" to KeyNotFound (`KvError(get)`), as nats.go does; nats.net leaves it as `NatsJSApiException` (`NatsKVStore.cs:357-361`). No e2e test runs a KV get against an AllowDirect=false bucket (no match in `e2e/go/jetstream_kv_test.go`); only the stream-level choice is tested (`e2e/go/jetstream_message_test.go:227-241`) |
| Typed values / serializers (`INatsSerialize<T>`, `NatsKVEntry<T>.Value`, `.Error`, `EnsureSuccess`) | `INatsKVStore.cs:31`, `NatsKVEntry.cs:11-54` | PARTIAL: `byte[]` plus `PutStringAsync` only (`nq:JetStreamKv.cs:103-121`) | `[]byte` only | nats.net-only | |
| Watch (single key / pattern) | `NatsKVStore.cs:445` | PRESENT `WatchAsync(string)` `nq:JetStreamKv.cs:341` | `kv.go:167`/`1392` (impl) | — | NQ returns `NatsKVWatcher` (a channel plus a null marker at the end of the initial values); nats.net returns `IAsyncEnumerable` |
| Watch all | `NatsKVStore.cs:565` | PRESENT `WatchAllAsync` `nq:JetStreamKv.cs:344` | `kv.go:171` (impl) | — | |
| Watch multiple keys (FilterSubjects) | `NatsKVStore.cs:449`, `Internal/NatsKVWatcher.cs:455-466` | PRESENT `WatchFilteredAsync(IList<string>)` `nq:JetStreamKv.cs:350` → `js_kv_watch_consumer` (`filter_subjects` when there is more than one) | `WatchFiltered` `kv.go:175`/`1244` (impl) | — | |
| Watch opts IncludeHistory / UpdatesOnly / IgnoreDeletes / MetaOnly / ResumeAtRevision | `NatsKVOpts.cs:23-60` | PRESENT `NatsJS.IncludeHistory/UpdatesOnly/IgnoreDeletes/MetaOnly/ResumeFromRevision` `nq:JetStreamKv.cs:770-778` | `kv_options.go:29,41,52,61,70` (impl) | — | exclusivity is checked in `js_kv_watch_option`; nats.net checks it in `ThrowIfInvalid` `NatsKVOpts.cs:62` |
| Watch `IdleHeartbeat` option | `NatsKVOpts.cs:18` | PARTIAL: heartbeat, gap reset and activity check exist inside `LegacyOrdered` (`nq:JetStreamKv.cs:554-700`) but the interval is not configurable | not configurable in nats.go | nats.net-only | |
| Watch `OnNoData` callback | `NatsKVOpts.cs:51` | PARTIAL: NQ sends a null marker when there are no initial values (`js_kv_watch_start`, `nq:JetStreamKv.cs:365-367`); there is no callback | null marker in `kv.go:1244+` | nats.net-only (callback) | |
| KV watcher OTel receive-span lifecycle (#1236) | `Internal/NatsKVWatchSub.cs:64`, `Internal/NatsKVWatcher.cs:353-358` | MISSING: no `ActivitySource` or OpenTelemetry anywhere in `nq:` (grep `ActivitySource\|OpenTelemetry` finds nothing) | none | nats.net-only `[new]` | 0033cfd |
| History | `NatsKVStore.cs:503` | PRESENT `HistoryAsync` (returns `List`) `nq:JetStreamKv.cs:401` | `kv.go:195`/`1499` (impl) | — | for a missing key NQ throws KeyNotFound (as nats.go); nats.net yields empty (`NatsKVStore.cs:511`) |
| Keys / GetKeysAsync | `NatsKVStore.cs:641` | PRESENT `KeysAsync` (sorted list, NoKeysFound when empty) and `ListKeysAsync` (streaming) `nq:JetStreamKv.cs:374,394` | `Keys` `kv.go:180`/`1402`, `ListKeys` `kv.go:186`/`1432` (impl) | — | nats.net yields empty and has no NoKeysFound error |
| Keys with filters | `GetKeysAsync(IEnumerable<string> filters)` `NatsKVStore.cs:645` | PRESENT `ListKeysFilteredAsync(params string[])` `nq:JetStreamKv.cs:397` | `ListKeysFiltered` `kv.go:191`/`1461` (impl) | — | |
| PurgeDeletes (DeleteMarkersThreshold, default 30m, Keep=1 for recent markers) | `NatsKVStore.cs:569-638`, `NatsKVOpts.cs:88-96` | PRESENT `PurgeDeletesAsync(NatsJS.DeleteMarkersOlderThan(age))` `nq:JetStreamKv.cs:419`, `js_kv_purge_keep` | `kv.go:206`/`1528`, `DeleteMarkersOlderThan` `kv_options.go:83` (impl) | — | threshold meaning differs: in NQ/nats.go 0 means the 30m default and a negative value removes every marker; in nats.net an explicit `TimeSpan.Zero` removes every marker |
| PurgeDeletes `RetainRecentlyDeletedKeyHistory` (#1252) | `NatsKVOpts.cs:103`, `NatsKVStore.cs:619-620` | MISSING: `NatsKVPurgeOpt` only carries the threshold (`nq:JetStreamKv.cs:97-99`) | none: nats.go only has `DeleteMarkersOlderThan` (`kv_options.go:77-90`) | nats.net-only `[new]` | 7635056 |
| Status | `GetStatusAsync` `NatsKVStore.cs:555`; `NatsKVStatus(Bucket, IsCompressed, LimitMarkerTTL, Info)` `:889` | PRESENT `StatusAsync` → `NatsKVStatus` (Bucket, Values, History, TTL, BackingStore, StreamInfo, Bytes, IsCompressed, LimitMarkerTTL, Metadata, Config) `nq:JetStreamKv.cs:125-170,337` | `kv.go:209`/`1584`, `KeyValueBucketStatus` `kv.go:811-870` (methods impl; type **planned**) | — | NQ has more status fields than nats.net |
| Key validation | `NatsKVStore.IsValidKey` (public static) `NatsKVStore.cs:98,~760` | PARTIAL: validation runs (`CheckKey` → `js_kv_key`, `nq:JetStreamKv.cs:897`) but there is no public validator | `keyValid` / `searchKeyValid` are private (`kv.go:911,918`) | nats.net-only (public helper) | NQ/nats.go also reject `..` (`kv.go:912`); nats.net does not |
| Typed KV exceptions (KeyDeleted with Revision, WrongLastRevision, Create, KeyNotFound) | `NatsKVException.cs:31-61` | PRESENT (shape): `NatsJSException` with kinds `KeyDeleted`, `KeyRevisionMismatch`, `KeyExists`, `KeyNotFound` (errors.go caps impl) | `ErrKey*` `jetstream/errors.go:393-432` (impl) | — | NQ's KeyDeleted has no revision field (nats.go's has none either) |
| `NatsKVDefaults.MaxHistory` | `NatsKVDefaults.cs:5` | PRESENT `NatsJS.KeyValueMaxHistory` `nq:JetStreamKv.cs:760` | `kv.go:494` (impl) | — | |

## 2. ObjectStore

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | class | notes |
|---|---|---|---|---|---|
| Obj context (`INatsObjContext`, `CreateObjectStoreContext`) | `INatsObjContext.cs`, `ObjectStore/NatsClientExtensions.cs:15-31` | PRESENT (shape): methods on `NatsJSContext` `nq:JetStreamKv.cs:1968+` | `ObjectStoreManager` `object.go:40` (cap: **planned**, type) | oracle-backed | |
| CreateObjectStore(config) | `INatsObjContext.cs:29`, `NatsObjContext.cs:29` | PRESENT `CreateObjectStoreAsync` `nq:JetStreamKv.cs:1980` | `object.go:53` (impl) | — | |
| CreateObjectStore(string) | `INatsObjContext.cs:21` | PARTIAL: config form only | none | nats.net-only | trivial |
| GetObjectStore | `INatsObjContext.cs:37` | PRESENT `GetObjectStoreAsync` `nq:JetStreamKv.cs:1970` | `object.go:46` (impl) | — | |
| DeleteObjectStore | `INatsObjContext.cs:45` | PRESENT `DeleteObjectStoreAsync` `nq:JetStreamKv.cs:2004` | `object.go:71` (impl) | — | nats.net names it `DeleteObjectStore` (no Async) and returns `bool` |
| Update / CreateOrUpdate / list store names / list stores | not in nats.net (`INatsObjContext.cs` has only Create/Get/Delete) | PRESENT `UpdateObjectStoreAsync`, `CreateOrUpdateObjectStoreAsync`, `ObjectStoreNames`, `ObjectStores` `nq:JetStreamKv.cs:1990-2040` | `object.go:60,65,80,89` (impl) | — | NQ has these and nats.net lacks them |
| **NatsObjConfig: Bucket, Description, MaxAge, MaxBytes, Storage, NumberOfReplicas, Placement, Metadata, Compression** | `NatsObjConfig.cs:18-58` | PRESENT `NatsObjConfig` (Bucket, Description, TTL, MaxBytes, Storage, Replicas, Placement, Compression, Metadata) `nq:JetStreamKv.cs:1142-1151` | `ObjectStoreConfig` `object.go:256-292` (cap: type + 9 fields **planned**) | — | |
| Put from Stream (with meta) | `INatsObjStore.cs:71` | PRESENT `PutAsync(NatsObjMeta, Stream)` `nq:JetStreamKv.cs:1421` | `Put` `object.go:111` (impl) | — | nats.net's `leaveOpen` flag has no NQ counterpart; NQ never disposes the caller's stream |
| Put(key, Stream) | `INatsObjStore.cs:59` | PARTIAL: wrap with `new NatsObjMeta{Name=key}` | none (meta only) | nats.net-only | trivial |
| Put bytes | `INatsObjStore.cs:47` | PRESENT `PutBytesAsync` (also `PutStringAsync`, `PutFileAsync`) `nq:JetStreamKv.cs:1529-1536` | `object.go:118,125,132` (impl) | — | PutString and PutFile exist only in NQ |
| Chunk size (`MetaDataOptions.MaxChunkSize`, default 128 KiB) | `Models/ObjectMetadata.cs:108`, `NatsObjStore.cs:24,180-190` | PRESENT `ObjectMetaOptions.ChunkSize` `nq:JetStream.cs:5169`; default via `js_obj_put_check` (`nq:Core.cs:58992`) | `ObjectMetaOptions.ChunkSize` `object.go:362` (**planned** field) | — | |
| Meta: Description, Headers, Metadata | `Models/ObjectMetadata.cs:23-91` | PRESENT `NatsObjMeta` `nq:JetStreamKv.cs:1155-1160`, `ObjectInfo` `nq:JetStream.cs:5175-5188` | `ObjectMeta` `object.go:366-381` (**planned**) | — | |
| Get to a caller-supplied Stream (`GetAsync(key, Stream, leaveOpen)`) | `INatsObjStore.cs:38`, `NatsObjStore.cs:56` | PARTIAL: NQ `GetAsync` returns a readable `NatsObjResult : Stream` (`nq:JetStreamKv.cs:1242,1549`), so the caller does `CopyToAsync`; `GetFileAsync` writes to a path | `Get` returns `ObjectResult` (an io.ReadCloser) `object.go:143` (impl) | nats.net-only (overload) | NQ also bounds the whole download by the context timeout, as nats.go does |
| Get bytes | `INatsObjStore.cs:27` | PRESENT `GetBytesAsync` (also `GetStringAsync`, `GetFileAsync`) `nq:JetStreamKv.cs:1624-1650` | `object.go:152,161,170` (impl) | — | |
| Digest verification on Get | `NatsObjStore.cs:127-135` | PRESENT `js_obj_digest_check` at EOF in `NatsObjResult.ReadAsync` `nq:JetStreamKv.cs:1284-1295` → `DigestMismatch` | `object.go:1497` `ErrDigestMismatch` | — | nats.net also checks chunk count and size mismatch (`NatsObjStore.cs:138-145`); nats.go does not (nats.net-only) |
| Link resolution on Get (same and other bucket) | `NatsObjStore.cs:62-66` | PRESENT `js_obj_plan_get` same_bucket_link / other_bucket_link `nq:JetStreamKv.cs:1560-1563` | `object.go:143` (impl) | — | NQ refuses Get on a bucket link (`ErrCantGetBucket`), as nats.go does |
| GetInfo (showDeleted) | `INatsObjStore.cs:126` | PRESENT `GetInfoAsync(obj, NatsJS.GetObjectInfoShowDeleted())` `nq:JetStreamKv.cs:1654-1672` | `object.go:179`, `object_options.go:27` (impl) | — | |
| UpdateMeta (rename checks, purge old meta) | `INatsObjStore.cs:81`, `NatsObjStore.cs:307` | PRESENT `UpdateMetaAsync` `nq:JetStreamKv.cs:1676` | `object.go:186` (impl) | — | nats.net returns `ObjectMetadata`; NQ returns void, as nats.go does |
| Delete (marker + chunk purge) | `INatsObjStore.cs:157`, `NatsObjStore.cs:587` | PRESENT `DeleteAsync` `nq:JetStreamKv.cs:1696` | `object.go:194` (impl) | — | |
| AddLink(link, ObjectMetadata target) | `INatsObjStore.cs:99` | PRESENT `AddLinkAsync(obj, ObjectInfo)` `nq:JetStreamKv.cs:1726` | `object.go:207` (impl) | — | |
| AddLink(link, string target) | `INatsObjStore.cs:90`, `NatsObjStore.cs:344` | PARTIAL: build the `ObjectInfo` yourself (or `GetInfoAsync` first) | none (takes *ObjectInfo) | nats.net-only | |
| AddBucketLink | `INatsObjStore.cs:109` | PRESENT `AddBucketLinkAsync` `nq:JetStreamKv.cs:1747` | `object.go:217` (impl) | — | |
| Seal | `INatsObjStore.cs:116` | PRESENT `SealAsync` `nq:JetStreamKv.cs:1782` | `object.go:220` (impl) | — | |
| List (ShowDeleted) | `INatsObjStore.cs:134`, `NatsObjListOpts` `NatsObjStore.cs:669` | PRESENT `ListAsync(NatsJS.ListObjectsShowDeleted())` `nq:JetStreamKv.cs:1836` | `object.go:238`, `object_options.go:36` (impl) | — | NQ returns a `List` and throws NoObjectsFound when empty; nats.net yields empty; `NatsObjListOpts.OnNoData` is nats.net-only |
| Watch (IgnoreDeletes, IncludeHistory, UpdatesOnly) | `INatsObjStore.cs:149`, `NatsObjStore.cs:523-585,650-666` | PRESENT `WatchAsync(params NatsKVWatchOpt[])` `nq:JetStreamKv.cs:1793` | `object.go:233` (impl) | — | |
| Watch `InitialSetOnly`, `OnNoData` | `NatsObjStore.cs:661,666` | PARTIAL: NQ sends a null marker at the end of the initial values (`js_obj_watch_message`); there is no auto-stop flag or callback | null marker in nats.go | nats.net-only | |
| Status | `INatsObjStore.cs:141`, `NatsObjStatus(Bucket, IsCompressed, Info)` `NatsObjStore.cs:679` | PRESENT `StatusAsync` → `NatsObjStatus` (more fields: Description, TTL, Storage, Replicas, Sealed, Size, BackingStore, Metadata, StreamInfo, IsCompressed) `nq:JetStreamKv.cs:1205-1232,1858` | `object.go:241`, `ObjectBucketStatus` `object.go:1392-1428` (methods impl; type **planned**) | — | |
| base64url meta-subject / digest encoding (#1199) | `Internal/Encoder.cs`, `NatsObjStore.cs:626` | PRESENT `js_base64url_padded` in `js_obj_meta_subject` (`nq:Core.cs:58952-58954`), `js_obj_digest_text` | `object.go` (padded `base64.URLEncoding`) | — `[new]` (f9576b1: perf/refactor only, padded output unchanged; nothing for NQ to do) | |
| Inverted TryGetArray check in Put, netstandard2.0 (#1235) | `NatsObjStore.cs` (netstandard branch) | N/A: NQ has a single read loop into a reused `byte[]` chunk (`nq:JetStreamKv.cs:1465-1476`) and no TFM-specific path | n/a | — `[new]` (d47c5d9: nats.net-internal perf bug; no gap) | |
| Object name validation | `NatsObjStore.cs:640-647` | PRESENT `js_obj_put_check` / `js_obj_link_check` (`nq:JetStreamKv.cs:1426,1658`) | `object.go` (ErrBadObjectMeta, ErrNameRequired) | — | |

## 3. Services

| Feature | nats.net ref | NQ C# status + ref | nats.go oracle | class | notes |
|---|---|---|---|---|---|
| Services context (`INatsSvcContext`, `CreateServicesContext`) | `INatsSvcContext.cs:8`, `Services/NatsClientExtensions.cs:14-22` | PRESENT (shape): static `NatsSvc.AddServiceAsync(nc, config)` `nq:NatsSvc.cs:69` | `micro.AddService` `micro/service.go:325` (impl) | — | |
| AddServiceAsync(name, version, queueGroup) overload | `INatsSvcContext.cs:23`, `NatsSvcContext.cs:27` | PARTIAL: config form only | none | nats.net-only | trivial |
| NatsSvcConfig: Name, Version, Description, Metadata, QueueGroup | `NatsSvcConfig.cs:39-59` | PRESENT `NatsSvcConfig` `nq:NatsSvc.cs:516-529` | `Config` `service.go:180-201` (impl) | — | NQ also has `DoneHandler`, `ErrorHandler` and `Endpoint` (from nats.go) |
| Disable queue group (service level) | `UseQueueGroup=false` `NatsSvcConfig.cs:65` | PRESENT `NatsSvcConfig.QueueGroupDisabled` `nq:NatsSvc.cs:523` | `Config.QueueGroupDisabled` `service.go:191` (impl) | — | |
| Endpoint queue group / disable it | `AddEndpointAsync(queueGroup:)` `INatsSvcServer.cs:33` | PRESENT `WithEndpointQueueGroup` / `WithEndpointQueueGroupDisabled` `nq:NatsSvc.cs:143,146` | `service.go:984,991` (impl) | — | nats.net cannot disable the queue group for one endpoint (NQ can) |
| Stats handler | `NatsSvcConfig.StatsHandler` `NatsSvcConfig.cs:71` | PRESENT `NatsSvcConfig.StatsHandler` (`NatsSvcStatsHandler`) `nq:NatsSvc.cs:510,525` | `Config.StatsHandler` `service.go:195` (impl) | — | |
| AddEndpoint (name, subject, metadata) | `INatsSvcServer.cs:33`, `NatsSvcServer.cs:109,251-268` | PRESENT `AddEndpointAsync(name, handler, WithEndpointSubject/WithEndpointMetadata[Key])` `nq:NatsSvc.cs:615,624,131-137` | `AddEndpoint` `service.go:403`, `service.go:957-974` (impl) | — | nats.net accepts a subject without a name and derives the name by replacing `.` with `-` (`NatsSvcServer.cs:255-256`); NQ requires a name (nats.go) |
| Typed endpoint payloads (`AddEndpointAsync<T>` + `INatsDeserialize<T>`, `NatsSvcMsg<T>.Exception`) | `INatsSvcServer.cs:33`, `NatsSvcMsg.cs:20-43` | PARTIAL: `byte[] Data` only (`nq:NatsSvc.cs:968`) | `[]byte` | nats.net-only | |
| Groups (+ queue group) | `AddGroupAsync` `INatsSvcServer.cs:61`, `NatsSvcServer.cs:333-389` | PRESENT `AddGroup(name, WithGroupQueueGroup/Disabled)` `nq:NatsSvc.cs:628,881-901,162-165` | `service.go:486,884,1013,1019` (impl) | — | |
| **RemoveEndpointAsync: stop one endpoint independently, drain then dispose (#1255)** | `INatsSvcServer.cs:52`, `NatsSvcServer.cs:130-145`, `NatsSvcEndPoint.cs:176-181` | MISSING: no match for grep `RemoveEndpoint` in `nq:`; `NatsSvcServer` exposes only `StopAsync` for the whole service (`nq:NatsSvc.cs:780`) | none public: nats.go has only the private `(*Endpoint).stop()` `micro/service.go:908`, used by `Service.Stop` | nats.net-only `[new]` | 18ce034 |
| PING / INFO / STATS monitoring subjects | `NatsSvcServer.cs:236-248,270-318` | PRESENT (`svc_encode_ping/info/stats`) `nq:NatsSvc.cs:~704-712`; `NatsSvc.ControlSubject` `:123` | `service.go:659-694,940` (impl) | — | |
| GetInfo / GetStats | `NatsSvcServer.cs:161,203` | PRESENT `GetInfo`, `GetStats` `nq:NatsSvc.cs:839,854` | `service.go:758,781` (impl) | — | NQ also has `Reset` and `Stopped` (`:874,827`, `service.go:812,822`) |
| Per-endpoint counters (Requests, ProcessingTime, Errors, LastError, AverageProcessingTime) | `NatsSvcEndPoint.cs:78-96` | PRESENT via `NatsSvcEndpointStats` from `GetStats` `nq:NatsSvc.cs:452-461` | `EndpointStats` `service.go:114-123` (impl) | — | nats.net exposes them live on `INatsSvcEndpoint`; NQ only through stats snapshots |
| Error reply (`ReplyErrorAsync(code, message[, data])`) | `NatsSvcMsg.cs:98,120` | PRESENT `ReplyErrorAsync(string code, string description, byte[]? data)` `nq:NatsSvc.cs:952` | `Request.Error` `micro/request.go:133` (impl) | — | the code is `int` in nats.net and `string` in NQ/nats.go |
| Reply / ReplyJson | `NatsSvcMsg.cs:68,82` | PRESENT `ReplyAsync`, `ReplyJsonAsync` `nq:NatsSvc.cs:920,936` | `request.go:104,122` (impl) | — | |
| Handler exception becomes an automatic error reply (`NatsSvcEndpointException` code/body, else 999 "Handler error") | `NatsSvcException.cs:23-46`, `NatsSvcEndPoint.cs:260-300` | MISSING: a handler exception goes to the connection error report (`nq:NatsSvc.cs:675-676`), is counted, and gets no reply | none (nats.go has no recovery or auto-reply) | nats.net-only | |
| Requester-side error helpers `IsServiceSuccess` / `EnsureServiceSuccess` / `GetServiceStatus`, `NatsSvcStatus`, `NatsSvcConstants` (#1152) | `NatsSvcMsgExtensions.cs:27,46,75`, `NatsSvcStatus.cs:7`, `NatsSvcConstants.cs:11,16` | PARTIAL: only the header constants `NatsSvc.ErrorHeader` / `ErrorCodeHeader` `nq:NatsSvc.cs:33-34`; no helpers (no match for grep `IsServiceSuccess\|GetServiceStatus`) | only constants `ErrorHeader`, `ErrorCodeHeader` `micro/service.go:270-271` (impl) | nats.net-only (helpers) `[new]` | 7d64ac7 |
| Endpoint OTel span (receive activity made current around the handler; exception recorded; span ended) (#1236) | `NatsSvcEndPoint.cs:222,252-312` | MISSING: no OTel in NQ | none | nats.net-only `[new]` | 0033cfd |
| `INatsSvcServer : IAsyncDisposable` | `INatsSvcServer.cs:9`, `NatsSvcServer.cs:228` | MISSING: `NatsSvcServer` is not `IAsyncDisposable` (`nq:NatsSvc.cs:586`) | n/a (Go) | nats.net-only | trivial (`DisposeAsync => StopAsync`) |
| Done / Error handlers, pending limits, `ContextHandler` | not in nats.net | NQ has DoneHandler/ErrorHandler/`WithEndpointPendingLimits`; `ContextHandler` is **unsupported** (`micro/request.go:96`) | `service.go:86-94,1002` | — | capability file: 120 implemented, 1 unsupported, 0 planned |

---

## 4. "planned" KV/Obj capability symbols (`ir/capabilities/jetstream/csharp.async.json`)

There are 71 planned symbols across `jetstream/kv.go` (29) and `jetstream/object.go` (42): 30 types and 41 struct fields,
all with the reason "Awaiting a C# binding and oracle conformance evidence." No method, constructor, constant or option is
planned. Every KV/Obj method, all 9 KV option constructors, the 3 Obj ShowDeleted options, the digest helpers and every
KV/Obj `Err*` sentinel are **implemented**. The same pattern holds for the rest of the jetstream file (`StreamConfig.*`,
`ConsumerConfig.*`, `PubAck.*` and so on are planned too). So this is a systematic records gap: types and fields are not
recorded. Each planned KV/Obj symbol below already has a C# counterpart.

| Group | Planned symbols (nats.go line) | Existing C# counterpart |
|---|---|---|
| KV managers / handles | `KeyValueManager` kv.go:35, `KeyValue` :92, `KeyWatcher` :351, `KeyLister` :285, `KeyValueLister` :295, `KeyValueNamesLister` :305 | `NatsJSContext`, `NatsKVStore`, `NatsKVWatcher`, `NatsKVKeyLister`, `NatsKVLister`, `NatsKVNamesLister` |
| KV status / entry | `KeyValueStatus` :311, `KeyValueBucketStatus` :811, `KeyValueEntry` :357 | `NatsKVStatus`, `NatsKVEntry` |
| KV config type + 15 fields | `KeyValueConfig` :213; `.Bucket` :217, `.Description` :220, `.MaxValueSize` :224, `.History` :228, `.TTL` :231, `.MaxBytes` :235, `.Storage` :239, `.Replicas` :243, `.Placement` :247, `.RePublish` :251, `.Mirror` :255, `.Sources` :263, `.Compression` :267, `.LimitMarkerTTL` :273, `.Metadata` :277 | `NatsKVConfig` fields `nq:JetStreamKv.cs:54-68` |
| KV option types | `WatchOpt` :384, `KVDeleteOpt` :402, `KVCreateOpt` :418, `KVPurgeOpt` :427 | `NatsKVWatchOpt`, `NatsKVDeleteOpt`, `NatsKVCreateOpt`, `NatsKVPurgeOpt` |
| Obj managers / handles | `ObjectStoreManager` object.go:40, `ObjectStore` :104, `ObjectWatcher` :250, `ObjectStoresLister` :300, `ObjectStoreNamesLister` :310, `ObjectResult` :429 | `NatsJSContext`, `NatsObjStore`, `NatsObjWatcher`, `NatsObjLister`, `NatsObjNamesLister`, `NatsObjResult` |
| Obj status | `ObjectStoreStatus` :316, `ObjectBucketStatus` :1392 | `NatsObjStatus` |
| Obj config type + 9 fields | `ObjectStoreConfig` :256; `.Bucket` :260, `.Description` :263, `.TTL` :268, `.MaxBytes` :272, `.Storage` :276, `.Replicas` :280, `.Placement` :284, `.Compression` :288, `.Metadata` :292 | `NatsObjConfig` `nq:JetStreamKv.cs:1142-1151` |
| Obj meta / options / link | `ObjectMeta` :366 (+`.Name` :369, `.Description` :372, `.Headers` :375, `.Metadata` :378, `.Opts` :381); `ObjectMetaOptions` :354 (+`.Link` :358, `.ChunkSize` :362); `ObjectLink` :417 (+`.Bucket` :419, `.Name` :423) | `NatsObjMeta`; `ObjectMetaOptions`, `ObjectLink` `nq:JetStream.cs:5163-5172` |
| Obj info + 8 fields | `ObjectInfo` :386 (+`.ObjectMeta` :388, `.Bucket` :391, `.NUID` :395, `.Size` :399, `.ModTime` :402, `.Chunks` :406, `.Digest` :410, `.Deleted` :413) | `ObjectInfo` `nq:JetStream.cs:5175-5188` (ObjectMeta embedded is flattened) |
| Obj option types | `GetObjectOpt` :436, `GetObjectInfoOpt` :439, `ListObjectsOpt` :442 | `NatsObjGetOpt`, `NatsObjGetInfoOpt`, `NatsObjListOpt` |

To promote these, add `binding` and `evidence` entries (for example `TestDotNetJetStreamAgainstOracle` / the KV/Obj oracle
fixtures) to the records. No new code is needed. The other targets show the same symbols as planned with a "J6 (KeyValue)"
reason (checked: `go.native`, `typescript.async`, `java.nio` for `KeyValueConfig.History`).

---

## 5. Most significant gaps (prioritized)

No oracle-backed KV, Obj or Services functionality is missing in NQ C#. Every gap below is **nats.net-only**: a nats.net
API with no public nats.go equivalent. Under NQ's rules a decision needs an oracle, so each of these would be a recorded
divergence or an NQ extension, not a port.

1. **Services `RemoveEndpointAsync` (#1255) `[new]`.** This is the most user-visible capability gap: you can stop one
   endpoint while the service keeps running. nats.go only has the private `Endpoint.stop` (`micro/service.go:908`). Adding it
   would need IR for removing an endpoint from the INFO/STATS state (`svc_*`) and a drain-then-dispose shell step.
2. **Handler exception gives an automatic error reply (`NatsSvcEndpointException`, code 999).** It changes wire behavior: a
   requester gets an error reply instead of a timeout. Not in nats.go.
3. **Requester-side service error helpers (#1152) `[new]`** (`IsServiceSuccess`, `EnsureServiceSuccess`, `GetServiceStatus`,
   `NatsSvcStatus`). These are pure header inspection and the constants already exist. Cheap to add as an IR decision if
   wanted.
4. **KV `PurgeDeletes` `RetainRecentlyDeletedKeyHistory` (#1252) `[new]`.** A small extension of `js_kv_purge_keep` (skip
   instead of Keep=1). Not in nats.go.
5. **OpenTelemetry spans for the KV watcher and service endpoints (#1236) `[new]`.** NQ C# has no OTel at all, so this
   would be cross-cutting work.
6. **Typed / serializer generics** (KV `NatsKVEntry<T>`, Svc `NatsSvcMsg<T>`) and **`Try*`/`NatsResult` variants.** These
   are ergonomic, nats.net-only, and the biggest shape difference for anyone migrating from nats.net.
7. **Convenience overloads and callbacks:** KV `CreateStoreAsync(string)`, Obj `CreateObjectStoreAsync(string)`,
   `PutAsync(key, Stream)`, `GetAsync(key, Stream)`, `AddLinkAsync(string, string)`, Svc `AddServiceAsync(name, version, qg)`,
   `INatsSvcServer : IAsyncDisposable`, watch `OnNoData` / `InitialSetOnly` / `IdleHeartbeat`, `WatcherThrowOnCancellation`
   (#1084 `[new]`), public `IsValidKey`. All trivial.

Already handled in NQ, with nothing to port: #1240 AllowDirect-disabled KV get (NQ's non-direct path classifies markers and
maps not-found to KeyNotFound, as nats.go does; no AllowDirect=false KV e2e case exists), #1149 duplicate-window cap
(`ir/jetstream-kv.nqir:191-194`), #1199 base64url (padded, IR), #1235 (nats.net-internal perf bug).

Behavioral differences worth recording in docs (NQ follows nats.go in each case):
- Get on a deleted key gives KeyNotFound, not KeyDeleted+revision.
- History on a missing key throws.
- Keys/List on an empty bucket or store throws NoKeysFound/NoObjectsFound.
- GetRevision uses `GetMsg(seq)` plus a subject check, not `NextBySubj`.
- PurgeDeletes threshold: 0 means the default, a negative value removes all.
- Keys containing `..` are rejected.
- Bucket statuses are filtered to `KV_` streams.
- Mirror buckets are mapped. nats.net has these last two as a TODO or bug.
