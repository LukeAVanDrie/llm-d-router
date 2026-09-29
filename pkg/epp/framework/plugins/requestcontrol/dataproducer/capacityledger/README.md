# Capacity Ledger Plugin

**Type:** `capacity-ledger` (Alpha)

Books every placed request against the endpoint that serves it and publishes, per endpoint, an
`EndpointCapacity` view with three axes:

| Axis | Unit | Booked from | Scraped from | Capacity |
|------|------|-------------|--------------|----------|
| Memory | KV cache blocks | each sequence's prompt, generated tokens and next decode step, rounded up to blocks | `vllm:kv_cache_usage_perc` times the block count, rounded up | block count from `vllm:cache_config_info`, less vLLM's null block |
| Step | tokens committed to the next step | each request's uncached prompt, capped at `stepTokenBudget`, until prefill ends; then one decode step per sequence | one decode step per running request | `stepTokenBudget` |
| Slots | sequences | one per sequence | `vllm:num_requests_running` | `slotsPerEndpoint` |

A decode step is 1 plus `speculativeTokens` tokens: the next token's slot and the speculative
tokens. A request generates `n` sequences per prompt; the client's `n` is clamped so a request
books at most `slotsPerEndpoint` sequences.

On each axis, `Used` is `Scraped` plus the increases the ledger has booked since that scrape, and
never less than `Booked`. Releases after the scrape do not offset the increases, since the engine
may have freed a finished request before the scrape; a booking made and released between two
scrapes counts until the next scrape. When the ledger has no record for the current scrape, `Used`
is the larger of `Booked` and `Scraped`. No in-tree plugin reads the
view; it exists so its accuracy can be observed before any admission decision depends on it.

## Behavior

- **Booking.** `PreRequest` books a lease on the primary profile's first target endpoint. The
  tokenized prompt is a required dependency; a request that reaches the ledger without one is
  bounded by its size in bytes. Each lease's charge is recomputed from the endpoint's current block
  size on every update, so a lease booked before the endpoint's first scrape books memory once the
  block size is known. A lease is released at end of stream, or when the request's stream context
  ends, so a missed end of stream cannot leak it.
- **Streaming.** Any chunk ends prefill. Each sequence's age is its share of the response's
  cumulative streamed events, one decode step per event, capped at the maximum output; cumulative
  completion tokens replace the event count when the stream reports usage.
- **Imputed progress.** A request that has streamed no chunk, including a non-streaming request or a
  streaming request answered with one buffered body, advances at each sampling pass by the
  endpoint's prefill rate (uncached prompt tokens per second to first chunk, measured from prompts
  of at least 128 uncached tokens) and decode rate (tokens per second between chunks, and, for a
  request that streamed none, its reported completion tokens over its time at end of stream). An
  endpoint without a decode rate uses the rate across all endpoints. Imputed progress never
  decreases; the first chunk's observed age replaces it. A rate expires 30s after its last
  observation. Without a prefill rate the request counts as prefilled at the next sampling pass,
  since the step budget is held only for the prefill's few steps. Without any decode rate the
  request books no output, and the engine's report covers its growth from the next scrape;
  `capacity_ledger_unrated_leases` counts such requests.
- **Prefix cache.** The prefix match reduces the step axis only, counting cached blocks without
  device-tier weighting. Memory is booked for the full prompt: vLLM counts cached blocks that no
  running request references as free, and a prefix hit on one moves it out of the free pool. Memory
  is also booked per sequence, so k running requests or sequences that share a prefix book it k
  times while the engine holds it once; the drift histogram shows the overstatement.
- **Endpoint replacement.** When the datalayer adds a new endpoint under an ID before deleting the
  old one, the ID keeps its account, so requests still running on the replaced endpoint stay
  booked until they end. When the delete arrives first, the new endpoint starts with an empty
  account.
- **Eligibility.** The view reports when the accounting does not hold for an endpoint: `stale`
  metrics (older than `--metrics-staleness-threshold`), `no-block-count`, `multi-engine` (more than one vLLM `engine` label value behind the
  endpoint), `hybrid-layout` (a sliding-window or hybrid cache layout), `speculative-unconfigured`
  (the engine exports `vllm:spec_decode_*` metrics and `speculativeTokens` is 0), `engine-type`
  (not vLLM), or `disaggregated` (a prefill or decode role). The engine type is read from the
  metrics extractor's default label keys; an extractor configured with another label key or default
  engine is not detected.
- **Replicas.** The accounting assumes this router is the only sender to its endpoints.

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `slotsPerEndpoint` | `int` | Yes | | The engine's sequence limit (vLLM `max_num_seqs`). |
| `stepTokenBudget` | `int` | Yes | | The engine's per-step token budget (vLLM `max_num_scheduled_tokens` when set, else `max_num_batched_tokens`). |
| `maxModelLen` | `int` | Yes | | The model's context length (vLLM `max_model_len`); bounds every request's output. |
| `speculativeTokens` | `int` | No | `0` | vLLM `num_speculative_tokens`. |
| `prefixMatchInfoProducerName` | `string` | No | approximate-prefix producer | Which prefix-cache producer's match to read. |

The plugin is not created automatically, because its engine parameters have no defaults.

**Configuration Example:**
```yaml
plugins:
  - type: capacity-ledger
    parameters:
      slotsPerEndpoint: 256
      stepTokenBudget: 8192
      maxModelLen: 32768
```

## Related Documentation
- [Metrics](../../../../../../../docs/metrics.md#capacity-ledger)
