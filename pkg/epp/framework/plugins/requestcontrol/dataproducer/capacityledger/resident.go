/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package capacityledger

import (
	"math"
	"sync"
	"time"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// residentState is one request's state on an endpoint.
type residentState struct {
	// prompts holds each prompt's length in tokens; one entry for a single-prompt request.
	prompts []int64
	// uncachedPrompt is the prompt tokens the endpoint must prefill, summed over prompts.
	uncachedPrompt int64
	// n is the number of sequences generated per prompt.
	n int64
	// maxOutput bounds each sequence's output tokens.
	maxOutput int64
	// age is the output tokens each sequence has generated.
	age int64
	// decoding is set once the request has finished prefill.
	decoding bool
}

func (r residentState) sequences() int64 { return mulSat(r.n, int64(len(r.prompts))) }

// geometry is an endpoint's parameters that convert a request's state into a charge.
type geometry struct {
	blockSize int64
	// stepBudget is the engine's per-step token budget.
	stepBudget int64
	// decodeStep is the tokens one decode step schedules per sequence, and the KV slots one
	// sequence holds beyond its prompt and output: the next token plus the speculative tokens.
	decodeStep int64
}

// footprint is the charge a request in state r places on an endpoint with geometry g.
//   - Memory: each sequence holds its prompt, its output so far and its next decode step, rounded
//     up to blocks per sequence. The prefix match does not reduce memory: a hit on a cached block
//     that no running request references moves that block out of the engine's free pool.
//   - Step: while prefilling, the uncached prompt of every sequence, capped at the step budget,
//     since the engine prefills a larger prompt in chunks across steps; while decoding, one decode
//     step per sequence.
//   - Slots: one per sequence.
func footprint(r residentState, g geometry) vec {
	var c vec
	for _, p := range r.prompts {
		c[axisMemory] = addSat(c[axisMemory], mulSat(r.n, blocksFor(p+r.age+g.decodeStep, g.blockSize)))
	}
	if r.decoding {
		c[axisStep] = mulSat(r.sequences(), g.decodeStep)
	} else {
		c[axisStep] = min(mulSat(r.n, r.uncachedPrompt), g.stepBudget)
	}
	c[axisSlots] = r.sequences()
	return c
}

// blocksFor rounds tokens up to whole blocks. It returns 0 when the block size is unknown.
func blocksFor(tokens, blockSize int64) int64 {
	if blockSize <= 0 || tokens <= 0 {
		return 0
	}
	return (tokens + blockSize - 1) / blockSize
}

// mulSat multiplies non-negative a and b, saturating at math.MaxInt64.
func mulSat(a, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

// addSat adds non-negative a and b, saturating at math.MaxInt64.
func addSat(a, b int64) int64 {
	if a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}

// lease is one placed request's charge on its endpoint. Its footprint is recomputed from the
// endpoint's current geometry on every update, and the account moves by the change.
type lease struct {
	ep           *endpoint
	dispatchedAt time.Time

	mu sync.Mutex
	// stopBackstop cancels the release armed on the request's context.
	stopBackstop func() bool
	released     bool
	state        residentState
	// sawChunk is set by the first streamed chunk; until then the lease's progress is imputed.
	sawChunk  bool
	lastChunk time.Time
	contrib   vec
}

// update applies fn to the lease's state and moves the endpoint's account by the change in its
// footprint. It is a no-op after release. The account is updated under the lease's lock, so the
// account always equals the sum of its leases' recorded contributions; the account never takes a
// lease's lock.
func (l *lease) update(g geometry, fn func()) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return
	}
	fn()
	next := footprint(l.state, g)
	l.ep.acct.apply(next.sub(l.contrib), vec{})
	l.contrib = next
}

// release removes the lease's whole contribution. It is idempotent.
func (l *lease) release() {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return
	}
	l.released = true
	l.ep.acct.apply(vec{}.sub(l.contrib), vec{})
	l.contrib = vec{}
	stop := l.stopBackstop
	l.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// setBackstop records the cancel function of the release armed on the request's context, calling
// it at once if the lease is already released.
func (l *lease) setBackstop(stop func() bool) {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		stop()
		return
	}
	l.stopBackstop = stop
	l.mu.Unlock()
}

// promptLengths returns each prompt's length in tokens. Without a tokenized prompt it returns one
// entry bounded by bytes, the request's size in bytes, which exceeds the token count for text.
func promptLengths(req *fwksched.InferenceRequest, bytes int64) []int64 {
	if req != nil && req.Body != nil && req.Body.TokenizedRequest != nil {
		lengths := make([]int64, 0, len(req.Body.TokenizedRequest.Prompts))
		for _, p := range req.Body.TokenizedRequest.Prompts {
			lengths = append(lengths, int64(len(p.TokenIDs)))
		}
		if len(lengths) > 0 {
			return lengths
		}
	}
	return []int64{max(bytes, 0)}
}

func sum(xs []int64) int64 {
	var s int64
	for _, x := range xs {
		s += x
	}
	return s
}

// sequencesPerPrompt returns the request's n, the number of sequences it generates per prompt,
// clamped to limit. n is client-supplied, and the engine runs at most its sequence limit at once.
func sequencesPerPrompt(req *fwksched.InferenceRequest, limit int64) int64 {
	if req == nil || req.Body == nil || req.Body.Payload == nil {
		return 1
	}
	m, ok := req.Body.Payload.AsMap()
	if !ok {
		return 1
	}
	if n := fwkrh.MaxOutputTokensFromPayload(m, "n"); n != nil && *n > 1 {
		return max(min(*n, limit), 1)
	}
	return 1
}

// maxOutputTokens returns the most output tokens one sequence can generate: the client's cap,
// bounded by the model's context length less the longest prompt.
func maxOutputTokens(req *fwksched.InferenceRequest, longestPrompt, maxModelLen int64) int64 {
	byContext := max(maxModelLen-longestPrompt, 0)
	if req == nil || req.Body == nil || req.Body.MaxOutputTokens == nil || *req.Body.MaxOutputTokens <= 0 {
		return byContext
	}
	return min(*req.Body.MaxOutputTokens, byContext)
}

// uncachedPromptTokens returns the prompt tokens the endpoint must prefill: the indexed blocks it
// has not cached, plus any prompt beyond the portion the prefix index hashes. Cached blocks are
// counted without device-tier weighting, since a block cached in any tier is not prefilled again.
func uncachedPromptTokens(endpoint fwksched.Endpoint, prompt int64, prefixKey fwkplugin.DataKey) int64 {
	raw, ok := endpoint.Get(prefixKey)
	if !ok {
		return prompt
	}
	info, ok := raw.(*attrprefix.PrefixCacheMatchInfo)
	if !ok || info == nil || info.BlockSizeTokens() <= 0 {
		return prompt
	}
	blockSize := int64(info.BlockSizeTokens())
	indexed := int64(info.TotalBlocks()) * blockSize
	cached := min(int64(info.CachedBlockCount())*blockSize, indexed)
	return min(max(indexed-cached, 0)+max(prompt-indexed, 0), max(prompt, 0))
}

// imputedProgress estimates how far a request that has streamed no chunk has gone after elapsed
// time, from measured prefill and decode rates: whether its prefill is done, and the output tokens
// each sequence has generated, capped at its maximum output. Without a prefill rate, prefill is
// taken as done: the step budget is held only for the prefill's few steps, and holding it for the
// request's lifetime would cap concurrency at the budget over the prompt. Without a decode rate,
// the output is taken as none; the engine's report covers the growth from the next scrape.
func imputedProgress(elapsed time.Duration, uncachedPrompt, maxOutput int64,
	prefill, decode rateEstimate) (decoding bool, age int64) {
	decodingFor := elapsed
	if prefill.ok && prefill.value > 0 {
		prefillTime := time.Duration(float64(uncachedPrompt) / prefill.value * float64(time.Second))
		if elapsed < prefillTime {
			return false, 0
		}
		decodingFor = elapsed - prefillTime
	}
	if !decode.ok {
		return true, 0
	}
	return true, min(max(int64(decode.value*decodingFor.Seconds()), 0), maxOutput)
}
