/*
Copyright 2026 The Kubernetes Authors.

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

// Package sessionstate provides a DataProducer that tracks agentic session
// history and publishes it for scheduling plugins, attaching a dynamic
// attribute in RequestHeader for flow-control plugins.
package sessionstate

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/requestheader/agentidentity"
	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

const (
	defaultEvictionTTL           = time.Hour
	defaultEvictionSweepInterval = 5 * time.Minute
)

// Parameters configures idle session eviction.
type Parameters struct {
	// EvictionTTLSeconds is the maximum idle time before session state is removed.
	// Zero disables eviction.
	EvictionTTLSeconds float64 `json:"evictionTtlSeconds,omitempty"`
	// EvictionSweepSeconds is how often idle session state is scanned.
	EvictionSweepSeconds float64 `json:"evictionSweepSeconds,omitempty"`
}

func (p Parameters) validate() error {
	if p.EvictionTTLSeconds < 0 {
		return fmt.Errorf("evictionTtlSeconds must be >= 0, got %v", p.EvictionTTLSeconds)
	}
	if p.EvictionSweepSeconds <= 0 {
		return fmt.Errorf("evictionSweepSeconds must be > 0, got %v", p.EvictionSweepSeconds)
	}
	return nil
}

var (
	_ requestcontrol.RequestHeaderProcessor = &Producer{}
	_ requestcontrol.DataProducer           = &Producer{}
	_ requestcontrol.PreRequest             = &Producer{}
	_ requestcontrol.ResponseBodyProcessor  = &Producer{}
	_ fwkplugin.ConsumerPlugin              = &Producer{}
)

type dispatchTracker struct {
	mu       sync.Mutex
	done     bool
	stopFunc func() bool
}

// Producer tracks session history within one EPP instance.
type Producer struct {
	typedName fwkplugin.TypedName
	dk        fwkplugin.DataKey

	registry   SessionStateRegistry
	dispatches sync.Map // key: *fwksched.InferenceRequest, value: *dispatchTracker

	evictionTTL           time.Duration
	evictionSweepInterval time.Duration
}

// Factory builds a session-state producer.
func Factory(name string, rawParameters *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	params := Parameters{
		EvictionTTLSeconds:   defaultEvictionTTL.Seconds(),
		EvictionSweepSeconds: defaultEvictionSweepInterval.Seconds(),
	}
	if rawParameters != nil {
		if err := rawParameters.Decode(&params); err != nil {
			return nil, fmt.Errorf("invalid config for %s plugin %q: %w", SessionStateProducerType, name, err)
		}
	}
	if err := params.validate(); err != nil {
		return nil, fmt.Errorf("%s plugin %q: %w", SessionStateProducerType, name, err)
	}

	p := &Producer{
		typedName:             fwkplugin.TypedName{Type: SessionStateProducerType, Name: name},
		dk:                    SessionStateDataKey.WithNonEmptyProducerName(name),
		evictionTTL:           time.Duration(params.EvictionTTLSeconds * float64(time.Second)),
		evictionSweepInterval: time.Duration(params.EvictionSweepSeconds * float64(time.Second)),
	}
	if handle != nil && p.evictionTTL > 0 {
		go p.runEviction(handle.Context())
	}
	return p, nil
}

// TypedName returns the type and name of the plugin.
func (p *Producer) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// Produces declares the SessionState request attribute written by this producer.
func (p *Producer) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{p.dk: SessionState{}}
}

// Consumes declares agent identity as a required input. The agent-identity
// plugin is intentionally not a default producer, so operators must enable an
// identity provider explicitly rather than having one auto-created.
func (p *Producer) Consumes() fwkplugin.DataDependencies {
	return fwkplugin.DataDependencies{
		Required: map[fwkplugin.DataKey]any{agentidentity.AgentIdentityKey: ""},
	}
}

// RequestHeader attaches a DynamicAttribute so flow-control plugins observe
// live SessionState while the request waits for admission.
func (p *Producer) RequestHeader(_ context.Context, request *fwksched.InferenceRequest) error {
	identity, ok := readAgentIdentity(request)
	if !ok {
		return nil
	}
	request.PutAttribute(p.dk, &fwkdl.DynamicAttribute{
		Get: func() fwkdl.Cloneable {
			return p.registry.PeekState(identity)
		},
	})
	return nil
}

// Produce publishes the history observed before the current request is
// dispatched, then marks the session as seen at the current time.
func (p *Producer) Produce(_ context.Context, request *fwksched.InferenceRequest, _ []fwksched.Endpoint) error {
	identity, ok := readAgentIdentity(request)
	if !ok {
		return nil
	}

	state := p.registry.GetState(identity)
	request.PutAttribute(p.dk, state)
	return nil
}

// PreRequest records one dispatched turn and its primary target endpoint, and
// registers a context-cancellation backstop so aborted requests do not leak InFlightRequests.
func (p *Producer) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, result *fwksched.SchedulingResult) error {
	identity, ok := readAgentIdentity(request)
	if !ok {
		return nil
	}

	p.registry.RecordDispatch(identity, primaryTargetEndpoint(result))
	tracker := &dispatchTracker{}
	tracker.mu.Lock()
	p.dispatches.Store(request, tracker)
	if ctx != nil {
		tracker.stopFunc = context.AfterFunc(ctx, func() {
			tracker.mu.Lock()
			if tracker.done {
				tracker.mu.Unlock()
				return
			}
			tracker.done = true
			tracker.mu.Unlock()
			p.dispatches.Delete(request)
			p.registry.RecordResponse(identity, false, 0, 0)
		})
	}
	tracker.mu.Unlock()
	return nil
}

// ResponseBody updates session counters after the response lifecycle ends.
// Only naturally completed responses contribute completion and token totals.
func (p *Producer) ResponseBody(
	_ context.Context,
	request *fwksched.InferenceRequest,
	response *requestcontrol.Response,
	_ *fwkdl.EndpointMetadata,
) {
	if response == nil || !response.EndOfStream {
		return
	}
	identity, ok := readAgentIdentity(request)
	if !ok {
		return
	}

	val, tracked := p.dispatches.LoadAndDelete(request)
	if !tracked {
		return
	}
	tracker := val.(*dispatchTracker)
	tracker.mu.Lock()
	if tracker.done {
		tracker.mu.Unlock()
		return
	}
	tracker.done = true
	stop := tracker.stopFunc
	tracker.mu.Unlock()
	if stop != nil {
		stop()
	}

	p.registry.RecordResponse(
		identity,
		response.TerminationCause == requestcontrol.TerminationCauseNatural,
		int64(response.Usage.PromptTokens),
		int64(response.Usage.CompletionTokens),
	)
}

func primaryTargetEndpoint(result *fwksched.SchedulingResult) k8stypes.NamespacedName {
	if result == nil || len(result.ProfileResults) == 0 {
		return k8stypes.NamespacedName{}
	}
	primaryResult := result.ProfileResults[result.PrimaryProfileName]
	if primaryResult == nil && len(result.ProfileResults) == 1 {
		for _, r := range result.ProfileResults {
			primaryResult = r
		}
	}
	if primaryResult == nil || len(primaryResult.TargetEndpoints) == 0 || primaryResult.TargetEndpoints[0] == nil {
		return k8stypes.NamespacedName{}
	}
	if md := primaryResult.TargetEndpoints[0].GetMetadata(); md != nil {
		return md.ID
	}
	return k8stypes.NamespacedName{}
}

func readAgentIdentity(request *fwksched.InferenceRequest) (string, bool) {
	if request == nil {
		return "", false
	}
	if identity, ok := fwksched.ReadRequestAttribute[string](request, agentidentity.AgentIdentityKey); ok && identity != "" {
		return identity, true
	}
	if request.FairnessID != "" && request.FairnessID != metadata.DefaultFairnessID {
		return request.FairnessID, true
	}
	return "", false
}

func (p *Producer) runEviction(ctx context.Context) {
	ticker := time.NewTicker(p.evictionSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.evictIdle(time.Now())
		}
	}
}

func (p *Producer) evictIdle(now time.Time) {
	p.registry.EvictIdle(now, p.evictionTTL)
}
