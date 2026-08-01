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

// Package capacityledger bridges endpoint lifecycle events to the capacity
// ledger. It owns no accounting: it reports the hardware backing the pool and
// republishes the ledger's per-endpoint reading as an endpoint attribute.
package capacityledger

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	fwkcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/capacity"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrcapacity "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/capacity"
	capacityconstants "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/capacityledger/constants"
	sourcenotifications "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/notifications"
)

// CapacityLedgerAdapterType is the plugin type name used in configuration.
const CapacityLedgerAdapterType = capacityconstants.CapacityLedgerAdapterType

// ErrNoLedger reports that the adapter was configured in a build with no capacity
// ledger. Constructing it anyway would leave the ledger permanently empty, and an
// empty ledger reads as saturated, so the adapter refuses instead.
var ErrNoLedger = errors.New("capacity ledger is unavailable; enable the FlowControl feature gate")

// Adapter feeds endpoint lifecycle into the ledger and publishes its reading back.
type Adapter struct {
	typedName fwkplugin.TypedName
	ledger    fwkcapacity.Ledger
	dk        fwkplugin.DataKey

	// registeredEndpoints tracks the Endpoint object seen at add time so a delete
	// arriving after a replacement can be recognized as stale.
	registeredEndpoints sync.Map
}

var (
	_ datalayer.EndpointExtractor = (*Adapter)(nil)
	_ datalayer.Registrant        = (*Adapter)(nil)
	_ fwkplugin.ProducerPlugin    = (*Adapter)(nil)
)

func AdapterFactory(name string, _ *json.Decoder, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if handle == nil {
		return nil, errors.New("handle is nil")
	}
	l := handle.CapacityLedger()
	if l == nil {
		return nil, ErrNoLedger
	}
	return &Adapter{
		typedName: fwkplugin.TypedName{Type: CapacityLedgerAdapterType, Name: name},
		ledger:    l,
		dk:        attrcapacity.AvailableCapacityDataKey.WithNonEmptyProducerName(name),
	}, nil
}

func (a *Adapter) TypedName() fwkplugin.TypedName { return a.typedName }

// Produces declares the attribute this plugin publishes. There is no Produce
// method: the attribute is dynamic and resolves against live ledger state, and
// admission needs the reading before the per-request producer pass runs.
func (a *Adapter) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{a.dk: attrcapacity.AvailableCapacity{}}
}

// RegisterDependencies declares the endpoint-notification source the adapter needs
// to observe endpoint lifecycle. The source is auto-created when absent from the
// config.
func (a *Adapter) RegisterDependencies(r datalayer.Registrar) error {
	return r.Register(datalayer.PendingRegistration{
		Owner:      a.typedName,
		SourceType: sourcenotifications.EndpointNotificationSourceType,
		Extractor:  a,
		DefaultSource: sourcenotifications.NewEndpointDataSource(
			sourcenotifications.EndpointNotificationSourceType, sourcenotifications.EndpointNotificationSourceType),
	})
}

// Extract mirrors endpoint lifecycle into the ledger.
func (a *Adapter) Extract(ctx context.Context, event datalayer.EndpointEvent) error {
	if event.Endpoint == nil || event.Endpoint.GetMetadata() == nil {
		return nil
	}
	id := event.Endpoint.GetMetadata().NamespacedName.String()
	logger := log.FromContext(ctx)

	switch event.Type {
	case datalayer.EventDelete:
		// This guard assumes the datalayer delivers the same Endpoint pointer for
		// delete as was used for the preceding add. If the datalayer ever
		// reconstructs endpoint objects on delete, this check would need to use a
		// generation counter instead of pointer identity.
		if registered, ok := a.registeredEndpoints.Load(id); ok && registered != event.Endpoint {
			logger.V(logutil.DEFAULT).Info("Ignoring stale delete for replaced endpoint", "endpoint", id)
			break
		}
		a.registeredEndpoints.Delete(id)
		a.ledger.DeleteEndpoint(id)
		logger.V(logutil.DEFAULT).Info("Removed endpoint from capacity ledger", "endpoint", id)

	case datalayer.EventAddOrUpdate:
		a.registeredEndpoints.Store(id, event.Endpoint)
		a.ledger.UpsertEndpoint(endpointCapacity(id, event.Endpoint.GetMetrics()))
		event.Endpoint.GetAttributes().Put(a.dk.String(), &datalayer.DynamicAttribute{
			Get: func() datalayer.Cloneable {
				return &attrcapacity.AvailableCapacity{Footprint: a.ledger.EndpointAvailable(id)}
			},
		})
		logger.V(logutil.TRACE).Info("Refreshed endpoint capacity in ledger", "endpoint", id)
	}
	return nil
}

// endpointCapacity derives the KV axis from scraped block geometry. Metrics is nil
// before the first scrape completes, and both fields read zero until the engine
// reports them, which registers the endpoint with no KV capacity until a later
// event carries real numbers.
func endpointCapacity(id string, m *datalayer.Metrics) fwkcapacity.EndpointCapacity {
	if m == nil {
		return fwkcapacity.EndpointCapacity{ID: id}
	}
	blockSize := int64(m.CacheBlockSize)
	return fwkcapacity.EndpointCapacity{
		ID:              id,
		KVTokens:        int64(m.CacheNumBlocks) * blockSize,
		BlockSizeTokens: blockSize,
	}
}
