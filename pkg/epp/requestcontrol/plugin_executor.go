/*
Copyright 2025 The Kubernetes Authors.

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

package requestcontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// executePluginsAsDAG executes DataProducer plugins sequentially based on their dependencies.
// If any plugin fails with error or panics, it returns an error.
func executePluginsAsDAG(ctx context.Context, plugins []fwkrc.DataProducer, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	for _, plugin := range plugins {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := func() (prodErr error) {
			defer func() {
				if r := recover(); r != nil {
					prodErr = fmt.Errorf("panic in DataProducer %q: %v", plugin.TypedName().String(), r)
					log.FromContext(ctx).Error(prodErr, "panic caught during plugin execution", "plugin", plugin.TypedName().String())
				}
			}()
			return plugin.Produce(ctx, request, endpoints)
		}()
		if err != nil {
			return fmt.Errorf("DataProducer %q failed: %w", plugin.TypedName().String(), err)
		}
	}
	return nil
}

// dataProducerPluginsWithTimeout executes DataProducer plugins sequentially with a timeout.
// The child context is cancelled when the timeout fires so plugins can observe cancellation.
func dataProducerPluginsWithTimeout(ctx context.Context, timeout time.Duration, plugins []fwkrc.DataProducer,
	request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := executePluginsAsDAG(ctx, plugins, request, endpoints)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("DataProducer execution timed out: %w", ctx.Err())
		}
		return err
	}
	return nil
}
