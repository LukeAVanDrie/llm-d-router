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

// Package flowcontrol defines the core plugin interfaces for extending the Flow Control layer.
//
// It establishes the contracts that custom logic, such as queueing disciplines and dispatching policies, must adhere
// to. By building on these interfaces, the Flow Control layer can be extended and customized without modifying the
// core controller logic.
//
// The primary contracts are:
//   - FairnessPolicy: The interface for policies that govern the competition between flows.
//   - OrderingPolicy: The interface for policies that decide the strict sequence of service within a flow.
//   - SaturationDetector: The interface for plugins that provide real-time pool load signals.
//   - UsageLimitPolicy: The interface for policies that compute per-priority admission ceilings.
//   - EvictionFilterPolicy and EvictionOrderingPolicy: The interfaces for policies that govern
//     eviction of in-flight requests.
//
// An OrderingPolicy supplies the comparator (Less) that determines dispatch order within a flow's queue.
package flowcontrol
