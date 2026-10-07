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

package multimodal

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func newBenchMMContext(b *testing.B) context.Context {
	b.Helper()
	ctx, cancel := context.WithCancel(log.IntoContext(context.Background(), logr.Discard()))
	b.Cleanup(cancel)
	return ctx
}

func makeBenchPodNames(numPods int) []k8stypes.NamespacedName {
	pods := make([]k8stypes.NamespacedName, numPods)
	for i := range numPods {
		pods[i] = k8stypes.NamespacedName{
			Namespace: "default",
			Name:      "pod-" + strconv.Itoa(i),
		}
	}
	return pods
}

func makeBenchEndpoints(pods []k8stypes.NamespacedName) []scheduling.Endpoint {
	endpoints := make([]scheduling.Endpoint, len(pods))
	for i, pod := range pods {
		endpoints[i] = scheduling.NewEndpoint(
			&fwkdl.EndpointMetadata{ID: pod},
			&fwkdl.Metrics{},
			nil,
		)
	}
	return endpoints
}

func makeBenchMMRequest(requestID string, hashes []string) *scheduling.InferenceRequest {
	features := make([]fwkrh.MultiModalFeature, len(hashes))
	for i, h := range hashes {
		features[i] = fwkrh.MultiModalFeature{
			Modality: fwkrh.ModalityImage,
			Hash:     h,
			Length:   256,
		}
	}
	return &scheduling.InferenceRequest{
		RequestID: requestID,
		Body: &fwkrh.InferenceRequestBody{
			TokenizedRequest: &fwkrh.TokenizedRequest{
				Prompts: []fwkrh.PromptTokens{{MultiModalFeatures: features}},
			},
		},
	}
}

func reportMMThroughput(b *testing.B, numItems, numPods int) {
	b.Helper()
	elapsed := b.Elapsed().Seconds()
	if elapsed <= 0 {
		return
	}
	reqsPerSec := float64(b.N) / elapsed
	b.ReportMetric(reqsPerSec, "reqs/s")
	b.ReportMetric(reqsPerSec*float64(numItems), "item_ops/s")
	b.ReportMetric(reqsPerSec*float64(numItems*numPods), "pod_item_ops/s")
}

// BenchmarkEncoderCacheProduce measures multimodal encoder-cache Produce and
// PreRequest throughput and allocations across QueryOnly (Sequential and
// Parallel) and MixedOverload (90% Produce / 10% Produce+PreRequest under
// RunParallel) for fleet sizes P=16,96 and media item counts M=1,4,8.
func BenchmarkEncoderCacheProduce(b *testing.B) {
	const (
		numQueryBatches = 16
		numWriteBatches = 16
		batchMask       = numQueryBatches - 1
	)

	type benchMode struct {
		name       string
		parallel   bool
		withWrites bool
	}
	modes := []benchMode{
		{name: "QueryOnly_Sequential", parallel: false, withWrites: false},
		{name: "QueryOnly_Parallel", parallel: true, withWrites: false},
		{name: "MixedOverload_Parallel", parallel: true, withWrites: true},
	}
	podCounts := []int{16, 96}
	itemCounts := []int{1, 4, 8}

	for _, bm := range modes {
		for _, numPods := range podCounts {
			for _, numItems := range itemCounts {
				name := fmt.Sprintf("Mode=%s/Pods=%d/Items=%d", bm.name, numPods, numItems)
				b.Run(name, func(b *testing.B) {
					ctx := newBenchMMContext(b)
					producer, err := New(ctx, "bench-mm", &Parameters{CacheSizeInMBPerServer: 4096}, nil)
					if err != nil {
						b.Fatal(err)
					}
					pods := makeBenchPodNames(numPods)

					// Warm the cache with realistic partial/overlapping pod placement:
					// item m in query batch q is cached on half the pods (every 2nd pod shifted by m+q).
					queryReqs := make([]*scheduling.InferenceRequest, numQueryBatches)
					for q := range numQueryBatches {
						hashes := make([]string, numItems)
						for m := range numItems {
							h := fmt.Sprintf("img-hash-q%d-m%d", q, m)
							hashes[m] = h
							for pIdx := range numPods {
								if (pIdx+m+q)%2 == 0 {
									producer.putCacheEntry(h, pods[pIdx])
								}
							}
						}
						queryReqs[q] = makeBenchMMRequest("", hashes)
					}

					writeHashes := make([][]string, numWriteBatches)
					for w := range numWriteBatches {
						hashes := make([]string, numItems)
						for m := range numItems {
							hashes[m] = fmt.Sprintf("img-write-w%d-m%d", w, m)
						}
						writeHashes[w] = hashes
					}

					schedResults := make([]*scheduling.SchedulingResult, numPods)
					baseEndpoints := makeBenchEndpoints(pods)
					for pIdx := range numPods {
						schedResults[pIdx] = schedulingResult(baseEndpoints[pIdx])
					}

					b.ReportAllocs()
					b.ResetTimer()
					if !bm.parallel {
						endpoints := makeBenchEndpoints(pods)
						for i := 0; i < b.N; i++ {
							req := queryReqs[i&batchMask]
							if err := producer.Produce(ctx, req, endpoints); err != nil {
								b.Fatal(err)
							}
						}
					} else {
						var workerSeq atomic.Uint64
						b.RunParallel(func(pb *testing.PB) {
							wID := int(workerSeq.Add(1) - 1)
							endpoints := makeBenchEndpoints(pods)
							reqID := fmt.Sprintf("bench-worker-%d", wID)
							writeReqs := make([]*scheduling.InferenceRequest, numWriteBatches)
							for w := range numWriteBatches {
								writeReqs[w] = makeBenchMMRequest(reqID, writeHashes[w])
							}
							step := wID * 13
							writeStep := wID
							for pb.Next() {
								step++
								if bm.withWrites && step%10 == 0 {
									wReq := writeReqs[writeStep&batchMask]
									targetPod := writeStep % numPods
									writeStep++
									if err := producer.Produce(ctx, wReq, endpoints); err != nil {
										b.Error(err)
										return
									}
									if err := producer.PreRequest(ctx, wReq, schedResults[targetPod]); err != nil {
										b.Error(err)
										return
									}
									continue
								}
								req := queryReqs[step&batchMask]
								if err := producer.Produce(ctx, req, endpoints); err != nil {
									b.Error(err)
									return
								}
							}
						})
					}
					b.StopTimer()
					producer.wg.Wait()
					reportMMThroughput(b, numItems, numPods)
				})
			}
		}
	}
}
