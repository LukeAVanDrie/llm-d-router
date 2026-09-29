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

import "context"

// sampleLoop advances leases and records per-scrape metrics at the metrics refresh interval, so
// sampling does not depend on request load.
func (l *Ledger) sampleLoop(ctx context.Context) {
	ticker := l.clock.NewTicker(l.sampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			l.sample()
		}
	}
}

// sample runs one sampling pass. Every lease is recomputed with its endpoint's current geometry,
// and a lease that has streamed no chunk advances by its imputed progress.
func (l *Ledger) sample() {
	l.sampleMu.Lock()
	defer l.sampleMu.Unlock()

	l.reapReservations()
	now := l.clock.Now()
	pool := l.decodeRate.estimate(now)
	unrated := 0
	l.leases.Range(func(_, v any) bool {
		ls := v.(*lease)
		prefill, decode := ls.ep.prefillRate.estimate(now), ls.ep.decodeRate.estimate(now)
		if !decode.ok {
			decode = pool
		}
		ls.update(l.geometry(ls.ep), func() {
			if ls.sawChunk {
				return
			}
			if !decode.ok {
				unrated++
			}
			decoding, age := imputedProgress(now.Sub(ls.dispatchedAt), ls.state.uncachedPrompt,
				ls.state.maxOutput, prefill, decode)
			// A running request's memory never shrinks and its prefill never restarts, even when a
			// new rate estimate implies less progress.
			ls.state.decoding = ls.state.decoding || decoding
			ls.state.age = max(ls.state.age, age)
		})
		return true
	})
	l.metrics.unratedLeases.Set(float64(unrated))

	l.mu.Lock()
	endpoints := make([]*endpoint, 0, len(l.endpoints))
	for _, e := range l.endpoints {
		endpoints = append(endpoints, e)
	}
	l.mu.Unlock()
	for _, e := range endpoints {
		l.record(e)
	}
}

// record publishes the endpoint's gauges and, when its metrics have been scraped since the last
// pass, its drift and bypass observations. The caller holds sampleMu.
func (l *Ledger) record(e *endpoint) {
	s := l.snapshot(e)
	booked := s.acct.booked
	name, namespace := e.id.Name, e.id.Namespace
	for a := range numAxes {
		l.metrics.booked.WithLabelValues(name, namespace, a.String()).Set(float64(booked[a]))
		l.metrics.capacity.WithLabelValues(name, namespace, a.String()).Set(float64(s.capacity[a]))
	}
	if s.elig != e.lastEligibility {
		if e.lastEligibility != "" {
			l.metrics.eligibility.DeleteLabelValues(name, namespace, string(e.lastEligibility))
		}
		l.metrics.eligibility.WithLabelValues(name, namespace, string(s.elig)).Set(1)
		e.lastEligibility = s.elig
	}

	m := s.metrics
	if m == nil || m.UpdateTime.IsZero() || !m.UpdateTime.After(e.lastScrape) {
		return
	}
	e.lastScrape = m.UpdateTime
	for _, a := range []axis{axisMemory, axisSlots} {
		if s.capacity[a] > 0 {
			l.metrics.drift.WithLabelValues(a.String()).Observe(float64(booked[a]-s.scraped[a]) / float64(s.capacity[a]))
		}
	}
	l.metrics.bypass.Observe(float64(int64(m.RunningRequestsSize+m.WaitingQueueSize) - booked[axisSlots]))
}
