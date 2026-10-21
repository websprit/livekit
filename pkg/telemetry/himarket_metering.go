// Copyright 2026 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/livekit/protocol/logger"
	"github.com/livekit/protocol/utils/guid"
)

type HiMarketUsageEvent struct {
	EventID        string               `json:"event_id"`
	IdempotencyKey string               `json:"idempotency_key"`
	SessionID      string               `json:"session_id"`
	RoomName       string               `json:"room_name"`
	TenantID       string               `json:"tenant_id"`
	APIKeyID       string               `json:"api_key_id"`
	Source         string               `json:"source"`
	Category       string               `json:"category"`
	Model          string               `json:"model"`
	OccurredAtMs   int64                `json:"occurred_at_ms"`
	Metrics        HiMarketUsageMetrics `json:"metrics"`
}

type HiMarketUsageMetrics struct {
	RtcDurationMs *int64 `json:"rtc_duration_ms,omitempty"`
	RtcBytes      *int64 `json:"rtc_bytes,omitempty"`
	CostUsdMicros int64  `json:"cost_usd_micros"`
}

type hiMarketMeteringSink struct {
	endpointURL  string
	sharedSecret string
	client       *http.Client
}

func newHiMarketMeteringSinkFromEnv() *hiMarketMeteringSink {
	url := strings.TrimSpace(os.Getenv("HIMARKET_METERING_URL"))
	if url == "" {
		return nil
	}
	return &hiMarketMeteringSink{
		endpointURL:  url,
		sharedSecret: strings.TrimSpace(os.Getenv("HIMARKET_INTERNAL_SECRET")),
		client:       &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *StatsWorker) DrainHiMarketEvent(isFinal bool) (HiMarketUsageEvent, bool) {
	if s == nil {
		return HiMarketUsageEvent{}, false
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	if isFinal && s.finalEmitted {
		return HiMarketUsageEvent{}, false
	}

	deltaUp := s.cumulativeUpBytes - s.reportedUpBytes
	deltaDown := s.cumulativeDownBytes - s.reportedDownBytes
	totalDeltaBytes := int64(deltaUp + deltaDown)

	if !isFinal && totalDeltaBytes == 0 {
		return HiMarketUsageEvent{}, false
	}

	s.reportedUpBytes = s.cumulativeUpBytes
	s.reportedDownBytes = s.cumulativeDownBytes
	s.flushSeq++
	if isFinal {
		s.finalEmitted = true
	}

	attrs := maps.Clone(s.attributes)
	// Only emit RTC usage once per room session for the participant carrying tenant/session attributes
	tenantID := attrs["tenant_id"]
	apiKeyID := attrs["api_key_id"]
	sessionID := attrs["session_id"]
	if tenantID == "" && apiKeyID == "" && sessionID == "" {
		return HiMarketUsageEvent{}, false
	}
	if sessionID == "" {
		sessionID = string(s.roomName)
	}

	var durationMs *int64
	if isFinal && !s.joinedAt.IsZero() {
		end := s.closedAt
		if end.IsZero() {
			end = time.Now()
		}
		d := end.Sub(s.joinedAt).Milliseconds()
		if d < 0 {
			d = 0
		}
		durationMs = &d
	}

	ev := HiMarketUsageEvent{
		EventID:        guid.New("EV_RTC_"),
		IdempotencyKey: fmt.Sprintf("lk_rtc:%s:%s:%d", s.roomID, s.participantID, s.flushSeq),
		SessionID:      sessionID,
		RoomName:       string(s.roomName),
		TenantID:       tenantID,
		APIKeyID:       apiKeyID,
		Source:         "livekit_sfu",
		Category:       "rtc",
		Model:          "webrtc-audio",
		OccurredAtMs:   time.Now().UnixMilli(),
		Metrics: HiMarketUsageMetrics{
			RtcDurationMs: durationMs,
			RtcBytes:      &totalDeltaBytes,
			CostUsdMicros: 0,
		},
	}
	return ev, true
}

func (m *hiMarketMeteringSink) Send(ctx context.Context, ev HiMarketUsageEvent) {
	if m == nil || m.endpointURL == "" {
		return
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.endpointURL, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if m.sharedSecret != "" {
		req.Header.Set("X-HiMarket-Internal-Secret", m.sharedSecret)
	}
	resp, err := m.client.Do(req)
	if err != nil {
		logger.Warnw("failed to push RTC metering event to HiMarket", err, "room", ev.RoomName, "sessionID", ev.SessionID)
		return
	}
	_ = resp.Body.Close()
}
