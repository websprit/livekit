package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHiMarketMeteringDrainAndSend(t *testing.T) {
	var received []HiMarketUsageEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "test-secret", r.Header.Get("X-HiMarket-Internal-Secret"))
		var ev HiMarketUsageEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&ev))
		received = append(received, ev)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	guard := &ReferenceGuard{}
	w := newStatsWorker(
		context.Background(),
		NullTelemetryService{},
		"RM_1001",
		"lk_rm_sess_01",
		"PA_2001",
		"client_cons_01",
		guard,
	)
	joined := time.Now().Add(-15 * time.Second)
	w.SetParticipantMeta(map[string]string{
		"session_id": "sess_01",
		"tenant_id":  "dev_01",
		"api_key_id": "ak_live_01",
	}, joined)
	w.SetConnected()

	now := time.Now()
	w.OnTrackStat("TR_audio_up", livekit.StreamType_UPSTREAM, &livekit.AnalyticsStat{
		Streams: []*livekit.AnalyticsStream{
			{
				StartTime:      timestamppb.New(now.Add(-5 * time.Second)),
				EndTime:        timestamppb.New(now),
				PrimaryBytes:   48000,
				PrimaryPackets: 250,
			},
		},
	})
	w.OnTrackStat("TR_audio_down", livekit.StreamType_DOWNSTREAM, &livekit.AnalyticsStat{
		Streams: []*livekit.AnalyticsStream{
			{
				StartTime:      timestamppb.New(now.Add(-5 * time.Second)),
				EndTime:        timestamppb.New(now),
				PrimaryBytes:   96000,
				PrimaryPackets: 500,
			},
		},
	})

	_ = w.Flush(now, time.Minute)
	require.True(t, w.Close(guard))
	ev, ok := w.DrainHiMarketEvent(true)
	require.True(t, ok)
	require.Equal(t, "sess_01", ev.SessionID)
	require.Equal(t, "dev_01", ev.TenantID)
	require.Equal(t, "ak_live_01", ev.APIKeyID)
	require.Equal(t, "livekit_sfu", ev.Source)
	require.Equal(t, "rtc", ev.Category)
	require.NotNil(t, ev.Metrics.RtcBytes)
	require.Equal(t, int64(144000), *ev.Metrics.RtcBytes)
	require.NotNil(t, ev.Metrics.RtcDurationMs)
	require.GreaterOrEqual(t, *ev.Metrics.RtcDurationMs, int64(14000))

	sink := &hiMarketMeteringSink{
		endpointURL:  srv.URL,
		sharedSecret: "test-secret",
		client:       srv.Client(),
	}
	sink.Send(context.Background(), ev)
	require.Len(t, received, 1)
	require.Equal(t, "lk_rtc:RM_1001:PA_2001:1", received[0].IdempotencyKey)
}
