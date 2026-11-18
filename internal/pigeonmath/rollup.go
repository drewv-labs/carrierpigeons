package pigeonmath

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
)

// BucketPoint holds 1-minute aggregated telemetry & fleet liveness counts for PigeonMath graphs.
type BucketPoint struct {
	Bucket         time.Time `json:"bucket"`
	TotalNodes     int       `json:"total_nodes"`
	LiveNodes      int       `json:"live_nodes"`
	QuietNodes     int       `json:"quiet_nodes"`
	ObserverEvents int       `json:"observer_events"`
	RunnerEvents   int       `json:"runner_events"`
	TracerEvents   int       `json:"tracer_events"`
	TotalEvents    int       `json:"total_events"`
}

// DashboardSnapshot aggregates time-series rollups, current fleet status, and PMV catalog records.
type DashboardSnapshot struct {
	CapturedAt    time.Time        `json:"captured_at"`
	WindowMinutes int              `json:"window_minutes"`
	Points        []BucketPoint    `json:"points"`
	Cluster       []db.NodeSummary `json:"cluster"`
	Views         []db.PMVRecord   `json:"views"`
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// FetchDashboardSnapshot computes 1-minute time-series buckets for `#nodes` (`total`, `live`, `quiet`)
// and `#CTD/min` (`observer`, `runner`, `tracer`, `total`) strictly via Read-Only ledger queries.
func (e *Engine) FetchDashboardSnapshot(ctx context.Context, windowMinutes int) (*DashboardSnapshot, error) {
	if windowMinutes <= 0 {
		windowMinutes = 30
	}
	if windowMinutes > 240 {
		windowMinutes = 240
	}

	now := time.Now().UTC().Truncate(time.Minute)
	start := now.Add(-time.Duration(windowMinutes-1) * time.Minute)

	cluster, err := e.store.GetClusterStatus(ctx, 90*time.Second)
	if err != nil {
		return nil, err
	}
	totalKnownNodes := len(cluster)

	query := fmt.Sprintf(`
		SELECT
			date_trunc('minute', event_timestamp) AS bucket,
			COUNT(DISTINCT node_id)::bigint AS live_nodes,
			COUNT(*) FILTER (WHERE pillar = 'observer')::bigint AS observer_events,
			COUNT(*) FILTER (WHERE pillar = 'runner')::bigint AS runner_events,
			COUNT(*) FILTER (WHERE pillar = 'tracer')::bigint AS tracer_events,
			COUNT(*)::bigint AS total_events
		FROM telemetric_ledger
		WHERE event_timestamp >= NOW() - INTERVAL '%d minutes'
		GROUP BY 1
		ORDER BY 1 ASC
	`, windowMinutes)

	rows, err := e.store.QueryReadOnlyMaps(ctx, query)
	if err != nil {
		return nil, err
	}

	byMinute := make(map[int64]BucketPoint, len(rows))
	for _, r := range rows {
		ts, ok := r["bucket"].(time.Time)
		if !ok {
			continue
		}
		bTime := ts.UTC().Truncate(time.Minute)
		live := asInt(r["live_nodes"])
		totNodes := max(totalKnownNodes, live)
		byMinute[bTime.Unix()] = BucketPoint{
			Bucket:         bTime,
			TotalNodes:     totNodes,
			LiveNodes:      live,
			QuietNodes:     max(0, totNodes-live),
			ObserverEvents: asInt(r["observer_events"]),
			RunnerEvents:   asInt(r["runner_events"]),
			TracerEvents:   asInt(r["tracer_events"]),
			TotalEvents:    asInt(r["total_events"]),
		}
	}

	points := make([]BucketPoint, 0, windowMinutes)
	for i := range windowMinutes {
		t := start.Add(time.Duration(i) * time.Minute)
		if p, ok := byMinute[t.Unix()]; ok {
			points = append(points, p)
		} else {
			points = append(points, BucketPoint{
				Bucket:     t,
				TotalNodes: totalKnownNodes,
				LiveNodes:  0,
				QuietNodes: totalKnownNodes,
			})
		}
	}

	views, _ := e.store.ListPMVs(ctx)

	return &DashboardSnapshot{
		CapturedAt:    time.Now().UTC(),
		WindowMinutes: windowMinutes,
		Points:        points,
		Cluster:       cluster,
		Views:         views,
	}, nil
}

var sparkGlyphs = []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

// Sparkline renders a compact single-line btop-style Unicode block sparkline scaled to `width`.
func Sparkline(values []float64, width int) string {
	if len(values) == 0 || width <= 0 {
		return ""
	}
	resampled := resampleSeries(values, width)
	maxVal := 0.0
	for _, v := range resampled {
		if v > maxVal {
			maxVal = v
		}
	}
	var sb strings.Builder
	for _, v := range resampled {
		if maxVal <= 0 || v <= 0 {
			sb.WriteRune('·')
			continue
		}
		idx := int(math.Round((v / maxVal) * float64(len(sparkGlyphs)-1)))
		idx = max(0, min(len(sparkGlyphs)-1, idx))
		sb.WriteRune(sparkGlyphs[idx])
	}
	return sb.String()
}

// RenderMultiLineGraph renders a multi-row btop-inspired area/line plot with Y-axis scale labels.
func RenderMultiLineGraph(values []float64, width, height int, unit string) string {
	if height < 2 {
		height = 4
	}
	if width < 10 {
		width = 32
	}
	plotWidth := max(8, width-10)
	series := resampleSeries(values, plotWidth)

	maxVal := 0.0
	for _, v := range series {
		if v > maxVal {
			maxVal = v
		}
	}
	ceilVal := maxVal
	if ceilVal <= 0 {
		ceilVal = 1
	}

	lines := make([]string, height)
	for row := range height {
		topThreshold := float64(height-row) / float64(height)
		botThreshold := float64(height-row-1) / float64(height)

		var rowBuf strings.Builder
		for _, v := range series {
			norm := v / ceilVal
			switch {
			case v <= 0 && row == height-1:
				rowBuf.WriteRune('·')
			case norm >= topThreshold:
				rowBuf.WriteRune('█')
			case norm > botThreshold:
				frac := (norm - botThreshold) / (topThreshold - botThreshold)
				idx := int(math.Round(frac * float64(len(sparkGlyphs)-1)))
				idx = max(0, min(len(sparkGlyphs)-1, idx))
				rowBuf.WriteRune(sparkGlyphs[idx])
			default:
				rowBuf.WriteRune(' ')
			}
		}

		var axisLabel string
		switch row {
		case 0:
			axisLabel = fmt.Sprintf("%6.0f%-2s", maxVal, unit)
		case height - 1:
			axisLabel = fmt.Sprintf("%6.0f%-2s", 0.0, unit)
		default:
			axisLabel = "        "
		}
		lines[row] = fmt.Sprintf("%s │%s", axisLabel, rowBuf.String())
	}

	return strings.Join(lines, "\n")
}

func resampleSeries(values []float64, width int) []float64 {
	if len(values) == width {
		return values
	}
	out := make([]float64, width)
	if len(values) == 0 {
		return out
	}
	if len(values) > width {
		// Take most recent `width` buckets so live right-hand edge stays crisp.
		return values[len(values)-width:]
	}
	offset := width - len(values)
	copy(out[offset:], values)
	return out
}
