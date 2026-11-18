package pigeonmath

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/db"
	"github.com/drewv-labs/carrier-pigeons/pkg/adapters/llm"
	"github.com/drewv-labs/carrier-pigeons/pkg/brand"
)

var fencedSQLBlockRe = regexp.MustCompile("(?is)```(?:sql)?\\s*(.*?)```")

const sqlOptimizerSystemPrompt = `You are PigeonCare acting as the PostgreSQL 18 analytical query optimizer for PigeonMath.
Schema reference (strictly READ ONLY on public tables):
- public.telemetric_ledger(id, node_id, pillar, component, status, event_timestamp, metrics JSONB, params JSONB, ingested_at, db_actor, app_actor)
  Indexes: (node_id, event_timestamp DESC), (pillar), (event_timestamp DESC), GIN(metrics)
- public.ctd_nodes(node_id, arch, os, status, boot_tags TEXT[], operator_tags TEXT[], tags TEXT[], last_seen, updated_at, updated_by, meta JSONB)
- public.ctd_artifacts(id, kind, node_id, meta JSONB, created_at, updated_at, updated_by)
- public.ctd_traces(id, source_id, verb, target_id, node_id, run_id, meta JSONB, event_timestamp, db_actor)

Rules:
1. Preserve the exact analytical intent and output column semantics of the raw SQL query.
2. Output ONLY a single read-only SELECT or WITH ... SELECT statement inside a fenced ` + "```sql" + ` block, followed optionally by one short sentence of optimization notes.
3. NEVER emit INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, COPY, or multi-statement SQL.`

const sqlPostCheckSystemPrompt = `You are PigeonCare performing post-creation certification of a PigeonMath analytical view.
Compare the original raw SQL intent, the materialized SQL, and the sample output rows.
Reply with "CERTIFIED: <one-sentence verification note>" if the view faithfully satisfies the raw intent without structural defects, or "REJECTED: <reason>" if broken.`

// Engine manages PigeonMath analytical view propagation (`pigeonmath.pmv_*`) and time-series rollups.
// It operates exclusively through db.AgentSafeDB so it can never mutate public CTD Ledger tables.
type Engine struct {
	store      db.AgentSafeDB
	mathSchema string
}

// NewEngine constructs a PigeonMath Engine bound to an AgentSafeDB handle.
func NewEngine(store db.AgentSafeDB) *Engine {
	schema := brand.Active().MathSchema
	if strings.TrimSpace(schema) == "" {
		schema = "pigeonmath"
	}
	return &Engine{
		store:      store,
		mathSchema: schema,
	}
}

func extractCandidateSQL(reply string) (string, string) {
	trimmed := strings.TrimSpace(reply)
	if m := fencedSQLBlockRe.FindStringSubmatch(trimmed); len(m) >= 2 {
		sqlPart := strings.TrimSpace(m[1])
		notes := strings.TrimSpace(fencedSQLBlockRe.ReplaceAllString(trimmed, ""))
		return sqlPart, notes
	}
	return trimmed, ""
}

// SubmitAndCertifyPMV executes the full PigeonMath View Propagation pipeline:
//  1. Cache raw SQL intent in `pigeonmath.pmv_catalog` (survives LLM context flushes & supports backtesting).
//  2. Optimize/tune SQL via PigeonCare (`optimizer`, optional if nil/offline).
//  3. Validate read-only SELECT safety & PostgreSQL planner validity (`EXPLAIN (FORMAT JSON)`).
//  4. Materialize `pigeonmath.pmv_<view_name>`.
//  5. Execute post-creation sample query + PigeonCare backtest check, then mark `certified`.
func (e *Engine) SubmitAndCertifyPMV(
	ctx context.Context,
	actor string,
	viewName string,
	rawSQL string,
	optimizer llm.BaseLLM,
) (*db.PMVRecord, error) {
	if e.store == nil {
		return nil, fmt.Errorf("pigeonmath store is not connected")
	}
	if err := e.store.EnsureMathSchema(ctx); err != nil {
		return nil, fmt.Errorf("ensure %s schema: %w", e.mathSchema, err)
	}

	normName, err := db.NormalizePMVName(viewName)
	if err != nil {
		return nil, err
	}
	cleanedRaw, err := db.ValidateSelectOnlySQL(rawSQL)
	if err != nil {
		return nil, fmt.Errorf("raw SQL rejected: %w", err)
	}
	if strings.TrimSpace(actor) == "" {
		actor = "operator"
	}

	// Step 1: Persist raw intent immediately in pmv_catalog before any LLM transformation.
	draft, err := e.store.SavePMVDraft(ctx, db.PMVRecord{
		ViewName:     normName,
		SubmittedBy:  actor,
		RawIntentSQL: cleanedRaw,
		Status:       "draft",
		ValidationReport: map[string]any{
			"stage":     "cached_raw_intent",
			"cached_at": time.Now().UTC().Format(time.RFC3339),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cache PMV raw intent: %w", err)
	}

	candidateSQL := cleanedRaw
	var notes []string
	usedOptimized := false

	// Step 2: Optional PigeonCare SQL optimization pass.
	if optimizer != nil {
		draft.Status = "optimizing"
		_ = e.store.UpdatePMVState(ctx, *draft)

		optCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		reply, optErr := optimizer.Chat(optCtx, sqlOptimizerSystemPrompt, []llm.Message{
			{Role: "user", Content: fmt.Sprintf("View name: %s\nRaw SQL intent:\n%s", normName, cleanedRaw)},
		})
		cancel()

		if optErr != nil {
			notes = append(notes, fmt.Sprintf("optimizer unavailable (%v); preserved raw SQL", optErr))
		} else {
			extractedSQL, optNote := extractCandidateSQL(reply)
			if validOpt, vErr := db.ValidateSelectOnlySQL(extractedSQL); vErr == nil {
				candidateSQL = validOpt
				usedOptimized = validOpt != cleanedRaw
				if optNote != "" {
					notes = append(notes, optNote)
				} else if usedOptimized {
					notes = append(notes, "SQL tuned by PigeonCare")
				}
			} else {
				notes = append(notes, fmt.Sprintf("discarded invalid optimizer SQL (%v); preserved raw SQL", vErr))
			}
		}
	}

	// Step 3: Structural & Read-Only Planner Verification via EXPLAIN.
	draft.Status = "validating"
	draft.OptimizedSQL = candidateSQL
	draft.AgentNotes = strings.Join(notes, " | ")
	_ = e.store.UpdatePMVState(ctx, *draft)

	explainPlan, explainErr := e.store.ExplainReadOnly(ctx, candidateSQL)
	if explainErr != nil && candidateSQL != cleanedRaw {
		// Fallback to original raw intent SQL if LLM rewrite broke planner semantics!
		notes = append(notes, fmt.Sprintf("optimizer SQL failed EXPLAIN (%v); reverted to raw intent SQL", explainErr))
		candidateSQL = cleanedRaw
		usedOptimized = false
		explainPlan, explainErr = e.store.ExplainReadOnly(ctx, candidateSQL)
	}
	if explainErr != nil {
		draft.Status = "failed"
		draft.OptimizedSQL = candidateSQL
		draft.AgentNotes = strings.Join(append(notes, "EXPLAIN failed: "+explainErr.Error()), " | ")
		draft.ValidationReport = map[string]any{
			"explain_ok": false,
			"error":      explainErr.Error(),
		}
		_ = e.store.UpdatePMVState(ctx, *draft)
		return draft, fmt.Errorf("SQL validation failed for %s: %w", normName, explainErr)
	}

	// Step 4: Materialize view strictly inside `pigeonmath` schema (`pigeonmath.pmv_*`).
	if err := e.store.MaterializePMV(ctx, normName, candidateSQL); err != nil {
		draft.Status = "failed"
		draft.AgentNotes = strings.Join(append(notes, "materialize failed: "+err.Error()), " | ")
		_ = e.store.UpdatePMVState(ctx, *draft)
		return draft, fmt.Errorf("materialize %s.%s: %w", e.mathSchema, normName, err)
	}

	// Step 5: Post-creation read-only sample probe & PigeonCare backtest check.
	probeQuery := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 5", e.mathSchema, normName)
	sampleRows, probeErr := e.store.QueryReadOnlyMaps(ctx, probeQuery)
	if probeErr != nil {
		draft.Status = "failed"
		draft.AgentNotes = strings.Join(append(notes, "post-creation probe failed: "+probeErr.Error()), " | ")
		_ = e.store.UpdatePMVState(ctx, *draft)
		return draft, fmt.Errorf("post-creation verification failed for %s.%s: %w", e.mathSchema, normName, probeErr)
	}

	postCheckVerdict := "structural & read-only probe passed"
	if optimizer != nil {
		sampleJSON, _ := json.Marshal(sampleRows)
		chkCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		chkReply, chkErr := optimizer.Chat(chkCtx, sqlPostCheckSystemPrompt, []llm.Message{
			{
				Role: "user",
				Content: fmt.Sprintf("View: %s.%s\nRaw Intent SQL:\n%s\n\nMaterialized SQL:\n%s\n\nSample Rows (%d):\n%s",
					e.mathSchema, normName, cleanedRaw, candidateSQL, len(sampleRows), string(sampleJSON)),
			},
		})
		cancel()
		if chkErr == nil && strings.TrimSpace(chkReply) != "" {
			trimmedVerdict := strings.TrimSpace(chkReply)
			postCheckVerdict = trimmedVerdict
			if strings.HasPrefix(strings.ToUpper(trimmedVerdict), "REJECTED:") {
				draft.Status = "failed"
				draft.OptimizedSQL = candidateSQL
				draft.AgentNotes = strings.Join(append(notes, trimmedVerdict), " | ")
				draft.ValidationReport = map[string]any{
					"explain_ok":        true,
					"post_check_passed": false,
					"verdict":           trimmedVerdict,
				}
				_ = e.store.UpdatePMVState(ctx, *draft)
				return draft, fmt.Errorf("post-creation certification rejected %s: %s", normName, trimmedVerdict)
			}
		}
	}

	notes = append(notes, postCheckVerdict)
	draft.OptimizedSQL = candidateSQL
	draft.Status = "certified"
	draft.AgentNotes = strings.Join(notes, " | ")
	draft.UpdatedAt = time.Now().UTC()
	draft.ValidationReport = map[string]any{
		"explain_ok":         true,
		"explain_plan_bytes": len(explainPlan),
		"used_optimized_sql": usedOptimized,
		"sample_row_count":   len(sampleRows),
		"verdict":            postCheckVerdict,
		"certified_at":       draft.UpdatedAt.Format(time.RFC3339),
	}

	if err := e.store.UpdatePMVState(ctx, *draft); err != nil {
		return draft, fmt.Errorf("save certified PMV state: %w", err)
	}
	return draft, nil
}

// GetPMV retrieves a cached/certified PMV including its original `raw_intent_sql`.
func (e *Engine) GetPMV(ctx context.Context, viewName string) (*db.PMVRecord, error) {
	return e.store.GetPMV(ctx, viewName)
}

// ListPMVs lists all tracked PigeonMath views ordered by recency.
func (e *Engine) ListPMVs(ctx context.Context) ([]db.PMVRecord, error) {
	return e.store.ListPMVs(ctx)
}

// QueryPMV previews rows from an existing `pigeonmath.pmv_<slug>` view inside a READ ONLY transaction.
func (e *Engine) QueryPMV(ctx context.Context, viewName string, limit int) ([]map[string]any, error) {
	normName, err := db.NormalizePMVName(viewName)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 200 {
		limit = 25
	}
	q := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d", e.mathSchema, normName, limit)
	return e.store.QueryReadOnlyMaps(ctx, q)
}
