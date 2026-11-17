package observability

import (
	"bytes"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// RenderMetrics expands the application metrics surface without removing its
// existing compatibility families. Only administrators receive unpartitioned
// server-wide families; other callers receive admitted database-labelled samples.
// The telemetry listener continues to serve its unchanged process-wide registry.
func RenderMetrics(reg *prometheus.Registry, now time.Time, scope *LegacyReadScope, administrator bool) ([]byte, error) {
	if reg == nil {
		return nil, nil
	}
	families, err := reg.Gather()
	if err != nil {
		return nil, err
	}
	aliases := make(map[string]bool, len(legacyMappings))
	for _, mapping := range legacyMappings {
		aliases[mapping.LegacyName] = true
	}
	var body bytes.Buffer
	for _, family := range families {
		if family == nil || aliases[family.GetName()] {
			continue
		}
		view := family
		if !administrator {
			// Filtering this view must not remove samples needed by the admitted
			// compatibility aggregates rendered from the original gathered families.
			copy := &dto.MetricFamily{Name: family.Name, Help: family.Help, Type: family.Type, Metric: append([]*dto.Metric(nil), family.Metric...)}
			if scope == nil {
				continue
			}
			view = legacyReadFamily(copy, scope, false)
		}
		if len(view.Metric) == 0 {
			continue
		}
		if _, err := expfmt.MetricFamilyToText(&body, view); err != nil {
			return nil, err
		}
	}
	body.Write(renderLegacyFamilies(families, scope))
	return body.Bytes(), nil
}
