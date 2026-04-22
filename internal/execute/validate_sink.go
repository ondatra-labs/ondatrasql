// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// ValidateModelSinkCompat checks that model + sink configurations are compatible.
// Call after lib scanning and model parsing, before any execution.
func ValidateModelSinkCompat(models []*parser.Model, reg *libregistry.Registry) error {
	for _, model := range models {
		if model.Sink == "" {
			continue
		}
		if reg == nil || reg.Empty() {
			return fmt.Errorf("model %s: @sink %q requires lib/ directory with sink definitions, but no lib functions found", model.Target, model.Sink)
		}
		sink := reg.Get(model.Sink)
		if sink == nil {
			return fmt.Errorf("model %s: @sink %q not found in lib/", model.Target, model.Sink)
		}
		if sink.SinkConfig == nil {
			return fmt.Errorf("model %s: sink %q exists in lib/ but has no SINK dict", model.Target, model.Sink)
		}
		cfg := sink.SinkConfig

		// merge/tracked + @sink requires @unique_key (needed by materialization)
		if (model.Kind == "merge" || model.Kind == "tracked") && model.UniqueKey == "" {
			return fmt.Errorf("model %s: @kind: %s with @sink %q requires @unique_key for delta deduplication", model.Target, model.Kind, model.Sink)
		}

		// poll_interval/poll_timeout only valid with async batch_mode
		if cfg.BatchMode != "async" && (cfg.PollInterval != "" || cfg.PollTimeout != "") {
			return fmt.Errorf("model %s: sink %q has poll_interval/poll_timeout but batch_mode is %q (only valid with async)", model.Target, model.Sink, cfg.BatchMode)
		}

		// Validate poll_interval/poll_timeout format when set
		if cfg.PollInterval != "" {
			if _, err := parseDuration(cfg.PollInterval); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid poll_interval %q: %w", model.Target, model.Sink, cfg.PollInterval, err)
			}
		}
		if cfg.PollTimeout != "" {
			if _, err := parseDuration(cfg.PollTimeout); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid poll_timeout %q: %w", model.Target, model.Sink, cfg.PollTimeout, err)
			}
		}

		// Validate rate_limit format when set
		if cfg.RateLimit != nil {
			if _, err := parseDuration(cfg.RateLimit.Per); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid rate_limit.per %q: %w", model.Target, model.Sink, cfg.RateLimit.Per, err)
			}
		}

		// max_concurrent > 1 not allowed with atomic/async (all-or-nothing semantics)
		if cfg.MaxConcurrent > 1 && (cfg.BatchMode == "atomic" || cfg.BatchMode == "async") {
			return fmt.Errorf("model %s: sink %q has max_concurrent=%d but batch_mode %q forces sequential execution (max_concurrent=1)", model.Target, model.Sink, cfg.MaxConcurrent, cfg.BatchMode)
		}
	}
	return nil
}
