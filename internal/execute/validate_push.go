// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

package execute

import (
	"fmt"

	"github.com/ondatra-labs/ondatrasql/internal/libregistry"
	"github.com/ondatra-labs/ondatrasql/internal/parser"
)

// ValidateModelPushCompat checks that model + sink configurations are compatible.
// Call after lib scanning and model parsing, before any execution.
func ValidateModelPushCompat(models []*parser.Model, reg *libregistry.Registry) error {
	for _, model := range models {
		if model.Push == "" {
			continue
		}
		if reg == nil || reg.Empty() {
			return fmt.Errorf("model %s: @sink %q requires lib/ directory with sink definitions, but no lib functions found", model.Target, model.Push)
		}
		sink := reg.Get(model.Push)
		if sink == nil {
			return fmt.Errorf("model %s: @sink %q not found in lib/", model.Target, model.Push)
		}
		if sink.PushConfig == nil {
			return fmt.Errorf("model %s: sink %q exists in lib/ but has no SINK dict", model.Target, model.Push)
		}
		cfg := sink.PushConfig

		// scd2 + @sink is not supported — use @kind: table with WHERE is_current = true
		if model.Kind == "scd2" {
			return fmt.Errorf("model %s: @sink is not supported for scd2 kind — use @kind: table with WHERE is_current = true to push current state", model.Target)
		}

		// Validate supported_kinds if declared
		if len(cfg.SupportedKinds) > 0 {
			allowed := false
			for _, k := range cfg.SupportedKinds {
				if k == model.Kind {
					allowed = true
					break
				}
			}
			if !allowed {
				return fmt.Errorf("model %s: sink %q does not support @kind: %s (supported: %v)", model.Target, model.Push, model.Kind, cfg.SupportedKinds)
			}
		}

		// merge + @sink requires @unique_key
		if model.Kind == "merge" && model.UniqueKey == "" {
			return fmt.Errorf("model %s: @kind: merge with @sink %q requires @unique_key for delta deduplication", model.Target, model.Push)
		}
		// tracked + @sink requires @group_key
		if model.Kind == "tracked" && model.GroupKey == "" {
			return fmt.Errorf("model %s: @kind: tracked with @sink %q requires @group_key for delta deduplication", model.Target, model.Push)
		}

		// poll_interval/poll_timeout only valid with async batch_mode
		if cfg.BatchMode != "async" && (cfg.PollInterval != "" || cfg.PollTimeout != "") {
			return fmt.Errorf("model %s: sink %q has poll_interval/poll_timeout but batch_mode is %q (only valid with async)", model.Target, model.Push, cfg.BatchMode)
		}

		// Validate poll_interval/poll_timeout format when set
		if cfg.PollInterval != "" {
			if _, err := parseDuration(cfg.PollInterval); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid poll_interval %q: %w", model.Target, model.Push, cfg.PollInterval, err)
			}
		}
		if cfg.PollTimeout != "" {
			if _, err := parseDuration(cfg.PollTimeout); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid poll_timeout %q: %w", model.Target, model.Push, cfg.PollTimeout, err)
			}
		}

		// Validate rate_limit format when set
		if cfg.RateLimit != nil {
			if _, err := parseDuration(cfg.RateLimit.Per); err != nil {
				return fmt.Errorf("model %s: sink %q has invalid rate_limit.per %q: %w", model.Target, model.Push, cfg.RateLimit.Per, err)
			}
		}

		// max_concurrent > 1 not allowed with atomic/async (all-or-nothing semantics)
		if cfg.MaxConcurrent > 1 && (cfg.BatchMode == "atomic" || cfg.BatchMode == "async") {
			return fmt.Errorf("model %s: sink %q has max_concurrent=%d but batch_mode %q forces sequential execution (max_concurrent=1)", model.Target, model.Push, cfg.MaxConcurrent, cfg.BatchMode)
		}
	}
	return nil
}
