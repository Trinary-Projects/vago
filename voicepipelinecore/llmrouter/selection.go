package llmrouter

import (
	"context"
	"sort"
)

// selection is the resolved endpoint choice for one turn.
type selection struct {
	ConfigKey     string
	UsingFallback bool
	SelectedGroup string
}

// getFastestForGroup mirrors LLMSwitchingService.get_fastest_for_group:
//
//   - score every region-matching config in the group by selection
//     latency (mean of last 5 adjusted poll latencies), skipping
//     blacklisted endpoints and those with no latency data;
//   - pick the fastest;
//   - if none is available and the group declares a fallback group, repeat
//     against that group (using_fallback);
//   - as a last resort return the selected group's hardcoded fallback config
//     so one-endpoint groups can still proceed without health data.
func getFastestForGroup(ctx context.Context, store RedisStore, group, region string) (selection, bool) {
	configs, ok := groupConfigsForRegion(group, region)
	if !ok || len(configs) == 0 {
		return selection{}, false
	}
	groupConfig := modelGroups[group]

	if key, ok := fastestAvailable(ctx, store, configs); ok {
		return selection{ConfigKey: key, UsingFallback: false, SelectedGroup: group}, true
	}

	if groupConfig.FallbackGroup != "" {
		fbConfigs, fbOK := groupConfigsForRegion(groupConfig.FallbackGroup, region)
		if fbOK && len(fbConfigs) > 0 {
			if key, ok := fastestAvailable(ctx, store, fbConfigs); ok {
				return selection{ConfigKey: key, UsingFallback: true, SelectedGroup: groupConfig.FallbackGroup}, true
			}
			if fb := modelGroups[groupConfig.FallbackGroup].Fallback; fb != "" {
				return selection{ConfigKey: fb, UsingFallback: true, SelectedGroup: groupConfig.FallbackGroup}, true
			}
		}
	}

	if fb := groupConfig.Fallback; fb != "" {
		return selection{ConfigKey: fb, UsingFallback: false, SelectedGroup: group}, true
	}
	return selection{}, false
}

// fastestAvailable returns the key with the lowest selection latency among
// the available configs (see rankAvailable).
func fastestAvailable(ctx context.Context, store RedisStore, configs []endpointConfig) (string, bool) {
	ranked := rankAvailable(ctx, store, configs)
	if len(ranked) == 0 {
		return "", false
	}
	return ranked[0], true
}

// rankAvailable reads the health of every config via one MGET, drops
// blacklisted endpoints and those with no latency data, and returns the
// remaining keys fastest first. A Redis error yields no ranking.
func rankAvailable(ctx context.Context, store RedisStore, configs []endpointConfig) []string {
	keys := make([]string, len(configs))
	for i, cfg := range configs {
		keys[i] = healthKey(cfg.Key)
	}
	raws, err := store.MGetCache(ctx, keys...)
	if err != nil || len(raws) != len(configs) {
		return nil
	}

	type candidate struct {
		key     string
		latency float64
	}
	candidates := make([]candidate, 0, len(configs))
	for i, cfg := range configs {
		health := parseHealth(raws[i])
		if health.Blacklisted {
			continue
		}
		latency, ok := health.selectionLatency()
		if !ok {
			continue
		}
		candidates = append(candidates, candidate{key: cfg.Key, latency: latency})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].latency < candidates[j].latency
	})
	ranked := make([]string, len(candidates))
	for i, c := range candidates {
		ranked[i] = c.key
	}
	return ranked
}
