package tx

import (
	"fmt"
	"strings"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

// merge v0
// TODO: add rollback
func merge(ctx *sfplugins.StatefunContextProcessor, txGraphID, mode string) error {
	fmt.Println("[INFO] Start merging", "tx", txGraphID, "with mode", mode)

	prefix := generatePrefix(txGraphID)

	txGraphRoot := prefix + BUILT_IN_ROOT

	main := graphState(ctx, BUILT_IN_ROOT)
	txGraph := graphState(ctx, txGraphRoot)
	deleted := make(map[string]struct{})

	for k := range txGraph.objects {
		body, err := ctx.GlobalCache.GetValueAsJSON(k)
		if err != nil {
			return fmt.Errorf("tx graph object %s not found: %w", k, err)
		}

		normalID := strings.TrimPrefix(k, prefix)

		if _, ok := main.objects[normalID]; ok {
			// check for delete
			// otherwise, update
			if body.GetByPath("__meta.status").AsStringDefault("") == "deleted" {
				deleted[normalID] = struct{}{}

				if err := deleteLowLevelObject(ctx, normalID); err != nil {
					return fmt.Errorf("delete main graph object %s: %w", normalID, err)
				}
			} else {
				if err := updateLowLevelObject(ctx, mode, normalID, body); err != nil {
					return fmt.Errorf("update main graph object %s: %w", normalID, err)
				}
			}
		} else {
			// create
			if err := createLowLevelObject(ctx, normalID, body); err != nil {
				return fmt.Errorf("create main graph object %s: %w", normalID, err)
			}
		}
	}

	for _, l := range txGraph.links {
		normalParent := strings.TrimPrefix(l.from, prefix)
		normalChild := strings.TrimPrefix(l.to, prefix)

		if _, ok := deleted[normalParent]; ok {
			continue
		}

		if _, ok := deleted[normalChild]; ok {
			continue
		}

		normalLt := strings.TrimPrefix(l.lt, prefix)
		normalID := normalParent + normalChild + normalLt

		body, err := ctx.GlobalCache.GetValueAsJSON(l.cacheID)
		if err != nil {
			return fmt.Errorf("tx graph link %s: %w", l.cacheID, err)
		}

		if _, ok := main.links[normalID]; ok {
			// check for delete
			// otherwise, update
			if body.GetByPath("__meta.status").AsStringDefault("") == "deleted" {
				if err := deleteLowLevelLink(ctx, normalParent, normalChild, normalLt); err != nil {
					return fmt.Errorf("delete main graph link %s: %w", normalID, err)
				}
			} else {
				if err := updateLowLevelLink(ctx, normalParent, normalChild, normalLt, *body); err != nil {
					return fmt.Errorf("update main link %s: %w", normalID, err)
				}
			}
		} else {
			// create
			if err := createLowLevelLink(ctx, normalParent, normalChild, normalLt, "", *body); err != nil {
				return fmt.Errorf("create main graph link %s: %w", normalID, err)
			}
		}
	}

	return nil
}
