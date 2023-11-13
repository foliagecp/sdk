package tx

import (
	"fmt"
	"strings"
	"time"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

type objectsPolicy int8

const (
	strict objectsPolicy = iota
	allowCreation
)

type mergerOpt func(m *merger)

// TODO: add logger with need levels
type merger struct {
	debug                 bool
	uninitedObjectsPolicy objectsPolicy
	txID                  string
	mode                  string
}

func withDebug() mergerOpt {
	return func(m *merger) {
		m.debug = true
	}
}

func withObjectsPolicy(policy objectsPolicy) mergerOpt {
	return func(m *merger) {
		m.uninitedObjectsPolicy = policy
	}
}

func withMode(mode string) mergerOpt {
	return func(m *merger) {
		m.mode = mode
	}
}

func newMerger(txID string, opts ...mergerOpt) *merger {
	m := &merger{
		debug:                 false,
		uninitedObjectsPolicy: strict,
		txID:                  txID,
		mode:                  "merge",
	}

	for _, opt := range opts {
		opt(m)
	}

	return m
}

// merge v0
// TODO: add rollback
func (m *merger) Merge(ctx *sfplugins.StatefunContextProcessor) error {
	fmt.Println("[INFO] Start merging", "tx", m.txID, "with mode", m.mode)
	start := time.Now()

	prefix := generatePrefix(m.txID)

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
				if m.debug {
					fmt.Println("[DEBUG] Delete object", "id", normalID)
				}

				deleted[normalID] = struct{}{}

				if err := deleteLowLevelObject(ctx, normalID); err != nil {
					return fmt.Errorf("delete main graph object %s: %w", normalID, err)
				}
			} else {
				if m.debug {
					fmt.Println("[DEBUG] Update object", "id", normalID)
				}

				if err := updateLowLevelObject(ctx, m.mode, normalID, body); err != nil {
					return fmt.Errorf("update main graph object %s: %w", normalID, err)
				}
			}
		} else {
			// create
			if m.debug {
				fmt.Println("[DEBUG] Create object", "id", normalID)
			}

			// uninited objects policy
			if _, err := ctx.GlobalCache.GetValue(normalID); err == nil {
				if m.uninitedObjectsPolicy == strict {
					fmt.Println("[WARNING] Uninited object", "id", normalID)
					return fmt.Errorf("uninited object: %s", normalID)
				}
			}

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
				if m.debug {
					fmt.Println("[DEBUG] Delete link", "from", normalParent, "to", normalChild, "link_type", normalLt)
				}

				if err := deleteLowLevelLink(ctx, normalParent, normalChild, normalLt); err != nil {
					return fmt.Errorf("delete main graph link %s: %w", normalID, err)
				}
			} else {
				if m.debug {
					fmt.Println("[DEBUG] Update link", "from", normalParent, "to", normalChild, "link_type", normalLt)
				}

				if err := updateLowLevelLink(ctx, normalParent, normalChild, normalLt, *body); err != nil {
					return fmt.Errorf("update main link %s: %w", normalID, err)
				}
			}
		} else {
			// create
			if m.debug {
				fmt.Println("[DEBUG] Create link", "from", normalParent, "to", normalChild, "link_type", normalLt)
			}

			if err := createLowLevelLink(ctx, normalParent, normalChild, normalLt, "", *body); err != nil {
				return fmt.Errorf("create main graph link %s: %w", normalID, err)
			}
		}
	}

	fmt.Println("[INFO] Merge Done!", "dt", time.Since(start))

	return nil
}
