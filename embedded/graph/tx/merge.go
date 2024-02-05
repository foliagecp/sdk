package tx

import (
	"fmt"
	"strings"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"

	"github.com/foliagecp/easyjson"
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

/*func withObjectsPolicy(policy objectsPolicy) mergerOpt {
	return func(m *merger) {
		m.uninitedObjectsPolicy = policy
	}
}*/

func withMode(mode string) mergerOpt {
	return func(m *merger) {
		m.mode = mode
	}
}

func newMerger(txID string, opts ...mergerOpt) *merger {
	m := &merger{
		debug:                 false,
		uninitedObjectsPolicy: allowCreation,
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
	lg.Logln(lg.DebugLevel, "Start merging", "tx", m.txID, "with mode", m.mode)
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
				lg.Logln(lg.DebugLevel, "Delete object", "id", normalID)

				deleted[normalID] = struct{}{}

				if err := deleteLowLevelObject(ctx, normalID); err != nil {
					return fmt.Errorf("delete main graph object %s: %w", normalID, err)
				}
			} else {
				lg.Logln(lg.DebugLevel, "Update object", "id", normalID)

				if err := updateLowLevelObject(ctx, m.mode, normalID, body); err != nil {
					return fmt.Errorf("update main graph object %s: %w", normalID, err)
				}
			}
		} else {
			// create
			lg.Logln(lg.DebugLevel, "Create object", "id", normalID)

			// uninited objects policy
			if _, err := ctx.GlobalCache.GetValue(normalID); err == nil {
				if m.uninitedObjectsPolicy == strict {
					lg.Logln(lg.WarnLevel, "Uninited object", "id", normalID)
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

		removeTxPrefix(prefix, body)

		if _, ok := main.links[normalID]; ok {
			// check for delete
			// otherwise, update
			if body.GetByPath("__meta.status").AsStringDefault("") == "deleted" {
				lg.Logln(lg.DebugLevel, "Delete link", "from", normalParent, "to", normalChild, "link_type", normalLt)

				if err := deleteLowLevelLink(ctx, normalParent, normalChild, normalLt); err != nil {
					return fmt.Errorf("delete main graph link %s: %w", normalID, err)
				}
			} else {
				lg.Logln(lg.DebugLevel, "Update link", "from", normalParent, "to", normalChild, "link_type", normalLt)

				if err := updateLowLevelLink(ctx, normalParent, normalChild, normalLt, *body); err != nil {
					return fmt.Errorf("update main link %s: %w", normalID, err)
				}
			}
		} else {
			// create
			lg.Logln(lg.DebugLevel, "Create link", "from", normalParent, "to", normalChild, "link_type", normalLt)

			if err := createLowLevelLink(ctx, normalParent, normalChild, normalLt, "", *body); err != nil {
				return fmt.Errorf("create main graph link %s: %w", normalID, err)
			}
		}
	}

	lg.Logln(lg.DebugLevel, "Merge Done!", "dt", time.Since(start))

	return nil
}

func removeTxPrefix(prefix string, linkBody *easyjson.JSON) {
	if tags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
		normalTags := make([]string, 0, len(tags))
		for _, v := range tags {
			normalTags = append(normalTags, strings.ReplaceAll(v, prefix, ""))
		}
		linkBody.SetByPath("tags", easyjson.JSONFromArray(normalTags))
	}

	if lt, ok := linkBody.GetByPath("link_type").AsString(); ok && strings.HasPrefix(lt, prefix) {
		linkBody.SetByPath("link_type", easyjson.NewJSON(strings.TrimPrefix(prefix, lt)))
	}
}
