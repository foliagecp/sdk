package tx

import (
	"log/slog"
	"strings"

	"github.com/foliagecp/easyjson"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

// merge v0
func merge(ctx *sfplugins.StatefunContextProcessor, txGraphID string) error {
	slog.Info("Start merging", "tx", txGraphID)

	prefix := generatePrefix(txGraphID)

	txGraphRoot := prefix + BUILT_IN_ROOT

	main := treeToMap(ctx, BUILT_IN_ROOT)
	txGraph := treeToMap(ctx, txGraphRoot)

	created := make(map[string]struct{})
	for _, n := range main {
		created[n.parent] = struct{}{}
		created[n.id] = struct{}{}
		created[n.NormalID(prefix)] = struct{}{}
	}

	update := make(map[string]node)
	new := make(map[string]node)

	for _, n := range txGraph {
		normalID := n.NormalID(prefix)
		if _, ok := main[normalID]; ok {
			update[normalID] = n
			continue
		}

		new[normalID] = n
	}

	// create new elements
	for _, n := range new {
		// create parent if need
		normalParentID := strings.TrimPrefix(n.parent, prefix)
		if _, ok := created[normalParentID]; !ok {
			body, err := ctx.GlobalCache.GetValueAsJSON(n.parent)
			if err != nil {
				return err
			}

			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)

			// TODO: use high level api?
			if _, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", normalParentID, &payload, nil); err != nil {
				return err
			}

			created[normalParentID] = struct{}{}
		} else {
			update[normalParentID] = n
		}

		// create child if need
		normalChildID := strings.TrimPrefix(n.id, prefix)
		if _, ok := created[normalChildID]; !ok {
			body, err := ctx.GlobalCache.GetValueAsJSON(n.id)
			if err != nil {
				return err
			}

			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)

			// TODO: use high level api?
			if _, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", normalChildID, &payload, nil); err != nil {
				return err
			}

			created[normalChildID] = struct{}{}
		} else {
			update[normalChildID] = n
		}

		// create link if need
		normalLinkTypeID := n.NormalID(prefix)
		if _, ok := created[normalLinkTypeID]; !ok {
			err := createLowLevelLink(ctx, normalParentID, normalChildID, strings.TrimPrefix(n.lt, prefix), "", easyjson.NewJSONObject())
			if err != nil {
				return err
			}

			created[normalLinkTypeID] = struct{}{}
		}
	}

	return nil
}
