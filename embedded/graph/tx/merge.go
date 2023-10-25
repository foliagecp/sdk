package tx

import (
	"log/slog"
	"strings"

	"github.com/foliagecp/easyjson"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

// merge v0
// TODO: add rollback
func merge(ctx *sfplugins.StatefunContextProcessor, txGraphID string) error {
	slog.Info("Start merging", "tx", txGraphID)

	prefix := generatePrefix(txGraphID)

	txGraphRoot := prefix + BUILT_IN_ROOT

	main := graphState(ctx, BUILT_IN_ROOT)
	txGraph := graphState(ctx, txGraphRoot)

	for k := range txGraph.objects {
		normalID := strings.TrimPrefix(k, prefix)

		body, err := ctx.GlobalCache.GetValueAsJSON(k)
		if err != nil {
			return err
		}

		if _, ok := main.objects[normalID]; ok {
			// check for delete
			// otherwise, update
			slog.Info("update object", "id", normalID)
			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)
			// TODO: use high level api?
			if err := updateLowLevelObject(ctx, normalID, &payload); err != nil {
				return err
			}
		} else {
			// create
			slog.Info("create object", "id", normalID)
			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)
			// TODO: use high level api?
			if err := createLowLevelObject(ctx, normalID, &payload); err != nil {
				return err
			}
		}
	}

	for _, l := range txGraph.links {
		normalParent := strings.TrimPrefix(l.from, prefix)
		normalChild := strings.TrimPrefix(l.to, prefix)
		normalLt := strings.TrimPrefix(l.lt, prefix)

		normalID := normalParent + normalChild + normalLt

		body, err := ctx.GlobalCache.GetValueAsJSON(l.linkID)
		if err != nil {
			return err
		}

		if _, ok := main.links[normalID]; ok {
			// check for delete
			// otherwise, update
			slog.Info("update link", "id", normalID)

			if err := updateLowLevelLink(ctx, normalParent, normalChild, normalLt, *body); err != nil {
				return err
			}
		} else {
			// create
			slog.Info("create link", "id", normalID)

			if err := createLowLevelLink(ctx, normalParent, normalChild, normalLt, "", *body); err != nil {
				return err
			}
		}
	}

	return nil
}
