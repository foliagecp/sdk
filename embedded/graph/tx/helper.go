package tx

import (
	"github.com/foliagecp/easyjson"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

func createLowLevelLink(ctx *sfplugins.StatefunContextProcessor, from, to, lt, objectLt string) error {
	const op = "functions.graph.ll.api.link.create"
	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(lt))
	link.SetByPath("link_body", easyjson.NewJSONObject())
	if objectLt != "" {
		link.SetByPath("link_body.link_type", easyjson.NewJSON(objectLt))
	}

	if _, err := ctx.GolangCallSync(op, from, &link, nil); err != nil {
		return err
	}

	return nil
}
