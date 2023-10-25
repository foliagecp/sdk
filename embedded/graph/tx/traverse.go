package tx

import (
	"container/list"
	"strings"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

type state struct {
	objects map[string]struct{}
	links   map[string]link
}

type node struct {
	id     string
	lt     string
	linkID string
}

type link struct {
	from, to, lt, linkID string
}

func graphState(ctx *sfplugins.StatefunContextProcessor, startPoint string) *state {
	state := &state{
		objects: make(map[string]struct{}),
		links:   make(map[string]link),
	}

	root := startPoint

	queue := list.New()
	queue.PushBack(root)

	for queue.Len() > 0 {
		e := queue.Front()
		queue.Remove(e)

		node, ok := e.Value.(string)
		if !ok {
			continue
		}

		if _, exists := state.objects[node]; !exists {
			state.objects[node] = struct{}{}
		}

		for _, ch := range getChildren(ctx, node) {
			linkID := node + ch.id + ch.lt

			if _, ok := state.links[linkID]; !ok {
				state.links[linkID] = link{
					from:   node,
					to:     ch.id,
					lt:     ch.lt,
					linkID: ch.linkID,
				}
			}

			if _, ok := state.objects[ch.id]; !ok {
				queue.PushBack(ch.id)
			}
		}
	}

	return state
}

func getChildren(ctx *sfplugins.StatefunContextProcessor, id string) []node {
	pattern := id + ".out.ltp_oid-bdy.>"
	children := ctx.GlobalCache.GetKeysByPattern(pattern)

	nodes := make([]node, 0, len(children))

	for _, v := range children {
		split := strings.Split(v, ".")
		if len(split) == 0 {
			continue
		}

		nodes = append(nodes, node{
			id:     split[len(split)-1],
			lt:     split[len(split)-2],
			linkID: v,
		})
	}

	return nodes
}
