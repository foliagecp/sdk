package tx

import (
	"container/list"
	"strings"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

type node struct {
	parent string
	id     string
	lt     string
}

func (n node) ID() string {
	return n.parent + n.id + n.lt
}

func (n node) NormalID(prefix string) string {
	return strings.TrimPrefix(n.parent, prefix) + strings.TrimPrefix(n.id, prefix) + strings.TrimPrefix(n.lt, prefix)
}

func treeToMap(ctx *sfplugins.StatefunContextProcessor, startPoint string) map[string]node {
	visited := make(map[string]node)

	root := node{
		id: startPoint,
	}

	queue := list.New()
	queue.PushBack(root)

	for queue.Len() > 0 {
		e := queue.Front()
		queue.Remove(e)

		node, ok := e.Value.(node)
		if !ok {
			continue
		}

		if _, exists := visited[node.ID()]; !exists {
			visited[node.ID()] = node
		}

		for _, n := range getChildren(ctx, node.id) {
			if _, ok := visited[n.ID()]; !ok {
				queue.PushBack(n)
			}
		}
	}

	delete(visited, root.ID())

	return visited
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
			parent: id,
			id:     split[len(split)-1],
			lt:     split[len(split)-2],
		})
	}

	return nodes
}
