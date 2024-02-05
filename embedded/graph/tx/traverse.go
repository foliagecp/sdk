package tx

import (
	"container/list"
	"fmt"
	"strings"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

type state struct {
	objects map[string]struct{} // low level objects
	links   map[string]link     // low level links
}

/*func (s state) isEmpty() bool {
	return len(s.objects) == 0 && len(s.links) == 0
}*/

func (s state) initBuiltIn() {
	s.objects[BUILT_IN_ROOT] = struct{}{}
	s.objects[BUILT_IN_OBJECTS] = struct{}{}
	s.objects[BUILT_IN_TYPES] = struct{}{}

	// create root -> objects link
	s.links[BUILT_IN_ROOT+BUILT_IN_OBJECTS+OBJECTS_TYPELINK] = link{
		from: BUILT_IN_ROOT,
		to:   BUILT_IN_OBJECTS,
		lt:   OBJECTS_TYPELINK,
	}

	// create root -> types link
	s.links[BUILT_IN_ROOT+BUILT_IN_TYPES+TYPES_TYPELINK] = link{
		from: BUILT_IN_ROOT,
		to:   BUILT_IN_TYPES,
		lt:   TYPES_TYPELINK,
	}

	s.objects[BUILT_IN_GROUP] = struct{}{}

	// create types -> group link
	s.links[BUILT_IN_TYPES+BUILT_IN_GROUP+TYPE_TYPELINK] = link{
		from: BUILT_IN_TYPES,
		to:   BUILT_IN_GROUP,
		lt:   TYPE_TYPELINK,
	}

	// link from group -> group, need for define "group" link type
	s.links[BUILT_IN_GROUP+BUILT_IN_GROUP+TYPE_TYPELINK] = link{
		from:     BUILT_IN_GROUP,
		to:       BUILT_IN_GROUP,
		lt:       TYPE_TYPELINK,
		objectLt: GROUP_TYPELINK,
	}

	s.objects[BUILT_IN_NAV] = struct{}{}

	// create objects -> nav link
	s.links[BUILT_IN_OBJECTS+BUILT_IN_NAV+OBJECT_TYPELINK] = link{
		from: BUILT_IN_OBJECTS,
		to:   BUILT_IN_NAV,
		lt:   OBJECT_TYPELINK,
	}

	// create nav -> group link
	s.links[BUILT_IN_NAV+BUILT_IN_GROUP+TYPE_TYPELINK] = link{
		from: BUILT_IN_NAV,
		to:   BUILT_IN_GROUP,
		lt:   TYPE_TYPELINK,
	}

	// create group -> nav link
	s.links[BUILT_IN_GROUP+BUILT_IN_NAV+OBJECT_TYPELINK] = link{
		from: BUILT_IN_GROUP,
		to:   BUILT_IN_NAV,
		lt:   OBJECT_TYPELINK,
	}
}

type node struct {
	id     string
	lt     string
	linkID string
}

type link struct {
	from, to, lt, objectLt, cacheID string
}

func graphState(ctx *sfplugins.StatefunContextProcessor, startPoint string) *state {
	state := &state{
		objects: make(map[string]struct{}),
		links:   make(map[string]link),
	}

	if _, err := ctx.GlobalCache.GetValue(startPoint); err != nil {
		return state
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
					from:    node,
					to:      ch.id,
					lt:      ch.lt,
					cacheID: ch.linkID,
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
	pattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, id, ">")
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
