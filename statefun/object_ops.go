package statefun

import (
	"context"
	"fmt"
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"strings"
	"sync"
)

func ObjectCall(ctx *sfPlugins.StatefunContextProcessor, lq LinkQuery, ft string, payload *easyjson.JSON, options *easyjson.JSON) error {
	le := lg.GetLogger()

	id := ctx.Self.ID

	reply, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", ctx.Self.ID, payload, options)
	if err != nil {
		le.Errorf(context.TODO(), "cant read object: %v", err)
		return err
	}
	if reply.GetByPath("status").AsStringDefault("") != "ok" {
		if ctx.Reply != nil {
			ctx.Reply.With(reply)
		}
		return fmt.Errorf("cant read object: %v", reply.GetByPath("details").AsStringDefault("empty"))
	}

	if err = lq.Validate(); err != nil {
		return err
	}

	query := queryBuilder(lq)

	if payload == nil {
		payload = easyjson.NewJSONObject().GetPtr()
	}

	payload.SetByPath("query", easyjson.NewJSON(query))

	reply, err = ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", id, payload, options)
	if err != nil {
		le.Debugf(context.TODO(), "ObjectSignal: %s", err.Error())
		return err
	}
	if reply == nil {
		le.Errorf(context.TODO(), "ObjectSignal: reply is nil")
		return fmt.Errorf("reply is nil")
	}

	objectIDs := reply.GetByPath("data.uuids").ObjectKeys()

	switch ctx.Reply {
	case nil:
		for _, oID := range objectIDs {
			system.MsgOnErrorReturn(ctx.Signal(sfPlugins.AutoSignalSelect, ft, oID, payload, options))
		}
	default:
		returnData := easyjson.NewJSONObject().GetPtr()

		result := make(map[string]error, len(objectIDs))

		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, objectID := range objectIDs {
			wg.Add(1)
			go func(oID string) {
				defer wg.Done()
				_, err := ctx.Request(sfPlugins.AutoRequestSelect, ft, oID, payload, options)
				mu.Lock()
				result[oID] = err
				mu.Unlock()
			}(objectID)
		}
		wg.Wait()

		returnData.SetByPath("result", easyjson.NewJSON(result))

		ctx.Reply.With(returnData)
	}

	return nil
}

func queryBuilder(query LinkQuery) string {
	qb := strings.Builder{}
	qb.WriteString(".")

	if lName := query.GetName(); lName != "" {
		qb.WriteString(fmt.Sprintf("%s", lName))
	} else {
		qb.WriteString("*")
	}
	qb.WriteString(fmt.Sprintf("[l:type('%s')", query.GetType()))

	if len(query.GetTags()) > 0 {
		tagsList := make([]string, len(query.GetTags()))
		for i, tag := range query.GetTags() {
			tagsList[i] = fmt.Sprintf("%s", tag)
		}
		qb.WriteString(fmt.Sprintf(" && l:tags('%s')", strings.Join(tagsList, ", ")))
	}

	qb.WriteString("]")

	return qb.String()
}

type LinkQuery struct {
	linkType string                 //link type
	name     string                 //link name
	tags     []string               //link tags
	filter   map[string]interface{} //objects filter
}

func NewLinkQuery(lt string) LinkQuery {
	return LinkQuery{
		linkType: lt,
	}
}

func (lq LinkQuery) WithName(name string) LinkQuery {
	lq.name = name
	return lq
}

func (lq LinkQuery) WithTags(tags ...string) LinkQuery {
	lq.tags = tags
	return lq
}

func (lq LinkQuery) WithFilter(filter map[string]interface{}) LinkQuery {
	lq.filter = filter
	return lq
}

func (lq LinkQuery) GetType() string {
	return lq.linkType
}

func (lq LinkQuery) GetName() string {
	if lq.name == "" {
		return "*"
	}
	return lq.name
}

func (lq LinkQuery) GetTags() []string {
	if lq.tags == nil || len(lq.tags) == 0 {
		return []string{}
	}
	return lq.tags
}

func (lq LinkQuery) GetFilter() map[string]interface{} {
	if lq.filter == nil && len(lq.filter) == 0 {
		return map[string]interface{}{}
	}
	return lq.filter
}

func (lq LinkQuery) Validate() error {
	if lq.linkType == "" {
		return fmt.Errorf("link type is empty")
	}
	return nil
}
