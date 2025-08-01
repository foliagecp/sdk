package statefun

import (
	"fmt"
	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"strings"
	"sync"
	"time"
)

func (r *Runtime) ObjectCallRequest(sp sfPlugins.RequestProvider, lq sfPlugins.LinkQuery, ft, id string, payload, options *easyjson.JSON, timeout ...time.Duration) (map[string]*sfPlugins.ObjectRequestReply, error) {
	if ft == "" {
		return nil, fmt.Errorf("function typename is empty")
	}

	query := queryBuilder(lq)

	objectIDs, err := r.getObjectsByCTRA(query, id, payload, options)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*sfPlugins.ObjectRequestReply, len(objectIDs))

	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, objectID := range objectIDs {
		wg.Add(1)
		go func(oID string) {
			defer wg.Done()
			reply, err := r.Request(sp, ft, oID, payload, options, timeout...)
			mu.Lock()
			result[oID] = &sfPlugins.ObjectRequestReply{
				ReqReply: reply,
				ReqError: err,
			}
			mu.Unlock()
		}(objectID)
	}
	wg.Wait()

	return result, nil
}

func (r *Runtime) ObjectCallSignal(sp sfPlugins.SignalProvider, lq sfPlugins.LinkQuery, ft, id string, payload, options *easyjson.JSON) (map[string]error, error) {
	if ft == "" {
		return nil, fmt.Errorf("function typename is empty")
	}

	query := queryBuilder(lq)

	objectIDs, err := r.getObjectsByCTRA(query, id, payload, options)
	if err != nil {
		return nil, err
	}

	result := make(map[string]error, len(objectIDs))

	for _, objectID := range objectIDs {
		err := r.Signal(sp, ft, objectID, payload, options)
		result[objectID] = err
	}

	return result, nil
}

func (r *Runtime) getObjectsByCTRA(query, id string, payload, options *easyjson.JSON) ([]string, error) {
	if payload == nil {
		payload = easyjson.NewJSONObject().GetPtr()
	}
	payload.SetByPath("query", easyjson.NewJSON(query))
	reply, err := r.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", id, payload, options)
	if err != nil {
		return nil, err
	}
	return reply.GetByPath("data.uuids").ObjectKeys(), nil
}

func queryBuilder(query sfPlugins.LinkQuery) string {
	if query.GetCustom() != "" {
		return query.GetCustom()
	}

	qb := strings.Builder{}
	qb.WriteString(".")

	if lName := query.GetName(); lName != "" {
		qb.WriteString(lName)
	} else {
		qb.WriteString("*")
	}
	qb.WriteString(fmt.Sprintf("[l:type('%s')", query.GetType()))

	if len(query.GetTags()) > 0 {
		qb.WriteString(fmt.Sprintf(" && l:tags('%s')", strings.Join(query.GetTags(), ", ")))
	}

	qb.WriteString("]")

	return qb.String()
}
