

package statefun

import (
	"context"
	"fmt"
	"json_easy"
	"sync/atomic"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type Runtime struct {
	config     *RuntimeConfig
	nc         *nats.Conn
	js         nats.JetStreamContext
	kv         nats.KeyValue
	cacheStore *cache.CacheStore

	registeredFunctionTypes map[string]*FunctionType

	__gt0  int64 // Global time 0 - time of the very first message receving by any function type
	__glce int64 // Global last call ended - time of last call of last function handling id of any function type
	__gc   int64 // Global counter - max total id handlers for all function types
}

func NewRuntime(config *RuntimeConfig) (r *Runtime, err error) {
	r = &Runtime{config: config, registeredFunctionTypes: make(map[string]*FunctionType)}

	r.nc, err = nats.Connect(config.natsURL)
	if err != nil {
		return
	}

	r.js, err = r.nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return
	}

	// Create application key value store bucket if does not exist --
	kvExists := false
	for name := range r.js.KeyValueStoreNames() {
		if name == "KV_"+config.keyValueStoreBucketName {
			r.kv, err = r.js.KeyValue(config.keyValueStoreBucketName)
			if err != nil {
				return
			}
			kvExists = true
		}
	}
	if !kvExists {
		r.kv, err = r.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket: config.keyValueStoreBucketName,
		})
		if err != nil {
			return
		}
		kvExists = true
	}
	if !kvExists {
		err = fmt.Errorf("Nats KV was not inited!")
		return
	}
	// --------------------------------------------------------------

	return
}

func (r *Runtime) Start(cacheConfig *cache.CacheConfig, onAfterStart func(runtime *Runtime)) (err error) {
	// Create stream if does not exist ------------------------------
	streamExists := false
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for info := range r.js.StreamsInfo(nats.Context(ctx)) {
		if info.Config.Name == r.config.functionTypesStreamName {
			streamExists = true
			break
		}
	}
	if !streamExists {
		var subjects []string
		for _, function_type := range r.registeredFunctionTypes {
			subjects = append(subjects, function_type.subject)
		}
		r.js.AddStream(&nats.StreamConfig{
			Name:     r.config.functionTypesStreamName,
			Subjects: subjects,
		})
	}
	// --------------------------------------------------------------

	fmt.Println("Initializing the cache store...")
	r.cacheStore = cache.NewCacheStore(cacheConfig, context.Background(), r.kv)
	fmt.Println("Cache store inited!")

	// Start function subscriptions ---------------------------------
	for _, ft := range r.registeredFunctionTypes {
		ft.Start(r.config.functionTypesStreamName)
	}
	// --------------------------------------------------------------

	onAfterStart(r)
	r.runGarbageCellector()

	return
}

func (r *Runtime) runGarbageCellector() (err error) {
	for {
		// Start function subscriptions ---------------------------------
		var totalIdsGrbageCollected int
		var totalIdHandlersRunning int
		for _, ft := range r.registeredFunctionTypes {
			n1, n2 := ft.gc(r.config.functionTypeIdLifetimeMs)
			totalIdsGrbageCollected += n1
			totalIdHandlersRunning += n2
		}
		if totalIdsGrbageCollected > 0 && totalIdHandlersRunning == 0 {
			// Result time output -----------------------------------------------------------------
			if totalIdHandlersRunning == 0 {
				glce := atomic.LoadInt64(&r.__glce)
				gt0 := atomic.LoadInt64(&r.__gt0)
				gc := atomic.LoadInt64(&r.__gc)

				dt := glce - gt0

				if gc > 0 && dt > 0 {
					fmt.Printf("!!!!!!!!!!!!!!!!! %d runs, total time (ns/ms): %d/%d, function dt (ns/ms): %d/%d -> %dHz\n", gc, dt, dt/1000000, dt/gc, dt/gc/1000000, gc*1000000000/dt)
					atomic.StoreInt64(&r.__gc, 0)
				}
				// ------------------------------------------------------------------------------------
			}
			// --------------------------------------------------------------
			time.Sleep(1 * time.Second)
		}
	}
}

func (r *Runtime) IngressNATS(typename string, id string, payload *json_easy.JSON, options *json_easy.JSON) {
	r.callFunction("ingress", "nats", typename, id, payload, options)
}

func (r *Runtime) IngressGolangSync(typename string, id string, payload *json_easy.JSON, options *json_easy.JSON) *json_easy.JSON {
	return r.callFunctionGolangSync("ingress", "go", typename, id, payload, options)
}

func (r *Runtime) callFunction(callerTypename string, callerId string, targetTypename string, targetId string, payload *json_easy.JSON, options *json_easy.JSON) {
	data := json_easy.NewJSONObject()
	data.SetByPath("caller_typename", json_easy.NewJSON(callerTypename))
	data.SetByPath("caller_id", json_easy.NewJSON(callerId))
	data.SetByPath("payload", *payload)
	data.SetByPath("options", *options)
	go r.nc.Publish(targetTypename+"."+targetId, data.ToBytes())
}

func (r *Runtime) callFunctionGolangSync(callerTypename string, callerId string, targetTypename string, targetId string, payload *json_easy.JSON, options *json_easy.JSON) *json_easy.JSON {
	resultJSONChannel := make(chan *json_easy.JSON, 1)

	msg := &GoMsg{ResultJSONChannel: resultJSONChannel, Caller: &sfPlugins.StatefunAddress{Typename: callerTypename, ID: callerId}, Payload: payload}
	if targetFT, ok := r.registeredFunctionTypes[targetTypename]; ok {
		targetFT.sendMsgToIdHandler(targetId, msg, nil)
	} else {
		return nil
	}

	select {
	case resultJSON := <-resultJSONChannel:
		return resultJSON
	case <-time.After(time.Duration(r.config.ingressCallGoLangSyncTimeoutSec) * time.Second):
		return nil
	}
}
