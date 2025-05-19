package system

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

var (
	PrometricDifferentTypeExistsForIdError = errors.New("metrics with a different type exists for an id")
	PrometricInstanceIsNil                 = errors.New("prometrics instance the method is being call against to is nil")
)

type Prometrics struct {
	metricsMutex    *sync.Mutex
	metrics         map[string]any
	routinesCounter *RoutinesCounter
	cancelFunc      context.CancelFunc
}

func NewPrometrics(pattern string, addr string) *Prometrics {
	ctx, cancel := context.WithCancel(context.Background())
	pm := &Prometrics{
		metricsMutex:    &sync.Mutex{},
		metrics:         map[string]any{},
		routinesCounter: &RoutinesCounter{},
		cancelFunc:      cancel,
	}

	// Run HTTP-server with a separate ServeMux
	go func() {
		pm.GetRoutinesCounter().Started("prometrics-server")
		defer pm.GetRoutinesCounter().Stopped("prometrics-server")

		if len(pattern) == 0 {
			pattern = "/"
		}

		mux := http.NewServeMux()
		mux.Handle(pattern, promhttp.Handler())
		server := &http.Server{
			Addr:    addr,
			Handler: mux,
		}

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			lg.Logln(lg.FatalLevel, err.Error())
		}
	}()

	go pm.golangRuntimeStatsCollector(ctx)

	return pm
}

func (pm *Prometrics) Shutdown() {
	if pm.cancelFunc != nil {
		pm.cancelFunc()
	}
}

func (pm *Prometrics) golangRuntimeStatsCollector(ctx context.Context) {
	pm.GetRoutinesCounter().Started("r.statsGolangStatsCollector")
	defer pm.GetRoutinesCounter().Stopped("r.statsGolangStatsCollector")

	if pm == nil {
		return
	}

	mem := &runtime.MemStats{}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runtime.ReadMemStats(mem)

			if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_mem_alloc_bytes", "", []string{}); err == nil {
				gaugeVec.With(prometheus.Labels{}).Set(float64(mem.Alloc))
			}
			if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_routines_counter", "", []string{}); err == nil {
				gaugeVec.With(prometheus.Labels{}).Set(float64(runtime.NumGoroutine()))
			}

			pm.GetRoutinesCounter().Read(func(key string, counter int64) bool {
				if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_routines", "", []string{"routine_type_name"}); err == nil {
					gaugeVec.With(prometheus.Labels{"routine_type_name": key}).Set(float64(counter))
				}
				return true
			})
		}
	}
}

func (pm *Prometrics) GetRoutinesCounter() *RoutinesCounter {
	if pm == nil {
		return nil
	}
	return pm.routinesCounter
}

func (pm *Prometrics) Exists(id string) bool {
	if pm == nil {
		return false
	}
	pm.metricsMutex.Lock()
	defer pm.metricsMutex.Unlock()
	_, ok := pm.metrics[id]
	return ok
}

// GaugeVec ---------------------------------------------------------------------------------------

func (pm *Prometrics) EnsureGaugeVecSimple(id string, help string, labelNames []string) (*prometheus.GaugeVec, error) {
	if pm == nil {
		return nil, PrometricInstanceIsNil
	}
	name := strings.ReplaceAll(id, ".", "")
	metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	}, labelNames)
	return pm.EnsureGaugeVec(id, metric)
}

func (pm *Prometrics) EnsureGaugeVec(id string, metric *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
	if pm == nil {
		return nil, PrometricInstanceIsNil
	}
	pm.metricsMutex.Lock()
	defer pm.metricsMutex.Unlock()

	if existing, ok := pm.metrics[id]; ok {
		if gaugeVec, ok := existing.(*prometheus.GaugeVec); ok {
			return gaugeVec, nil
		}
		return nil, PrometricDifferentTypeExistsForIdError
	}

	if err := prometheus.Register(metric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			existingMetric := are.ExistingCollector.(*prometheus.GaugeVec)
			pm.metrics[id] = existingMetric
			return existingMetric, nil
		}
		return nil, err
	}

	pm.metrics[id] = metric
	return metric, nil
}

// HistogramVec -----------------------------------------------------------------------------------

func (pm *Prometrics) EnsureHistogramVecSimple(id string, help string, buckets []float64, labelNames []string) (*prometheus.HistogramVec, error) {
	if pm == nil {
		return nil, PrometricInstanceIsNil
	}
	name := strings.ReplaceAll(id, ".", "")
	metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	}, labelNames)

	return pm.EnsureHistogramVec(id, metric)
}

func (pm *Prometrics) EnsureHistogramVec(id string, metric *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	if pm == nil {
		return nil, PrometricInstanceIsNil
	}
	pm.metricsMutex.Lock()
	defer pm.metricsMutex.Unlock()

	if existing, ok := pm.metrics[id]; ok {
		if hist, ok := existing.(*prometheus.HistogramVec); ok {
			return hist, nil
		}
		return nil, PrometricDifferentTypeExistsForIdError
	}

	if err := prometheus.Register(metric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			existingMetric := are.ExistingCollector.(*prometheus.HistogramVec)
			pm.metrics[id] = existingMetric
			return existingMetric, nil
		}
		return nil, err
	}

	pm.metrics[id] = metric
	return metric, nil
}

// ------------------------------------------------------------------------------------------------

type RoutinesCounterValue struct {
	v int64
	m sync.Mutex
}

type RoutinesCounter struct {
	counter sync.Map
}

func (rc *RoutinesCounter) Started(routineTypeName string) {
	if rc == nil {
		return
	}
	if v, ok := rc.counter.Load(routineTypeName); ok {
		rcv := v.(*RoutinesCounterValue)
		rcv.m.Lock()
		rcv.v++
		rcv.m.Unlock()
	} else {
		rcv := &RoutinesCounterValue{}
		rc.counter.Store(routineTypeName, rcv)
	}
}

func (rc *RoutinesCounter) Stopped(routineTypeName string) {
	if rc == nil {
		return
	}
	if v, ok := rc.counter.Load(routineTypeName); ok {
		rcv := v.(*RoutinesCounterValue)
		rcv.m.Lock()
		rcv.v--
		if rcv.v < 0 {
			rc.counter.Delete(routineTypeName)
		}
		rcv.m.Unlock()
	}
}

func (rc *RoutinesCounter) Read(f func(key string, value int64) bool) {
	if rc == nil {
		return
	}
	rc.counter.Range(func(k, v any) bool {
		rcv := v.(*RoutinesCounterValue)
		rcv.m.Lock()
		res := f(k.(string), rcv.v)
		rcv.m.Unlock()
		return res
	})
}
