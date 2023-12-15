package system

import (
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
	PrometricDifferentTypeExistsForIdError error = errors.New("metrics with a different type exists for an id")
	PrometricInstanceIsNil                 error = errors.New("prometrics instance the method is being call against to is nil")
)

type Prometrics struct {
	metricsMutex    *sync.Mutex
	metrics         map[string]any
	routinesCounter *RoutinesCounter
}

func NewPrometrics(pattern string, addr string) *Prometrics {
	pm := &Prometrics{&sync.Mutex{}, map[string]any{}, &RoutinesCounter{}}
	go func() {
		pm.GetRoutinesCounter().Started("prometrics-server")
		defer pm.GetRoutinesCounter().Stopped("prometrics-server")
		if len(pattern) == 0 {
			pattern = "/"
		}
		http.Handle(pattern, promhttp.Handler())
		lg.Logln(lg.FatalLevel, http.ListenAndServe(addr, nil))
	}()

	go pm.golangRuntimeStatsCollector()
	return pm
}

func (pm *Prometrics) golangRuntimeStatsCollector() {
	pm.GetRoutinesCounter().Started("r.statsGolangStatsCollector")
	defer pm.GetRoutinesCounter().Stopped("r.statsGolangStatsCollector")

	if pm == nil {
		return
	}

	mem := &runtime.MemStats{}
	for {
		runtime.ReadMemStats(mem)
		if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_mem_alloc_bytes", "", []string{}); err == nil {
			gaugeVec.With(prometheus.Labels{}).Set(float64(mem.Alloc))
		}
		if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_routines_counter", "", []string{}); err == nil {
			gaugeVec.With(prometheus.Labels{}).Set(float64(runtime.NumGoroutine()))
		}

		pm.GetRoutinesCounter().Read(func(key, value interface{}) bool {
			if gaugeVec, err := pm.EnsureGaugeVecSimple("fg_runtime_routines", "", []string{"routine_type_name"}); err == nil {
				gaugeVec.With(prometheus.Labels{"routine_type_name": key.(string)}).Set(float64(value.(int)))
			}
			return true
		})

		time.Sleep(1 * time.Second)
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
	if metricAny, ok := pm.metrics[id]; ok {
		if metric, ok := metricAny.(*prometheus.GaugeVec); ok {
			return metric, nil
		} else {
			return nil, PrometricDifferentTypeExistsForIdError
		}
	}
	pm.metrics[id] = metric
	return metric, prometheus.Register(*metric)
}

// ------------------------------------------------------------------------------------------------

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
	if metricAny, ok := pm.metrics[id]; ok {
		if metric, ok := metricAny.(*prometheus.HistogramVec); ok {
			return metric, nil
		} else {
			return nil, PrometricDifferentTypeExistsForIdError
		}
	}
	pm.metrics[id] = metric
	return metric, prometheus.Register(*metric)
}

// ------------------------------------------------------------------------------------------------

type RoutinesCounter struct {
	counter sync.Map
}

func (rc *RoutinesCounter) Started(routineTypeName string) {
	if rc == nil {
		return
	}
	if v, ok := rc.counter.Load(routineTypeName); ok {
		counter := v.(int)
		rc.counter.Store(routineTypeName, counter+1)
	} else {
		rc.counter.Store(routineTypeName, 1)
	}
}

func (rc *RoutinesCounter) Stopped(routineTypeName string) {
	if rc == nil {
		return
	}

	if v, ok := rc.counter.Load(routineTypeName); ok {
		counter := v.(int)
		if counter > 0 {
			rc.counter.Store(routineTypeName, counter-1)
		} else {
			rc.counter.Delete(routineTypeName)
		}
	}

}

func (rc *RoutinesCounter) Read(f func(key any, value any) bool) {
	if rc == nil {
		return
	}
	rc.counter.Range(f)
}
