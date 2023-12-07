package prometrics

import (
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

var (
	PrometricDifferentTypeExistsForIdError error = errors.New("metrics with a different type exists for an id")
)

type Prometrics struct {
	metricsMutex *sync.Mutex
	metrics      map[string]any
}

func NewPrometrics() *Prometrics {
	return &Prometrics{&sync.Mutex{}, map[string]any{}}
}

func NewPrometricsWithServer(pattern string, addr string) *Prometrics {
	go func() {
		http.Handle(pattern, promhttp.Handler())
		lg.Logln(lg.FatalLevel, http.ListenAndServe(addr, nil))
	}()
	return NewPrometrics()
}

func (pm *Prometrics) Exists(id string) bool {
	pm.metricsMutex.Lock()
	defer pm.metricsMutex.Unlock()
	_, ok := pm.metrics[id]
	return ok
}

// GaugeVec ---------------------------------------------------------------------------------------
func (pm *Prometrics) EnsureGaugeVecSimple(id string, help string, labelNames []string) (*prometheus.GaugeVec, error) {
	name := strings.ReplaceAll(id, ".", "")
	metric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	}, labelNames)
	return pm.EnsureGaugeVec(id, metric)
}

func (pm *Prometrics) EnsureGaugeVec(id string, metric *prometheus.GaugeVec) (*prometheus.GaugeVec, error) {
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
	name := strings.ReplaceAll(id, ".", "")
	metric := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	}, labelNames)

	return pm.EnsureHistogramVec(id, metric)
}

func (pm *Prometrics) EnsureHistogramVec(id string, metric *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
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
