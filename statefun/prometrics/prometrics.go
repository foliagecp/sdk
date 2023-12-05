package prometrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

type Prometrics struct {
	metrics map[string]any
}

func NewPrometrics(pattern string, addr string) *Prometrics {
	go func() {
		http.Handle(pattern, promhttp.Handler())
		lg.Logln(lg.FatalLevel, http.ListenAndServe(addr, nil))
	}()
	return &Prometrics{map[string]any{}}
}

func (pm *Prometrics) RegisterHistogramVec(id string, metric *prometheus.HistogramVec) error {
	if _, ok := pm.metrics[id]; ok {
		return nil
	}
	pm.metrics[id] = metric
	return prometheus.Register(*metric)
}

func (pm *Prometrics) UnregisterHistogramVec(id string) bool {
	if metricAny, ok := pm.metrics[id]; ok {
		if metric, ok := metricAny.(*prometheus.HistogramVec); ok {
			return prometheus.Unregister(*metric)
		}
	}
	return false
}

func (pm *Prometrics) GetHistogramVec(id string) (*prometheus.HistogramVec, bool) {
	if metricAny, ok := pm.metrics[id]; ok {
		if metric, ok := metricAny.(*prometheus.HistogramVec); ok {
			return metric, true
		}
	}
	return nil, false
}

func (pm *Prometrics) Exists(id string) bool {
	_, ok := pm.metrics[id]
	return ok
}
