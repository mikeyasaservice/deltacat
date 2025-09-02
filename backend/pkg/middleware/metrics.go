package middleware

import (
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	activeConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "http_active_connections",
			Help: "Number of active HTTP connections",
		},
	)
)

// PrometheusMiddleware collects metrics for each request
func PrometheusMiddleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		
		// Increment active connections
		activeConnections.Inc()
		defer activeConnections.Dec()

		// Process request
		err := c.Next()

		// Record metrics
		status := strconv.Itoa(c.Response().StatusCode())
		duration := time.Since(start).Seconds()
		
		httpRequestsTotal.WithLabelValues(c.Method(), c.Path(), status).Inc()
		httpRequestDuration.WithLabelValues(c.Method(), c.Path(), status).Observe(duration)

		return err
	}
}

// PrometheusHandler returns the Prometheus metrics handler
func PrometheusHandler() fiber.Handler {
	return adaptor.HTTPHandler(promhttp.Handler())
}