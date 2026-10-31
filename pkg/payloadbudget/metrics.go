package payloadbudget

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type budgetMetrics struct {
	inFlight    metric.Int64ObservableGauge
	limit       metric.Int64ObservableGauge
	rejections  metric.Int64Counter
	acquireWait metric.Float64Histogram
}

func newBudgetMetrics(meter metric.Meter, b *Budget) *budgetMetrics {
	m := &budgetMetrics{}

	inFlight, err := meter.Int64ObservableGauge(
		"tunnel_payload_budget_inflight_bytes",
		metric.WithDescription("Bytes currently reserved against the tunnel link payload budget."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil
	}
	limitGauge, err := meter.Int64ObservableGauge(
		"tunnel_payload_budget_limit_bytes",
		metric.WithDescription("Configured tunnel link payload budget; zero means unlimited."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil
	}
	rejections, err := meter.Int64Counter(
		"tunnel_payload_budget_rejections_total",
		metric.WithDescription("Number of payloads rejected because they exceed the tunnel link payload budget."),
		metric.WithUnit("{count}"),
	)
	if err != nil {
		return nil
	}
	acquireWait, err := meter.Float64Histogram(
		"tunnel_payload_budget_acquire_wait_seconds",
		metric.WithDescription("Time spent waiting for tunnel link payload budget bytes before admission."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil
	}

	m.inFlight = inFlight
	m.limit = limitGauge
	m.rejections = rejections
	m.acquireWait = acquireWait

	if _, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(inFlight, b.InFlight())
		observer.ObserveInt64(limitGauge, b.Limit())
		return nil
	}, inFlight, limitGauge); err != nil {
		return nil
	}

	return m
}

func stageAttribute(stage string) attribute.KeyValue {
	return attribute.String("stage", stage)
}
