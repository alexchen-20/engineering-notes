# Debugging Feature Flag Stale Cache and Client-Server Mismatches

**Use a versioned flag snapshot, measure its age, and compare the client decision with the server decision before changing a feature flag polling interval.** A feature flags stale cache is often an eventual consistency issue, but a client-server mismatch can also come from targeting rules, identity shape, or two SDKs evaluating different snapshots.

I've been paged for missed cron runs and duplicate queue deliveries, so I treat a rollout decision like any other production input: record it, give it an age, and make replay possible. Don't start by turning polling down to a few seconds. First prove which side made which decision and from which revision.

## What does a feature flags stale cache, polling interval, eventual consistency, client-server mismatch mean in practice?

Most flag systems distribute a configuration document from a control plane to SDKs. The document is cached locally; polling, streaming, or a relay refreshes it. A rule edited in the dashboard therefore does not become a simultaneous global fact. It becomes visible to each evaluator after that evaluator receives and accepts a newer document. That window is eventual consistency, and it is normal enough to design for rather than a reason to guess at an incident cause.

The useful distinction is between configuration propagation and evaluation input. A stale cache means the client or server is holding an older flag document. A mismatch can instead mean both have fresh documents but are evaluating different user keys, attributes, environment keys, or default values. Client SDKs also need rules and segments that are safe to expose; a server-side evaluator can have a fuller audience model. Treating those two designs as interchangeable has caused plenty of bad rollbacks.

I spent 47 minutes chasing a rollout that looked cached. The client sent `account_id`, while the server expected `accountId`; the evaluation error said only “invalid context,” which was useless until I logged the normalized context beside the flag revision. The cache was current. The data shape wasn't.

Start every investigation with four fields in the decision log: flag key, variation, configuration revision or checksum, and snapshot age. Add the evaluated subject key and a request or trace ID. Hash or redact sensitive attributes; the point is comparability, not copying a customer profile into logs. A server response can return its own decision metadata so a browser report has two comparable records.

Short paths hide mistakes.

## How should I debug a feature flag polling interval when the client and server disagree?

Make one request that carries a stable correlation ID, then capture both evaluations without changing the rollout. I want to know whether the server saw a newer snapshot, whether the client evaluated before a refresh completed, and whether their inputs were actually equivalent. Polling interval is only one timestamp in that chain.

This small Go program models the diagnostic record I put around a flag adapter. It does not evaluate rules itself; your SDK should do that. It shows the minimum metadata worth emitting from both sides, and the `stale` field gives an alerting system a deterministic threshold to query.

```go
package main

import (
	"encoding/json"
	"log"
	"time"
)

type Decision struct {
	RequestID      string        `json:"request_id"`
	Evaluator      string        `json:"evaluator"`
	FlagKey        string        `json:"flag_key"`
	SubjectKey     string        `json:"subject_key"`
	Variation      bool          `json:"variation"`
	SnapshotID     string        `json:"snapshot_id"`
	SnapshotAge    time.Duration `json:"snapshot_age"`
	PollInterval   time.Duration `json:"poll_interval"`
}

func (d Decision) stale(now time.Time, refreshedAt time.Time) bool {
	return now.Sub(refreshedAt) > 2*d.PollInterval
}

func main() {
	now := time.Now()
	refreshedAt := now.Add(-75 * time.Second)
	d := Decision{
		RequestID: "req-7f3a", Evaluator: "server", FlagKey: "new-checkout",
		SubjectKey: "acct-42", Variation: true, SnapshotID: "rev-1842",
		SnapshotAge: now.Sub(refreshedAt), PollInterval: 30 * time.Second,
	}
	b, err := json.Marshal(d)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("decision=%s stale=%t", b, d.stale(now, refreshedAt))
}
```

Log the client record through your telemetry pipeline using the same field names. Then group by `request_id` and compare `snapshot_id`, `subject_key`, and `variation`. If snapshot IDs differ, inspect refresh timing, connectivity, and the SDK's documented update mode. If IDs match and variations differ, diff the normalized contexts and the environment or project key. If both match, look for a downstream cache of the application response rather than the flag cache.

Don't use browser clock time as proof. Mobile sleep, tab suspension, and proxy caches make it a poor source of ordering; server receive time and a monotonic snapshot age are more useful. I'm not sure why teams still omit revision metadata from flag events, but without it a polling change is mostly superstition.

For the actual incident call, I make a tiny worksheet and fill it from evidence rather than memory: dashboard edit time, control-plane revision, server refresh completion, browser refresh completion, server evaluation, browser evaluation, and the response that reached the user. A mismatch that starts before either evaluator sees the new revision is propagation. One that begins after both sides report the same revision is an input or application-cache problem. If the server response has the new decision but the screen renders the old experience, inspect CDN and API-response cache keys before changing the SDK. If only a particular tenant is wrong, compare the exact normalized attributes after redaction, including types; a string `"42"` and a numeric `42` may follow different rule paths depending on the evaluator. I also write down which evaluator was authoritative for the request. This sounds fussy during a quiet deploy, but it prevents an on-call handoff from turning a single stale page into a story about a broken flag platform. The runbook should name the first safe action, usually a bounded refresh or cache invalidation, and the action should be idempotent so two responders do not create a second problem while trying to observe the first.

## Choose the observation surface before you choose a feature flag platform

The product choice affects where you can obtain evaluation and delivery evidence, but it doesn't remove the need to model it. LaunchDarkly, Unleash, and OpenFeature are all credible options with different operating boundaries. OpenFeature is a vendor-neutral API specification, so it can reduce application coupling, while the provider still determines delivery behavior and the fields available for diagnosis.

| Option | Good fit for this debugging work | Trade-off to plan for |
| --- | --- | --- |
| LaunchDarkly | Teams that want managed flag delivery and vendor-provided SDK documentation | You still need to instrument your application context and response path |
| Unleash | Teams that want to operate or control their feature flag service | Operating the service makes cache freshness and telemetry your responsibility |
| OpenFeature | Teams that want a common application-level evaluation API | It is not a flag control plane or an observability backend by itself |
| OpenTelemetry with Grafana or Datadog | Teams correlating decisions with traces, logs, and service health | You must define stable event fields and protect sensitive attributes |

For a distributed service, I usually attach decision fields to the active trace and emit a sampled structured log for mismatches. Metrics are better for aggregate questions: snapshot-age percentile, refresh count, and mismatch rate. Logs answer a specific incident. Traces show whether the client report and the server evaluation belong to the same request. Keep the flag key out of high-cardinality metric labels if your backend charges or limits by series; use logs or exemplars for the detailed breakdown.

The catch is that observability data can create its own incident. A raw targeting context may contain email addresses, plan details, or internal tenant labels. Define an allowlist, hash subject identifiers where correlation is sufficient, and test that redaction happens before export. Cost also belongs in the design: log ingestion is commonly metered, and a per-request event from every browser can swamp the signal. Sampling ordinary decisions while always recording an explicit mismatch is a practical default.

## Deployment, testing, and rollback rules for flag consistency

Test the contract in three layers. Unit tests should hold a fixed flag snapshot and a canonical context, then assert the same variation for the client-facing and server-facing evaluators. Integration tests should update a non-production flag, wait for the documented delivery signal or bounded refresh interval, and record the observed snapshot ID. Finally, a synthetic check should request the real deployment path with a test subject and alert only after a sustained mismatch rate, not after one request.

This is where idempotency pays off. A decision record should be safe to emit twice, and a refresh should safely replace the old snapshot only after validation. For queue consumers, include the evaluated variation and snapshot ID in the job payload or durable audit record; otherwise a retried job can be explained with the wrong current flag state during a postmortem. For cron work, capture the snapshot at job start rather than quietly rereading it after a long task has begun.

Set an explicit rollout rule before deployment: a flag change is operationally complete only after the configured propagation window plus the client cache window, and after the synthetic check observes the intended decision. During a rollback, preserve the old diagnostics for a retention period and announce the expected window to support staff. This avoids the familiar race where a dashboard change looks finished while one cohort is still correctly evaluating an older snapshot.

No single tool is the right tool for every team. Stick with a managed platform when your team cannot staff control-plane operations, and stick with a self-hosted service when data residency or customization requires it. OpenFeature isn't a good fit when you expect it alone to provide targeting administration or a delivery network. Your mileage may vary with mobile clients because their refresh lifecycle is constrained by the operating system, so test background behavior on the devices you actually support.

## References

- https://openfeature.dev/docs/reference/intro/
- https://opentelemetry.io/docs/specs/otel/logs/
- https://docs.getunleash.io/reference/sdks
- https://docs.launchdarkly.com/sdk/concepts/client-side-server-side
- https://grafana.com/docs/grafana-cloud/send-data/otlp/send-data-otlp/
- https://www.datadoghq.com/product/log-management/
- https://aws.amazon.com/cloudwatch/pricing/
