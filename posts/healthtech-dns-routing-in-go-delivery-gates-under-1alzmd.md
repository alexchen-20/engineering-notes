# Healthtech DNS Routing in Go: Delivery Gates Under Caching and Failover

The page says appointment reminders are late after a regional hostname cutover. The on-call sees successful scheduling runs and healthy regional endpoints, but those signals leave a gap: did each scheduled reminder actually reach the next delivery stage? Treat this as a runbook scenario, not a reported incident. Geographic DNS is suitable for coarse regional placement when the service can tolerate cached destinations. A healthtech hostname migration also needs a rollback path that keeps those destinations working.

TL;DR: Use DNS to influence where new connections go. Use application delivery evidence to decide whether to promote a region, and use connection or request handling for recovery that cannot wait for DNS caches. A low TTL does not establish a delivery deadline. Keep the old region capable of processing work, and preserve the same logical reminder identity across retries.

## What should have fired before the reminder backlog page?

The missing signal is a gap between work becoming due and work reaching an explicitly named delivery boundary. For this migration, define that boundary before changing the hostname. A scheduler completing a run, a queue accepting a job, a receiving mail server accepting a message, and a recipient seeing that message are different observations. None can silently stand in for the next. The alert should identify which boundary is overdue, which region handled the work, and which routing generation the application intended to exercise.

Name the boundary.

For an illustrative rehearsal, use a synthetic reminder with a due time of 09:00 UTC, a stable logical ID, and an approved test destination. These are example inputs, not production measurements. Record when the candidate region accepts the reminder and when the chosen downstream boundary acknowledges it. A separate observation should record the destination actually reached. The intended routing generation is useful context, but it doesn't prove that a resolver returned the candidate address or that a client opened a new connection.

For email reminders, authentication evidence belongs beside this trace. DMARC evaluates alignment of the visible From domain with an authenticated SPF or DKIM identifier, as specified in RFC 7489. An application hostname move does not, by itself, establish whether that alignment passes. Inspect the actual sending path if it changes. Aggregate DMARC reports can inform authentication assessment; they aren't a synchronous receipt for a particular reminder and do not prove inbox placement.

That distinction changes the promotion decision. A healthy endpoint plus aligned mail authentication is still insufficient evidence that the reminder workflow met its delivery objective. Conversely, a late authentication report should not automatically trigger a traffic rollback while timely, direct workflow observations show the candidate path is behaving as intended.

## Can the DNS layer handle geographic routing while clients retain cached destinations?

DNS caching is useful work reduction, with consequences for cutovers. RFC 1034 describes cached resource records and their TTLs. Changing an authoritative answer affects later lookups; it does not revoke addresses already held by clients or move requests on existing connections. Lowering a TTL at cutover time also cannot shorten the remaining lifetime of an answer cached under the previous TTL. Prepare the change early enough for the old cache policy to age out.

There is another boundary: the location inferred by a geographic policy may reflect a recursive resolver rather than the application user. EDNS Client Subnet, described in RFC 7871, can supply client network information, but its presence and use cannot be assumed for every lookup. A regional answer therefore supplies placement guidance. It does not establish patient location or enforce a data residency rule. Keep any required authorization and processing restrictions in controls that validate the request and its permitted destination.

A TTL isn't an outage stopwatch.

For a rehearsal with a 300-second cached TTL, reducing the authoritative TTL to 30 seconds does not rewrite that existing cache entry. These are test parameters, not measured failover times. The older entry can still have time remaining under its original policy.

RFC 8767 specifies serving stale DNS data under defined conditions. Client behavior and connection reuse add further uncertainty, so the old TTL alone cannot establish a universal upper bound for recovery. Measure the behavior of the clients and resolvers that matter to this workflow. Where that evidence is unavailable, retain capacity on both destinations and make the uncertainty explicit in the promotion gate.

The practical trade-off is straightforward: DNS can keep initial placement outside the request path, but a request-handling layer can make decisions when a request arrives. That layer has its own operational burden. Choose it when the required recovery behavior depends on current request context or must operate while clients still hold old answers. DNS can remain the coarse entry mechanism.

## Instrument the boundary, then rehearse the rollback

Add one focused observation to the reminder worker: the age of an outstanding delivery acknowledgment, together with a stable work identity. The following Go example demonstrates an illustrative boundary check. Its timestamps must come from durable workflow state; a process-local timer would lose the evidence on restart. The threshold is supplied by the service's delivery objective, not a DNS default.

```go
package deliverygate

import "time"

type Observation struct {
    ReminderID string
    Region     string
    Generation string
    DueAt      time.Time
    AcceptedAt time.Time
}

func MissingAcceptance(o Observation, now time.Time, limit time.Duration) bool {
    if o.ReminderID == "" || o.DueAt.IsZero() || limit <= 0 {
        return false
    }
    if now.Before(o.DueAt) || !o.AcceptedAt.IsZero() {
        return false
    }
    return now.Sub(o.DueAt) >= limit
}
```

This predicate answers a narrow question: is an acknowledgment still missing after the configured interval? It doesn't measure completed deliveries that were late, validate timestamp ordering, perform deduplication, or decide that a region is healthy. Instrument those concerns separately. Missing IDs or timestamps need an instrumentation alert; the false return for incomplete input must not become a green promotion signal. Keep reminder IDs in controlled diagnostic records rather than using one metric label per reminder, and omit patient details from the routing trace.

Define what AcceptedAt means in the receiving system. If it means durable acceptance by the next delivery stage, populate it only after that stage confirms acceptance. Calling an attempted send an acceptance would make the new alert reproduce the original visibility gap. Also track completed delivery latency, since an acknowledgment arriving after the limit can clear an outstanding-work condition while still documenting a missed objective.

Now exercise both paths. The rehearsal should include a request using the candidate destination, one deliberately using the retained destination, and a retry of the same logical reminder. This tests behavior under mixed routing, which is the state a DNS cutover must tolerate. Require one logical delivery effect at the chosen boundary. A queue acknowledgment alone cannot demonstrate that property.

Keep the identity stable across regions.

If a worker times out after the receiver accepts work, a retry has an ambiguous outcome. Reuse the logical reminder ID and enforce deduplication at a durable boundary that both regions respect. Creating a fresh ID during rollback turns recovery into another logical send. If the receiving system lacks an idempotent acceptance contract, document the reconciliation step and withhold automatic replay until the outcome is known. This is a proposed operating requirement, not a guarantee supplied by DNS or DMARC.

## Promotion needs evidence from both destinations

The runbook should specify four checks before traffic promotion:

1. The candidate path reaches the declared delivery boundary within the service's approved objective.
2. The retained path continues to accept the work that cached destinations can send there.
3. A repeated logical reminder produces the intended single delivery effect at the boundary being tested.
4. Restoring the previous DNS answer leaves both in-flight work and later retries recoverable.

These checks cost rehearsal time and overlapping capacity. They buy evidence about the actual rollback, including the awkward interval when observations from both destinations are valid. Reducing overlap before that evidence exists trades a visible capacity expense for uncertainty about queued work. Record that decision explicitly; price is not the promotion criterion.

Assign ownership as well. The routing operator changes the answer, the delivery operator checks the workflow boundary, and an incident lead decides whether the evidence supports promotion or reversal. One person may hold several roles, but the runbook should name the decisions. A rollback entry must include the prior routing configuration and the application state needed to keep processing safely. Reversing a DNS edit alone does not reverse accepted work.

After reversal, continue observing the candidate destination. It may still receive traffic through cached answers or existing connections. Retire it when the agreed observation criteria support doing so, rather than declaring the migration finished at the instant the authoritative answer changes.

## A threshold can create its own incident

Close the loop at the page that started this note. Page on a delivery boundary that matters to the reminder objective, with explicit handling for absent telemetry. A candidate-region promotion gate can pause on less evidence than an overnight page requires: stopping a planned change and interrupting an on-call engineer have different costs.

Set the threshold too close to ordinary queue and acknowledgment delays, and routine variation can trigger repeated reversals. Each reversal extends the mixed-routing interval and creates more ambiguous retry outcomes to reconcile. Set it too far out, and the first useful signal arrives after reminders have already missed their objective. Establish the threshold from observed workflow behavior and the approved delivery requirement, then rehearse it with delayed acknowledgments and missing telemetry.

For this healthtech cutover, geographic DNS earns its place when cached destinations remain safe and delivery evidence controls promotion. The final acceptance test is a recoverable reminder workflow under mixed routing. A changed answer is only one step.

## References

The DNS standards below support the caching and placement boundaries. The DMARC specification supports the distinction between authentication assessment and delivery evidence. Operational gates and rehearsal inputs above are proposed runbook choices.

## Further reading

- RFC 1034 — Domain names: concepts and facilities, including caching and TTLs: https://datatracker.ietf.org/doc/html/rfc1034
- RFC 7871 — Client Subnet in DNS Queries: https://datatracker.ietf.org/doc/html/rfc7871
- RFC 8767 — Serving Stale Data to Improve DNS Resiliency: https://datatracker.ietf.org/doc/html/rfc8767
- RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
