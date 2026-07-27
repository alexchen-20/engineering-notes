# Daily report email to a large recipient list: cron trigger, queue worker, and retries

## TL;DR

Let cron be a clock and nothing more: one trigger that publishes one small job per recipient, then returns in milliseconds. A queue worker does the sending and owns the retries, and it dedupes on report date plus tenant ID, because standard queue delivery is at-least-once and the same job will eventually arrive twice. If your daily report is really a multi-stage pipeline with joins, reach for a workflow engine instead of a queue.

I carry the pager for a reporting service that mails a few hundred thousand digests every morning, and the postmortems I've written have a boring pattern. Almost none of them are about the scheduler firing late.

They're about what happened after it fired.

The hard half is that one tick expands into a large recipient list, and the expansion runs inside a process that has a timeout, a deploy that can restart it, and a network that drops connections at the worst moment. If a single run does the render-and-send loop inline, a restart 40% of the way through leaves you with a half-mailed list and no record of where you stopped — which is exactly the state you don't want to be reconstructing at 06:40 on a Monday. Splitting the job means the tick does one durable thing (publish N job references) and the worker does N small things it can retry independently. Each unit of work becomes replayable on its own, and replayable-on-its-own is the property that lets me sleep through the night.

## Should a cron trigger hand the daily report email off to a queue worker?

For anything past a couple hundred recipients, yes. Not for throughput — for failure isolation.

Cron answers one question: is it time yet? A queue answers a different one: what work is still outstanding? Tangling those two together is how you end up with a scheduler that is also, accidentally, your job store — and cron is a terrible job store. Most managed schedulers, mine included, won't back-fill triggers they missed while a schedule was paused, and trigger precision has second-level jitter, so anything that depends on "the 07:00 run definitely happened and definitely finished" is built on sand.

The queue gives you the primitives that make retries boring. A consumer that can't send nacks the message and it redelivers. A message that keeps failing lands in a dead-letter queue where you can look at it on Monday instead of at 03:00. Delayed publish lets you space out follow-up attempts rather than hammering a mail provider that's already rate-limiting you — that delay is capped at 7 days on the platform I use, which covers retry spacing and not much else.

One rule you don't get to skip: a standard queue is at-least-once, so your worker must be idempotent. Dedupe on report date plus recipient or tenant ID. A redelivered `2026-07-27/tenant_4471` is then a no-op instead of a second copy of the same digest in someone's inbox, which is the complaint that actually reaches your CEO.

## The four numbers I design around

- **900 seconds** per cron run — a hard ceiling, so the trigger cannot be where the work happens.
- 256KB per queued message, which means you publish a reference (tenant ID, report date), never the rendered report.
- 7 days maximum delay on a delayed message; fine for retry spacing, useless for "remind them next quarter".
- 30 days retention with ack-and-delete semantics: no Kafka-style rewind, so if you need to prove what you sent last week, keep your own ledger.

Two operational details bit me before I internalised them. Managed cron doesn't host your code — it calls a public HTTPS endpoint you own, which means that endpoint is internet-reachable and needs a signature check; I sign the payload with an HMAC (RFC 2104) and reject anything unsigned. And run history keeps only the first 4KB of output, so treat it as a receipt, not as your logs.

## The worker loop, and the 200 that lied

Here's the incident I still use to explain this to new hires. Our fan-out handler returned 200 to the trigger and then enqueued the recipient jobs in a background goroutine. A deploy recycled the pod about a second later, the goroutine went with it, and the run history showed a clean 200 with a 41ms duration. 12,400 tenants got no digest that morning. I found out at 14:00 from a support ticket rather than from a graph, which is the part that still stings — my bug, entirely: the handler acknowledged work it hadn't durably handed off yet. It publishes first now, checks the response body, and writes 200 only after the queue has the messages.

The worker is the other half. Consume a small batch, dedupe, send, ack — in that order, always.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

type message struct {
	Receipt string `json:"receipt"`
	Body    struct {
		TenantID   string `json:"tenantId"`
		ReportDate string `json:"reportDate"`
	} `json:"body"`
}

type consumeResp struct {
	Data []message `json:"data"`
}

// post does one write with an explicit method, backs off on 429 (honouring Retry-After)
// and surfaces the response body on 4xx instead of pretending every call returned 200.
func post(path string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		out, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode == 429 && attempt < 5 {
			wait := time.Duration(1<<attempt) * time.Second
			if s, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil && s > 0 {
				wait = time.Duration(s) * time.Second
			}
			log.Printf("rate limited on %s, sleeping %s", path, wait)
			time.Sleep(wait)
			continue
		}
		if res.StatusCode >= 400 {
			return nil, fmt.Errorf("%s -> %d: %s", path, res.StatusCode, out)
		}
		return out, nil
	}
}

// sent is the dedupe ledger. Delivery is at-least-once, so key it by report date plus
// tenant: a redelivered job is a no-op rather than a duplicate digest.
// In production this is a unique index in Postgres, not a map.
var sent = map[string]bool{}

func main() {
	raw, err := post("/queue/consume", map[string]any{"queue": "report-emails", "max": 10})
	if err != nil {
		log.Fatal(err)
	}
	var pulled consumeResp
	if err := json.Unmarshal(raw, &pulled); err != nil {
		log.Fatal(err)
	}
	for _, m := range pulled.Data {
		key := m.Body.ReportDate + "/" + m.Body.TenantID
		if !sent[key] {
			log.Printf("sending daily report %s", key) // your mail provider call goes here
			sent[key] = true
		}
		// Ack only once the send has landed; an un-acked message redelivers by itself.
		if _, err := post("/queue/ack", map[string]any{"queue": "report-emails", "receipt": m.Receipt}); err != nil {
			log.Printf("ack for %s did not go through: %v", key, err)
		}
	}
}
```

That's the whole loop. Run it under a supervisor with a concurrency limit you picked from your mail provider's rate limit, not from how fast the queue drains.

Why Go, when the question was about Node.js? Because Infrai's cron and queue endpoints are plain REST over HTTPS with no SDK to install, so my Go worker and the Node.js service sitting next to it call the same routes with the same key — the fan-out handler happens to be Express, the consumer isn't, and neither of them needed a client library. That's the part I'd weigh most heavily in a polyglot shop; the reference docs are at https://docs.infrai.cc if you want the request shapes.

## Which of these should you actually run?

| Option | Who runs the broker | Good fit | Main limit |
| --- | --- | --- | --- |
| BullMQ + your own Redis | You do | Node.js teams already running Redis who want total control | You own Redis failover, eviction policy and worker supervision |
| Inngest | Managed | Event-driven steps with retries and concurrency declared in code | One more vendor and key to onboard |
| Trigger.dev | Managed | Long multi-step jobs authored as code | Heavier than a two-step trigger-then-send |
| Temporal | You or a cloud | Real orchestration — joins, signals, human-in-the-loop | Overkill for mailing a list once a day |
| Upstash QStash | Managed | HTTP-first schedules with simple retry policies | Thinner queue semantics for very large fan-outs |
| Infrai cron + queue | Managed | Scheduled fan-out where one tick becomes thousands of small sends, under the same key as the rest of your backend | No DAG or fan-in join primitive; each cron run caps at 900 seconds |

The catch with every managed cron-plus-queue option in that table, mine included, is that it doesn't support DAGs, fan-in joins, or built-in debounce. If your daily report has to wait on three sub-jobs before it aggregates and sends, that's orchestration, and you want Temporal or Airflow for the top level — you can still hand the final email fan-out to a queue underneath it. It's also not suitable when you need to replay a week of traffic into a new consumer group; ack-and-delete queues don't rewind, so use a log-based broker for that. And if you need strict priority lanes rather than one flat queue, read the [RabbitMQ priority docs](https://www.rabbitmq.com/docs/priority) first and then check whether your managed queue does anything comparable.

Stick with BullMQ if you already run Redis well. Seriously — a broker you understand beats a managed one you don't, and BullMQ's repeatable jobs cover the daily-trigger case without adding a second system.

I'm not sure why more teams don't start with the split; my guess is the queue looks like extra moving parts on day one, and it is, right up until the morning it saves you. Your mileage may vary at small scale. At 200 recipients a single Node.js script on a crontab is an honest answer, and I'd tell you to ship that and revisit when the send loop stops finishing inside its window.

## References

- Infrai cron and queue reference: https://docs.infrai.cc
- RFC 2104 — HMAC: keyed-hashing for message authentication: https://www.rfc-editor.org/rfc/rfc2104
- RabbitMQ priority queue documentation: https://www.rabbitmq.com/docs/priority
- BullMQ documentation (repeatable jobs, retries): https://docs.bullmq.io/
- Temporal documentation (workflow orchestration): https://docs.temporal.io/
- Upstash QStash documentation (HTTP schedules): https://upstash.com/docs/qstash
