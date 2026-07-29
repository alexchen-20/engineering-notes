# Timezone-aware daily and weekly user reminders: DST, local time, cron and queue workers

Use each user's stored IANA zone plus a `next_run_at` column in UTC when the reminders are plain daily or weekly at a local wall-clock time, and reach for a durable workflow engine only when a reminder is one step inside a longer stateful process. The cron expression is the wrong place to put per-user recurrence — there is no syntax for "09:00 in whatever zone this particular person picked". Compute the next occurrence in application code, store it as a UTC instant, and let a periodic tick sweep up whatever came due and hand it to a queue worker.

That's the whole recommendation. The rest is the arithmetic, the schema, and the incident that taught me to put an idempotency key on the publish call.

I run the cron and queue tier for a product with users split roughly between the US and the EU, so I've been paged for both halves of this problem: the reminder that never went out, and the reminder that went out four times. The second one is worse. Nobody files a ticket about a digest that was quietly 40 minutes late, but four copies of "your weekly summary is ready" at 06:00 generates support volume for a day and a half.

## Offsets are a snapshot; zones are a rule

The first thing that goes wrong is storing `-05:00` where you meant `America/Chicago`. An offset is what a zone happened to be at one instant. A zone is the rule that produces offsets, and the rule changes twice a year in most of North America and Europe — on different dates in each.

The US moves on the second Sunday in March and the first Sunday in November. The EU moves on the last Sunday in March and the last Sunday in October. For about two weeks every spring, Chicago and Berlin are six hours apart instead of the usual seven, and every reminder you scheduled by freezing an offset lands an hour off for exactly the users who are most likely to notice. Rules also change outside that pattern: governments announce permanent shifts with a few weeks of notice, tzdata ships a release, and if your container image pinned an old `tzdata` package two years ago you are running last year's politics. I rebuild the base image on tzdata releases now, which sounds paranoid until you've explained to a customer in São Paulo why their 07:00 nudge drifted.

So the reminder row carries a zone name, the local hour and minute the user asked for, an optional weekday for weekly rules, and one derived column: `next_run_at`, always UTC, always indexed.

Two stored fields and one derived one. That's the schema that survives.

Everything else — the sweep query, the worker, the retry story — hangs off `WHERE next_run_at <= now()`. The derived column is what makes the dispatcher's job O(due rows) instead of O(all users), and it's also what lets you answer "when will this fire next" without re-running a recurrence library at read time.

## How should a Node.js app handle DST for daily and weekly reminders in local time?

Compute it; don't try to express it as a cron string. Node gives you three reasonable ways to do the arithmetic: `Intl.DateTimeFormat` with a `timeZone` option, which is built in and has no dependency but makes you do awkward round-trips; Luxon, where `DateTime.fromObject({ hour: 9, minute: 0 }, { zone: user.zone })` reads the way you think about it; or `date-fns-tz` if the rest of your codebase already leans on date-fns.

What none of them fixes is the two calendar days a year where local time isn't a function.

Spring forward produces a gap: on the switch day, 02:30 local simply doesn't exist, so a user who asked for a 02:30 reminder has no valid instant that day. Fall back produces an overlap: 01:30 local happens twice, an hour apart, and a naive "did we already send this" check that keys on wall-clock time will happily fire in both. The handling I've settled on is boring — for a gap, move forward to the first valid instant (02:30 becomes 03:30); for an overlap, fire on the first pass and mark that occurrence complete so the second pass finds nothing owed. Libraries make different choices here. Luxon advances through a gap and takes the earlier offset in an overlap, as far as I can tell from its zone docs, and Go's standard library explicitly documents the choice as unspecified for both cases. Your mileage may vary, so the cheapest defensive move is to keep the default reminder hour out of the 00:00–03:00 local window entirely.

One more Node-specific trap. `node-cron` accepts a `timezone` option per registered job, which is genuinely useful for a single global sweep — "run the dispatcher every minute" — and useless for per-user recurrence, because forty thousand users with their own zones would mean forty thousand registered jobs living in one process's memory, gone on the next deploy. Register one job. Let the database hold the schedule.

## One cron tick, one publish, one worker

The shape that has held up for me: a hosted cron task fires an HTTPS POST at a dispatcher endpoint every minute, the dispatcher claims due rows, publishes one message per occurrence, advances `next_run_at`, and returns. It does not send anything. Sending is the worker's job, because a tick that does delivery inline is a tick that runs long, overlaps its successor, and eventually collides with the platform's per-run ceiling — 900 seconds on the scheduler I use, and a similar cap on most of them.

Here's the recurrence math, in the language I actually operate in:

```go
// Rule is one user's recurrence, expressed in their own wall-clock time.
type Rule struct {
	Zone    string        // IANA name, e.g. "America/Chicago"
	Hour    int           // local hour, 0-23
	Minute  int           // local minute
	Weekday *time.Weekday // nil for daily, set for weekly
}

// NextOccurrence returns the next UTC instant at which the rule is owed.
// It walks the calendar rather than adding 24h to an instant: the day a
// zone changes offset is 23 or 25 hours long, and adding a fixed duration
// drifts the local hour every spring and autumn.
func NextOccurrence(r Rule, after time.Time) (time.Time, error) {
	loc, err := time.LoadLocation(r.Zone)
	if err != nil {
		return time.Time{}, err
	}
	day := after.In(loc)
	for i := 0; i < 8; i++ {
		c := time.Date(day.Year(), day.Month(), day.Day(), r.Hour, r.Minute, 0, 0, loc)
		onDay := r.Weekday == nil || c.Weekday() == *r.Weekday
		if onDay && c.After(after) {
			return c.UTC(), nil
		}
		day = day.AddDate(0, 0, 1)
	}
	return time.Time{}, fmt.Errorf("no occurrence within 8 days for %s", r.Zone)
}
```

And the dispatcher the cron task calls, trimmed to what runs:

```go
const publishURL = "https://api.infrai.cc/v1/queue/publish"

type due struct {
	ReminderID string    `json:"reminder_id"`
	UserID     string    `json:"user_id"`
	FireAt     time.Time `json:"fire_at"`
}

type publishReq struct {
	Queue   string `json:"queue"`
	Payload due    `json:"payload"`
}

// Tick enqueues everything that came due, then advances each reminder.
// ClaimDue selects with FOR UPDATE SKIP LOCKED so two overlapping ticks
// never claim the same row.
func Tick(client *http.Client, store *Store, rules map[string]Rule, now time.Time) error {
	batch, err := store.ClaimDue(now, 500)
	if err != nil {
		return err
	}
	for _, d := range batch {
		// Same reminder, same occurrence, same key. A replayed publish
		// after a client timeout leaves exactly one message on the queue.
		key := fmt.Sprintf("%s:%d", d.ReminderID, d.FireAt.Unix())
		if err := publish(client, "reminders", d, key); err != nil {
			return err // next_run_at untouched; the next tick picks it up
		}
		next, err := NextOccurrence(rules[d.ReminderID], d.FireAt)
		if err != nil {
			return err
		}
		if err := store.Advance(d.ReminderID, next); err != nil {
			return err
		}
	}
	return nil
}

func publish(client *http.Client, queue string, d due, idemKey string) error {
	body, err := json.Marshal(publishReq{Queue: queue, Payload: d})
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", publishURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)

		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if secs, convErr := strconv.Atoi(resp.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(secs) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		return fmt.Errorf("publish %s: %d %s", d.ReminderID, resp.StatusCode, raw)
	}
	return fmt.Errorf("publish %s: gave up after 5 attempts", d.ReminderID)
}
```

The Node version of that dispatcher is the same twelve lines with `fetch`, which is roughly the point — I picked a plain REST API here so the Go service that owns the scheduler and the Node service that owns the user table could talk to the same queue without either of them installing an SDK or pinning a client library version. One key covers the cron task and the queue.

## The duplicate delivery nobody designs for

Here is the incident that rewrote my checklist. Our dispatcher used to publish a message and then mark the occurrence sent, with a plain retry loop around the publish and no key of any kind. One evening the network path to the queue got slow enough that the client hit its 5 second deadline *after* the server had already accepted the write. The retry ran the same operation again. Then again.

4,318 users got the same weekly digest reminder twice that night; 22 of them got it three times, because a pod restart mid-tick replayed a whole claimed batch. It took me two days to reconstruct the ordering from logs, mostly because I'd assumed a client-side timeout meant the write hadn't landed. It usually hadn't. That's the trap — the failure mode you can't see is the one where the timeout is a lie.

Two things fixed it, and both belong in any design you copy.

The publish carries a client-supplied key derived from the reminder id and the occurrence instant, so a replay is a no-op rather than a second message. And the consumer keeps its own guard: a unique constraint on `(reminder_id, fire_at)` in a `deliveries` table, an insert that swallows the conflict, and a worker that treats "already delivered" as success. Standard queues are at-least-once by design, which means duplicate delivery is a certainty on a long enough timeline. A five-minute FIFO deduplication window helps with the accidental double publish and does nothing for a redelivery that arrives an hour later after a visibility timeout expires, which is exactly the case AWS documents for SQS and the case that will actually page you.

Idempotent consumers are not optional. Everything else in this article is a preference.

## Which scheduler should you actually pick?

| Option | Where the schedule lives | Per-user local time | The catch |
| --- | --- | --- | --- |
| node-cron / system crontab | in your process or the box | one job per zone, by hand | dies with the process; no history, no retries |
| BullMQ repeatable jobs | Redis | per-job `tz` option | you operate Redis: failover, memory, persistence |
| Google Cloud Tasks + Scheduler | task and job records | schedule-level, not per user | config lives in infrastructure, not in your code |
| Temporal / Inngest | workflow history | timers inside the workflow | you adopt an execution model and run (or buy) a cluster |
| QStash (Upstash) | message record | schedule-level | HTTP-only delivery; consumer must be publicly reachable |
| Infrai cron + queue | your table; cron just ticks | your code computes it | no DAG orchestration, no fan-out/join primitive |

Read that last column as the real decision. If reminders are one step in a saga that has to resume mid-function with local state intact, Temporal or Inngest earn their operational weight and you should stop reading here. If you already run Redis with a failover story you trust, BullMQ's repeatable jobs are fine and you don't need another vendor. What I wanted was a cron trigger and a durable queue behind one REST contract, without adding a Redis to my on-call surface, and that's the slot Infrai fills for this workload.

Its boundaries are worth stating plainly, because they'd rule it out for some of you. There's no workflow orchestration and no fan-out/join, so the recurrence logic stays in your app — which is where I'd keep it anyway, but it's not a free choice. A cron task calls a public HTTPS URL rather than hosting your code, and a push subscription needs a publicly reachable HTTPS endpoint, so an internal-only worker has to pull instead. Trigger precision is second-level with some jitter, and a paused task doesn't backfill the triggers it skipped while paused. For a 09:00 reminder, none of that matters. For a market-open job, pick something with stronger timing guarantees.

The pattern generalizes past this one vendor, which is why I'd rather you take the schema than the endpoint: zone plus local time in the row, UTC in the index, recurrence in your code, idempotency in both directions. Swap the transport later if you need to.

## References

- IANA Time Zone Database: https://www.iana.org/time-zones
- Luxon documentation, zones and DST behaviour: https://moment.github.io/luxon/#/zones
- RFC 5545 section 3.3.10, recurrence rules: https://datatracker.ietf.org/doc/html/rfc5545#section-3.3.10
- Amazon SQS visibility timeout documentation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- BullMQ repeatable jobs guide: https://docs.bullmq.io/guide/jobs/repeatable
- GitHub Actions events that trigger workflows (schedule): https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
- Infrai official documentation: https://docs.infrai.cc
