# Cron or Job Queue for Nightly Payment Sync: Retries, Idempotency, Delayed Work in Node.js

Use cron to decide when the nightly reconciliation starts, and a background job queue to carry the work — retries included. The deciding constraint is not Node.js, and it is not which broker you install. It's the cost of waiting: what a stale mismatch costs you between the moment a payment provider settles and the moment your ledger agrees with it.

Everything else follows from that number.

The system I have in mind is a media SaaS — subscription billing, ad payouts, a provider that posts webhooks all day and drops one settlement file each night. Reconciliation is the job that compares the two and emails finance a report before the morning stand-up. Nobody watches it until it's wrong.

## What should the nightly job cost you: a cron tick, or a background queue with retries and delayed messages?

Both, and the split is by who owns the failure.

Cron owns time. It answers exactly one question — is this work due yet — and it should do nothing else. A crontab entry that opens a database connection, pulls a 400 MB settlement file, parses it and sends email is four failure domains stapled to a timer that has no retry semantics and no backlog you can inspect.

A queue owns delivery. Once a job is on it, the broker keeps handing it to a consumer until someone acknowledges, which is what file processing, email sending and webhook sync all actually need. Those three are the background jobs a small SaaS grows first, and they fail for boring reasons: an object-store read stalls, an SMTP relay rate-limits you, the provider answers 429 during its own nightly peak.

Latency and cost enter here, and they pull in opposite directions. A queue with a warm worker pool buys you seconds-to-minutes latency and charges you for a process that runs whether or not there's work, plus a broker to operate. Cron on a box you already own costs close to nothing and charges you a latency floor equal to the tick interval. For a nightly close that floor is 24 hours and it is usually fine — money that settled at 02:00 does not need to be reconciled at 02:01. For the webhook sync feeding an in-app revenue dashboard, 24 hours is not fine, and no amount of tick tuning fixes it. At five-minute ticks you have built a queue with worse observability.

| What creates the work | Mechanism that fits | What it costs you |
|---|---|---|
| A clock: nightly close, weekly invoice | cron that only enqueues | latency floor equal to the tick interval |
| A product event: upload, signup, inbound webhook | queue plus a worker | an always-on consumer and a broker to run |
| A date past the broker's delay horizon | your own due-at table, swept by cron | one table and a sweep query |

That last row is the honest answer to delayed messages. Broker-side delay is fine for the short hops — retry in 30 seconds, debounce for 5 minutes. Stretch it to seven days and you've parked business state somewhere you can't query, migrate or correct: no broker will tell you which dunning emails are scheduled for next Tuesday, and none of them let you cancel one because the customer paid on Monday. Keep the due timestamp in your own table. Sweep it every few minutes. A row you can `SELECT` survives a broker migration; an in-flight delayed message does not.

The catch is that a timer plus a queue is not a workflow engine. Once the nightly close grows a fan-out and a join — reconcile, then chargebacks, then a payout summary that can only run when both finished — you end up hand-rolling state machines inside job payloads, and a work queue is not the right tool for that. The same trade-off applies to replay: an acknowledged message is gone, so if finance may need last quarter's events fed through a new consumer, that requirement belongs on a durable log rather than on a queue you drain. Neither limit shows up in month one. Both are worth writing down before they do.

## Two failure modes: the run that never fired, and the file processed twice

I've been paged for both halves of this — jobs that never ran, and jobs that ran twice. The second one costs more.

Missed runs come from treating a scheduler as a guarantee. Hosted schedulers say plainly that they are not: GitHub Actions documents that scheduled workflows can be delayed during periods of high load, that the shortest interval is five minutes, and that schedules stop firing after 60 days of repository inactivity. A Unix crontab is more predictable and no more forgiving — if the host is down at 02:00, then 02:00 simply does not happen for you. Neither one will phone you about the run that didn't occur. That is the whole argument for driving reconciliation off a due row you can query rather than off the belief that a tick fired.

Duplicate runs come from the delivery contract you already signed. Manual acknowledgement means at-least-once, and at-least-once means exactly what it says. RabbitMQ redelivers messages that were delivered but never acknowledged when a channel or connection closes, and it enforces a delivery acknowledgement timeout — 30 minutes by default — after which the channel is closed and the message goes back on the queue. Automatic acknowledgement trades that safety for throughput and drops in-flight work when a consumer dies. Now put a large settlement file in front of a single-threaded parser: exceed the ack timeout and your slow-but-correct worker gets its message handed to a second worker, mid-file, while the first one keeps writing. Two workers, one ledger, same rows. In payments that surfaces as a number on somebody's invoice, which is a much more expensive way to learn about it than a test.

## Make the worker idempotent before you make it fast

Every retry policy is a duplicate generator. Accept that and the design gets short: derive a stable key from the business rather than from the attempt, write it under a unique constraint in the same transaction as the effect, and acknowledge only after the commit returns.

Which key you pick matters more than the storage engine. For the nightly file the job key is the provider and the settlement date — `settle:acme:2026-08-10` — never a UUID minted at publish time, because a sweep that runs twice has to produce the same key twice. Inside the file, use the provider's own transaction id. For writes going back out to the provider, send a client-generated `Idempotency-Key` header; that pattern is common enough across payment APIs to be the subject of an IETF Internet-Draft, and it's the difference between a retried refund and two refunds. Pair it with `Retry-After` handling on 429 and 503 responses, which RFC 9110 defines for exactly this purpose, and keep the same key across every attempt.

```go
// Nightly reconciliation worker: one message per settlement file,
// one ledger row per provider transaction, ack only after commit.
package reconcile

import (
	"context"
	"database/sql"
)

type Job struct {
	Key       string // "settle:acme:2026-08-10" — stable across cron re-sweeps
	ObjectRef string // pointer into object storage, never the file bytes
}

func Handle(ctx context.Context, db *sql.DB, src RowSource, j Job, ack func() error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op once Commit succeeds

	// The unique index on job_key turns a duplicate delivery into a no-op
	// instead of a second pass over the same settlement file.
	claim, err := tx.ExecContext(ctx,
		`INSERT INTO job_runs (job_key, started_at) VALUES ($1, now())
		 ON CONFLICT (job_key) DO NOTHING`, j.Key)
	if err != nil {
		return err // stays unacked; the broker will redeliver
	}
	if n, _ := claim.RowsAffected(); n == 0 {
		return ack() // already reconciled: drop the duplicate, do not redo it
	}

	rows, err := src.Open(ctx, j.ObjectRef)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		r := rows.Record()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ledger (provider_txn_id, cents, settled_on)
			 VALUES ($1, $2, $3) ON CONFLICT (provider_txn_id) DO NOTHING`,
			r.TxnID, r.Cents, r.SettledOn); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Only now. A crash above this line costs one redelivery, not one duplicate ledger row.
	return ack()
}
```

The producer side stays boring on purpose. One crontab line, finishing in under a second, whose only side effect is a message:

```bash
15 2 * * * reconcile-enqueue --date="$(date -u -d yesterday +\%F)" >> /var/log/reconcile.log 2>&1
```

Email is the awkward case, because sending is a side effect you cannot roll back. Invert the rule there: record the intent to send, commit, then send, and dedupe at the mail provider with a per-report message key. Worst case somebody in finance gets the same report twice. That's cheaper than a ledger they never hear about.

## Verify with three tests, then roll back without deleting the evidence

Verification is not "the scheduled run went green". Three tests, all of which fit on a laptop.

- Deliver the same job twice on purpose. Assert one `job_runs` row, one set of ledger rows, one report — and that the second delivery still acks.
- SIGKILL the worker between commit and acknowledgement. The redelivery has to be a no-op that acks; if it re-parses the file, your claim insert is in the wrong transaction.
- Skip a night. Stop the timer, start it the following day, and confirm the sweep picks up both due dates instead of silently reconciling only the most recent one.

That third test is the one teams skip, and it's the one that maps to the real incident.

Rollback is shorter, and mostly a list of things not to do. Don't purge the queue to make a dashboard green — you are deleting the only record of what was in flight. Pause the producer, let consumers drain what they can safely apply, deploy the previous worker, then replay from the dead-letter queue in small batches with the original job keys intact. Keys are what make replay safe; delete them and every retry is a fresh write. Keep the raw settlement files for as long as finance keeps disputes open, which is months rather than days.

One number I can't give you is the right acknowledgement timeout. It depends on your file sizes and your parser, so measure the p99 processing time for the largest month you have on disk and set the timeout above it, then alert when a job crosses half of it. Cron decides when. The queue decides whether the work survived — and idempotency decides what it cost you when it didn't.

## References

- [RabbitMQ: consumer acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms)
- [GitHub Actions: events that trigger workflows (schedule)](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows)
- [RFC 9110: HTTP Semantics — Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#field.retry-after)
- [IETF Internet-Draft: The Idempotency-Key HTTP Header Field](https://datatracker.ietf.org/doc/draft-ietf-httpapi-idempotency-key-header/)
- [POSIX crontab specification (The Open Group)](https://pubs.opengroup.org/onlinepubs/9699919799/utilities/crontab.html)
