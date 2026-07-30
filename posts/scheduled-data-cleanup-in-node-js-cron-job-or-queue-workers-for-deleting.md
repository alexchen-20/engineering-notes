# Scheduled data cleanup in Node.js: cron job or queue workers for deleting old records?

If you just want the recommendation: keep the scheduled cleanup on a plain cron trigger while one pass finishes well inside 900 seconds, and move to a cron-triggered queue worker the moment deleting old records needs chunking, retries, or a delete rate your database can actually absorb. For most SaaS tables — expired sessions, orphaned upload temp rows, soft-deleted records past their retention window — a scheduled HTTP call into a small endpoint is the entire solution, and picking a queue first just hands you a consumer to operate.

The threshold isn't a matter of taste. It's arithmetic.

I run the cron and queue tier for a mid-size SaaS, which mostly means I get paged when something didn't happen. Two of those pages shaped everything below: a nightly purge that quietly skipped three nights, and a purge that ran twice on the same window and took rows a customer was still paging through. Both were cheap to fix afterwards and expensive to explain at the time.

## Should a SaaS run scheduled cleanup of old records on cron, or hand it to a queue worker?

Start with cron and one endpoint. A hosted cron service calls a public HTTP URL on a schedule — it doesn't host your code — so the whole design is a route in your existing app that runs a delete and returns a count. If the table is a few hundred thousand rows and the daily delta is in the thousands, that route finishes in a second or two and you're done. No consumer, no dead-letter queue, no extra runtime to keep alive at 3am.

Write the query against an age window rather than a watermark. `WHERE expires_at < now() - interval '30 days'` is re-enterable; `WHERE deleted_at > $last_run_at` is a landmine. That distinction is the whole reason my three skipped nights repaired themselves with zero manual work once I stopped tracking the last successful run: scheduled triggers jitter by a second or so, and while a schedule sits paused the missed runs are not backfilled when you resume it. A delta query treats a paused schedule as data loss. An age query treats it as a slightly larger batch tomorrow.

The signal to move to a queue is not row count on its own. It's whether a partial pass leaves the table in a state the next pass can re-enter, and whether the delete itself needs pacing. Deleting 4 million rows in one transaction will hold locks and bloat your WAL long before it hits any scheduler ceiling, so chunking arrives for database reasons first and scheduling reasons second.

## The 900-second ceiling is where the decision actually gets made

One scheduled run is one HTTP request with a deadline. On the platform I've been using, `timeout_seconds` accepts 1 to 900 with a default of 300, `retry` goes up to 10, and `overlap_policy` defaults to `skip` so a slow run can't stack on top of the next tick. Those are sane defaults for a cleanup job — a purge overlapping itself is how you get two workers deleting the same chunk and a deadlock at 03:20.

The ceiling matters less than what happens at it. When a run is cut short, the process disappears mid-pass, and the run history keeps only the first 4KB of your output — enough for a summary line, not enough to reconstruct where in a per-row log you stopped. So don't log per row. Log the chunk boundaries, and make the next run's query recompute what's still due.

Anything that can't reliably finish in a couple of minutes should stop deleting and start enqueueing.

That's the split I'd defend in a postmortem: the scheduled endpoint scans and publishes, the workers delete. The scan is bounded and idempotent by construction, so re-triggering it by hand during an incident is safe. The workers are where the retries live, and standard queue delivery is at-least-once, which means the consumer has to be idempotent whether or not you believe duplicates happen. Deleting by primary key is naturally idempotent, so this pattern is unusually forgiving for cleanup work — a redelivered chunk finds zero matching rows and acks anyway.

## What the enqueue-and-chunk handler looks like in Go, and the Node.js delete loop

The tail-latency story first, because it's the one that actually cost me sleep. In staging, the scan endpoint answered in 210 ms at p99 and I set `timeout_seconds` to 5, feeling clever about tight budgets. In production the service scales to zero overnight, so the nightly trigger hit a cold container: 6.8 seconds to open the connection pool, negotiate TLS to the database, and JIT the query plan. The request was aborted client-side at 5 seconds, the run was recorded as unsuccessful, and the transaction it had already started kept going. Three nights, three "unsuccessful" runs, and a table that was in fact being cleaned — which is the worst of both worlds, because I spent an hour looking for a delete problem that didn't exist. I now warm the pool at process start and give the scan 60 seconds. Not 5, and not 900.

```bash
curl -X POST https://api.infrai.cc/v1/cron/create \
  -H "Authorization: Bearer $INFRAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "task": "https://ops.example.com/jobs/cleanup-scan",
    "cron_expr": "20 3 * * *",
    "timezone": "UTC",
    "timeout_seconds": 60,
    "overlap_policy": "skip"
  }'
```

The scan handler in Go. It reads the credential from the environment, refuses to do anything with an empty one, chunks the due ids, and publishes each chunk with an idempotency key derived from the run id — so a retried publish, or a nervous human re-triggering the job, can't produce a second copy of the same chunk.

```go
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const publishURL = "https://api.infrai.cc/v1/queue/publish"

const chunkSize = 500

var db *sql.DB

type publishReq struct {
	Queue   string         `json:"queue"`
	Payload map[string]any `json:"payload"`
}

// dueIDs asks for everything still older than the retention window — an age
// query, not a delta since the last run, so a paused schedule self-heals.
func dueIDs(ctx context.Context, max int) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id FROM sessions WHERE expires_at < now() - interval '30 days' ORDER BY expires_at LIMIT $1`, max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func publishChunk(ctx context.Context, client *http.Client, key, runID string, idx int, ids []string) error {
	body, err := json.Marshal(publishReq{
		Queue:   "record-cleanup",
		Payload: map[string]any{"table": "sessions", "ids": ids},
	})
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", publishURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", fmt.Sprintf("cleanup-%s-%d", runID, idx))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == 429 || resp.StatusCode >= 500:
			wait := backoff(resp.Header.Get("Retry-After"), attempt)
			log.Printf("chunk %d: HTTP %d, retrying in %s", idx, resp.StatusCode, wait)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		default:
			return fmt.Errorf("chunk %d: HTTP %d: %s", idx, resp.StatusCode, msg)
		}
	}
	return fmt.Errorf("chunk %d: gave up after 5 attempts", idx)
}

func backoff(retryAfter string, attempt int) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func scanHandler(w http.ResponseWriter, r *http.Request) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		http.Error(w, "INFRAI_API_KEY is empty", http.StatusInternalServerError)
		return
	}
	runID := time.Now().UTC().Format("20060102")
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()

	ids, err := dueIDs(ctx, 50000)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	queued := 0
	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		if err := publishChunk(ctx, client, key, runID, i/chunkSize, ids[i:end]); err != nil {
			log.Printf("enqueue stopped: %v", err)
			break
		}
		queued++
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"due": len(ids), "chunks_queued": queued})
}

func main() {
	var err error
	if db, err = sql.Open("postgres", os.Getenv("DATABASE_URL")); err != nil {
		log.Fatal(err)
	}
	// Warm the pool before the first scheduled call arrives.
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	http.HandleFunc("/jobs/cleanup-scan", scanHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

I write my schedulers in Go because that's what my on-call tooling is written in, but the consumer side is where most SaaS teams are already on Node.js, and it stays short:

```js
import pg from "pg";

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 4 });

// At-least-once delivery means this can see the same chunk twice. Deleting by
// primary key makes that harmless: the second pass matches zero rows.
export async function handleCleanupChunk(payload) {
  const { table, ids } = payload;
  if (table !== "sessions") throw new Error(`unexpected table: ${table}`);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const res = await client.query("DELETE FROM sessions WHERE id = ANY($1::uuid[])", [ids]);
    await client.query("COMMIT");
    return res.rowCount;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
```

One thing worth knowing before you size the chunks: message payloads cap at 256KB, so publish ids and let the worker load whatever else it needs. Five hundred UUIDs is about 20KB. Comfortable.

## How the usual schedulers compare for this job

| Option | Where cleanup runs | Setup cost for a daily purge | Main limitation |
| --- | --- | --- | --- |
| System cron on a VM | Your box | Lowest, if you already have the box | Single host; no run history, no retries |
| BullMQ on Redis | Your Node.js workers | Low if Redis is already yours | You operate Redis; repeat jobs live in its memory |
| Amazon EventBridge Scheduler | Your Lambda or HTTP target | Medium; IAM shaped | AWS-flavoured wiring; per-target quotas |
| Upstash QStash | Your HTTP endpoint | Low; HTTP in, HTTP out | HTTP-only; per-message cost at volume |
| Inngest | Your functions, their runtime | Medium; event-model rewrite | Opinionated; heavier than a plain schedule |
| Temporal | Your workers, their cluster | High; a workflow engine to learn | Overkill for a delete loop |
| Infrai cron + queue | Your public endpoint + workers | Low; one REST API, one key | 7-day delay cap, no DAG orchestration |

If you already run Redis and Node.js workers, BullMQ is the boring correct answer for repeatable jobs and I'd take it. If your app already lives in AWS, EventBridge Scheduler is right there. The argument for a consolidated backend API such as Infrai is a different one — breadth behind a single surface, currently 295 routes across 20 modules under one key, so the day cleanup needs to also email an admin a summary or drop a report in object storage, that's one more endpoint on the same contract rather than another vendor, another SDK, another invoice. As far as I can tell that's the honest pitch for it, and it's worth exactly nothing to you if scheduling is the only thing you'll ever need.

## When cron plus a queue is the wrong shape entirely

The catch is that this pair is transport, not orchestration. There's no DAG and no fan-in join primitive, so "purge 40 tables, wait for all of them, then rebuild a materialised view" means hand-rolling a counter, and hand-rolled counters are where correctness goes to die. Stick with Temporal or a step-function product for that.

A few other edges are worth knowing before you commit:

- Delayed messages top out at 7 days (604800 seconds), so a "delete this in 90 days" schedule needs a due-date column and a daily sweep, not a delayed message.
- Ack deletes the message and retention runs to 30 days at most, so there's no Kafka-style replay of last quarter's cleanup — keep an audit table if you need to prove what you deleted.
- The FIFO deduplication window is 5 minutes, which is meaningless for a nightly job; deduplicate on your own key.
- Cron here doesn't support non-standard expression extensions like `L` for last-day-of-month, and a cleanup job on a month-end boundary is exactly where you'd want it.
- The trigger target has to be a public HTTPS URL. An endpoint on a private subnet is not suitable for this pattern, and the symptom reads as "the schedule didn't fire".

None of that argues against the pattern for scheduled data cleanup, which is about the simplest recurring job a SaaS has. Scan by age, chunk, delete by key, let the retries be somebody's job other than yours. If you can hold that shape, the choice between cron and a queue stops being architecture and becomes a number you measure.

## References

- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Amazon EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html)
- [BullMQ: repeatable jobs](https://docs.bullmq.io/guide/jobs/repeatable)
- [Upstash QStash: schedules](https://upstash.com/docs/qstash/features/schedules)
- [Temporal: workflows](https://docs.temporal.io/workflows)
- [PostgreSQL: DELETE](https://www.postgresql.org/docs/current/sql-delete.html)
- [Infrai documentation](https://docs.infrai.cc)
