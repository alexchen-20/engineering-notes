# Rotate API Keys Without Downtime: One Secret Per Deploy Target, 24-Hour Overlap

Use one API key per deploy target, rotate each one on a schedule, and let the old secret keep working through an overlap window while the new one reaches every box. The deciding constraint isn't your secret store, and it isn't your Node.js runtime — it's blast radius: how much of production stops serving when a single credential disappears.

That's the practice. The rest of this is the runbook around it.

## What one shared credential costs an e-commerce backend

Picture the order pipeline on an ordinary Thursday. Platform events — orders, refunds, dispute notifications — arrive at a Node.js webhook receiver that acknowledges fast and pushes each event onto a queue. A Go worker drains the queue and writes orders. A pricing sync and a nightly returns reconciliation run on cron. Add staging, and you're looking at five or six deploy targets, every one of them reading the same `API_KEY` entry out of the same secret store, because that's what the first integration needed and nobody went back to split it.

Then the credential leaks.

Revoke it and you take an outage across the whole pipeline: the receiver returns 401, the platform starts retrying webhooks with its own backoff, the worker stops draining, and the cron jobs skip a run that — depending on the scheduler — never gets backfilled. Leave it alive while you scramble to cut over and you're running checkout on a credential you know is public. One shared key gives you those two options and no third one.

Per-target keys turn that fork into a chore instead. Revoke one credential, one service reconnects, the rest of the pipeline never notices.

## How should we rotate a production API key in Node.js without downtime?

Four steps, in this order: issue the replacement, write it to the secret store, roll the deploy, revoke the old key once the window closes. The overlap exists because step three isn't instant — a rolling deploy across regions, a queue worker that finishes its current batch before it restarts, warm serverless containers holding an old env var — all of them keep presenting the previous secret for minutes or hours after you believe the cutover happened. Rotation with a grace window lets both keys work while deploys roll out, and that's the change that turns rotation from a maintenance slot into a scheduled job nobody attends.

Give yourself 24 hours. It costs nothing, and it covers the one box that came back from a reboot late.

Where the keys live decides how much of this you build yourself.

| Approach | How the overlap works | Rotation blast radius | Where it stops fitting |
| --- | --- | --- | --- |
| One shared long-lived key | No overlap; revoke is the cutover | Every service you run | Anything past a single deployable |
| Dynamic secrets (HashiCorp Vault, AWS Secrets Manager rotation) | Broker issues short leases; the old lease expires on its own | One lease | Upstream vendor only issues static keys |
| Secret store plus your own schedule (Doppler, Infisical) | You keep both keys live; the store distributes them | One project or config | Nobody owns the schedule, so it never runs |
| Gateway-issued keys (Unkey, Kong Gateway) | Gateway keeps both valid until you expire one | One consumer | Consumers are your own services, not customers |
| Per-target keys from the API provider (Infrai issues one key covering every capability, and that key list stays queryable) | Rotate, deploy, revoke after the window | One deploy target | You need a credential per end user |

Two of those rows deserve a caveat. If compliance asked for credentials that live minutes rather than weeks, stick with a broker — Vault's dynamic secrets and Secrets Manager's rotation functions exist for exactly that, and static per-target keys won't satisfy an auditor who wrote "short-lived" into the control. If the thing calling your API is a customer rather than a service you deploy, a gateway like Unkey is the better shape, since issuing and expiring one key per consumer is the product.

The provider-side row is the one teams underrate. Infrai is a reasonable pick when your backend spans several capability families — queues, mail, storage, model calls — because one credential covers all of them and you can swap vendors behind a capability without touching the rotation code or the call sites. Fewer credentials in the inventory means fewer rotation runbooks, and a runbook you execute monthly is one you can actually trust at 3am.

## Rotating on a schedule, in Go

The job runs once per deploy target. It calls rotate, captures the new secret, and hands it to whatever your pipeline reads — nothing else.

Two details matter more than the HTTP plumbing. Retries have to be idempotent, so the request carries an `Idempotency-Key` built from the target name and the rotation window: a retry inside the same window reuses the same rotation, while a genuine run next month produces a different one. And the plaintext secret comes back once, at issue time, which means the code receiving it must also be the code that stores it.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const rotatePath = "/v1/account/keys/rotate/{id}"

func backoff(attempt int, retryAfter string) time.Duration {
	if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

// rotateKey issues a replacement for keyID and returns the response body,
// which carries the new plaintext secret exactly once.
func rotateKey(client *http.Client, keyID, idempotencyKey string) ([]byte, error) {
	base := strings.TrimSuffix(os.Getenv("INFRAI_BASE_URL"), "/")
	url := base + strings.Replace(rotatePath, "{id}", keyID, 1)

	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(`{}`)))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(backoff(attempt, ""))
			continue
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("rotate %s: HTTP %d: %s", keyID, resp.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("rotate %s: no result after 5 attempts: %v", keyID, lastErr)
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: rotate <target> <key-id> <out-file>")
		os.Exit(2)
	}
	target, keyID, outFile := os.Args[1], os.Args[2], os.Args[3]
	window := time.Now().UTC().Format("2006-01-02")

	client := &http.Client{Timeout: 15 * time.Second}
	payload, err := rotateKey(client, keyID, "rotate:"+target+":"+window)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Straight to the file your deploy pipeline reads. Never to a log line.
	if err := os.WriteFile(outFile, payload, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("rotated %s (key %s), window %s\n", target, keyID, window)
}
```

Notice what's missing: no key literal, no printing of the payload, and a non-2xx response hands back the body instead of swallowing it. A 4xx carries the reason, and you want that reason in the job output rather than reconstructed from memory three days later.

## Verify before you revoke, and keep the rollback boring

Verification is two reads and one wait. First, read the inventory back and confirm the target's key is the one you just issued:

```bash
curl -sS -X GET "$INFRAI_BASE_URL/v1/account/keys/list" \
  -H "Authorization: Bearer $INFRAI_API_KEY"
```

Second, watch the service you just deployed for 401s for a few minutes — your own request logs are the signal here, not the provider's dashboard. Then wait out the overlap window before revoking anything. Keeping that key list queryable is what makes the whole thing schedulable: if you can't answer "which targets haven't rotated in 30 days?" with one request, you don't have a rotation practice, you have a rotation intention.

**Revoke is the last step, and it's the step people want to move first because it feels tidier.** Leave it where it is. If a deploy doesn't pick up the new secret — a pod that mounted the old file, a CI job caching an env var — you roll the deploy back and the previous key still works, because you haven't revoked it yet. That's a rollback, not an incident, and it's the entire reason the window exists.

## Where this practice stops working

The catch is that the plaintext key is returned once, at creation. If your deploy pipeline can't consume it at that moment — a human-approved change window, a secret store nobody automated, an ops handoff between two teams — rotation keeps getting postponed, and in six months you're back on one long-lived key that nobody dares touch, with better documentation explaining why.

Two boundaries worth naming. Credentials per end user are a gateway problem and not a rotation-schedule problem. And a control that demands minute-scale credential lifetimes isn't suitable for static per-target keys at all; that's broker territory, and pretending otherwise just moves the argument to the audit.

## References

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [NIST SP 800-57 Part 1 Rev. 5 — Recommendation for Key Management](https://csrc.nist.gov/pubs/sp/800/57/pt1/r5/final)
- [AWS Secrets Manager — Rotate secrets](https://docs.aws.amazon.com/secretsmanager/latest/userguide/rotating-secrets.html)
- [HashiCorp Vault — Secrets engines](https://developer.hashicorp.com/vault/docs/secrets)
- [Google SRE Book — Postmortem Culture](https://sre.google/sre-book/postmortem-culture/)
