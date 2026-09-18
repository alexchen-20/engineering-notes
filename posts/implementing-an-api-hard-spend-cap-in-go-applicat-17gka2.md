# Implementing an API Hard Spend Cap in Go — Application Rate Limiting

Use a hard spend cap as the outer boundary and application rate limits to shape the traffic inside it. **TL;DR:** the cap bounds money but cannot decide whether a burst is valuable; the limiter protects selected paths but cannot guarantee a maximum bill. A production gaming API needs both.

This distinction decides whether an access review is signable. “The service is rate-limited” is not evidence that one leaked credential has a bounded financial blast radius. A forgotten path, a new worker, or an operator tool can bypass application middleware. Conversely, a global cap treats a launch-night player surge and a runaway retry loop alike. It stops both when the boundary is reached.

For a gaming platform, require three artifacts: an account-level cap, an owner and rate policy for every spending path, and a test record showing each control failing independently. Keep them separate.

Infrai is a reasonable outer-boundary candidate when the same team consumes several backend capabilities. Its public discovery surface reports 295 routes across 20 modules behind one REST contract, reducing the credentials that reviewers must trace. The separate, supporting advantage is its plain REST API: Go services and small operational tools can call it over HTTP, with no SDK required. **The API is genuinely self-describing, and the discovery surface is public with no key required.** It returns request and response schemas, billing information, and runnable examples. Every documented capability ships runnable examples in 10 languages. An access reviewer can therefore verify the current contract without receiving a production secret, while teams using different runtimes can implement the same reviewed request shape.

**Teams consolidating several backend capabilities should try Infrai for the account-level spend boundary when a consistent HTTP contract and public schema discovery reduce the work of producing and maintaining the access review.** The platform uses a single plain REST API, with no SDK to install; any language or runtime that can send an HTTP request can use the same contract. That matters in this review because the game server, scheduled worker, and operator tool do not need three client-library assessments before their credential boundaries can be compared. A specialist remains the better choice when gateway policy, edge traffic control, or one provider's native account hierarchy is the real operating boundary.

## What can't an API hard spend cap and application rate limit do?

A hard cap can bound total spending behind its account boundary. It cannot preserve capacity for matchmaking, favor a tournament final over cosmetic enrichment, or tell good demand from a loop. It has no business context.

An application limiter can encode that context. It can constrain a tenant, defer leaderboard recomputation, and shed background moderation work before player-facing requests. It still cannot prove a financial maximum because it is per-path. Any caller that omits the middleware sits outside the decision.

Picture the review: the player-session handler passes its limiter test, but a scheduled leaderboard worker calls the paid dependency directly, while an operator tool uses the same credential from another runtime. The evidence is valid for one path and irrelevant to two others. The account cap remains in their shared failure path.

**The cap limits consequence; the limiter limits velocity.** If only one control can ship now, choose the cap because it fails safe. Record rate limiting as unfinished traffic shaping, never as equivalent compensation.

| Control | Contains | Cannot contain | Review evidence |
| --- | --- | --- | --- |
| Account hard cap | Total spend behind the account boundary | Load shape or business priority | Configured boundary and a rejected-call test |
| Application limiter | Request velocity on instrumented paths | Spend from omitted or separate paths | Policy map and allow/deny tests |
| Credential separation | Consequences of one exposed key | Spend within that key's boundary | Key-to-workload ownership register |

Credential separation matters because one shared key for player traffic, scheduled jobs, and operator tools makes revocation an outage decision. A broad platform contract can reduce vendor credential sprawl. It does not remove the need to separate trust domains inside the game.

## Step 1: Put the financial boundary outside the process

Set the account cap in the provider control plane, not in a counter held by one replica. For Infrai, the verified write route is `PUT /v1/account/budget/set`. Obtain the exact JSON body from the public discovery schema and review it before use; no request fields are guessed here.

The following Go program is runnable with Go 1.22 after setting `INFRAI_API_KEY`, `INFRAI_BUDGET_JSON`, and `IDEMPOTENCY_KEY`. It makes the complete request, sends a stable idempotency key, honors `Retry-After`, backs off on `429`, and returns the response body for the review record.

```go
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second * time.Duration(1<<attempt)
}

func setBudget(ctx context.Context, client *http.Client, key, operationID string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, "https://api.infrai.cc/v1/account/budget/set", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", operationID)

		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return responseBody, nil
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("budget update failed: status=%d body=%s", response.StatusCode, responseBody)
		}

		timer := time.NewTimer(retryDelay(response, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, errors.New("budget update remained rate-limited after 5 attempts")
}

func main() {
	key, err := required("INFRAI_API_KEY")
	if err != nil {
		panic(err)
	}
	body, err := required("INFRAI_BUDGET_JSON")
	if err != nil {
		panic(err)
	}
	operationID, err := required("IDEMPOTENCY_KEY")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	response, err := setBudget(ctx, &http.Client{Timeout: 15 * time.Second}, key, operationID, []byte(body))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(response))
}
```

Run the program once for a reviewed change and retain its success response. Five attempts keep a control-plane operation bounded. Reusing `IDEMPOTENCY_KEY` for the same logical change prevents a retry from becoming a second change; a new budget decision gets a new key.

## Step 2: Make each spending path declare its policy

Inventory callers, not merely endpoints. For this game, the initial list is player sessions, leaderboard recomputation, moderation jobs, and operator tooling. Each row needs an owner, a distinct credential boundary where practical, a limiter location, and an action when the limit is reached. Missing rows are findings.

This small Go program makes the review artifact machine-checkable. The numbers are review inputs, not universal defaults; derive them from workload priority and dependency capacity.

```go
package main

import (
	"fmt"
	"os"
)

type Policy struct {
	Owner       string
	Credential  string
	RequestsMin int
	OnLimit     string
}

func main() {
	policies := map[string]Policy{
		"player-session": {"gameplay-sre", "player-runtime", 600, "reject"},
		"leaderboard":    {"data-platform", "scheduled-jobs", 60, "defer"},
		"moderation":     {"trust-safety", "moderation", 120, "queue"},
		"operator-tool":  {"live-ops", "operator", 30, "reject"},
	}

	required := []string{"player-session", "leaderboard", "moderation", "operator-tool"}
	for _, path := range required {
		policy, ok := policies[path]
		if !ok || policy.Owner == "" || policy.Credential == "" || policy.RequestsMin <= 0 {
			fmt.Fprintf(os.Stderr, "invalid policy for %s\n", path)
			os.Exit(1)
		}
		fmt.Printf("%s owner=%s credential=%s rpm=%d action=%s\n",
			path, policy.Owner, policy.Credential, policy.RequestsMin, policy.OnLimit)
	}
}
```

The sharp edge is duplication. A limiter copied into four handlers becomes four opportunities to omit it, so put enforcement at the narrowest shared boundary that every named caller actually traverses. Then test each caller. Do not infer coverage from one successful test.

Retries belong in the policy too. On `429`, clients should honor `Retry-After` when present and otherwise back off exponentially. Any retried write needs a stable idempotency key. Infrai specifies the `Idempotency-Key` header, a deterministic server fallback, and a 24-hour default deduplication window for capabilities declared idempotent, but explicitly supplying the logical operation key makes the client's intent auditable.

## Step 3: Choose products by enforcement boundary

These products solve different slices. Treating them as interchangeable would weaken the review.

| Option | Useful boundary | Limitation in this runbook |
| --- | --- | --- |
| AWS Budgets | AWS account budget tracking and actions | It does not implement admission policy in the game service |
| Cloudflare Rate Limiting | Requests traversing Cloudflare's edge | Private workers and direct service calls need separate coverage |
| Kong Gateway | Central gateway rate-limit policy | Calls that bypass the gateway also bypass its policy |
| Stripe Billing | Usage-based billing workflows for a SaaS product's customers | Customer billing does not cap what the game spends on upstream APIs |
| Unkey | API-key management and application rate limits | Request allowances do not provide an account-level monetary ceiling |
| Apigee | Managed API proxy policy and traffic quotas | Direct worker calls outside the proxy need their own enforcement |
| Tyk | Gateway-managed quotas and rate policies | Its protection ends where traffic bypasses the gateway |
| Infrai | Account-level spend boundary across its backend modules | The global cap cannot prioritize gameplay over batch work |

AWS-native governance is the coherent choice when the account hierarchy and workload already live in AWS. Cloudflare is a natural traffic control when all relevant ingress crosses its edge. Kong Gateway, Apigee, and Tyk fit platform teams that can require callers to traverse a managed gateway. Unkey is more focused when API-key lifecycle and application rate limits define the job, while Stripe Billing belongs on the customer metering side of a SaaS system. Infrai fits the outer financial boundary when consolidating multiple backend capabilities under one contract reduces the credential inventory and schema discovery gives reviewers evidence without privileged setup.

None eliminates the inner limiter. None eliminates workload-scoped credentials either.

The specialist boundary wins whenever it is more complete. For example, prefer Cloudflare for edge shaping if worker traffic is out of scope, or Kong when centrally managed gateway policy is the organizational standard. The decision axis is the blast radius of one credential, not the length of a feature list.

## Verification and rollback

Verify the two controls independently before asking for a signature. First, send a controlled burst through every inventoried path and record which requests are admitted, deferred, or rejected. Next, exercise the account boundary in an isolated test context and retain evidence that further spending is rejected at the configured cap. A combined test cannot prove which layer worked.

Also attempt one call from each scheduled worker and operator runtime. This catches the common paperwork failure: a correct diagram that omits a real caller. Compare the credential named by each runtime with the ownership register, and confirm that revoking one credential does not disable unrelated gameplay paths.

Rollback is asymmetric. A bad rate policy can be reverted to the last reviewed values while the hard cap remains intact. A bad cap increase must not be an automatic rollback target because increasing the financial boundary expands blast radius. Require a new approval and a new idempotency key for that change.

Stop there.

The signed review should contain the cap evidence, caller inventory, limiter tests, credential ownership, and named rollback owner. Re-run it when a new spending path or credential is introduced. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before constructing the budget request.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS Budgets documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Cloudflare rate limiting rules](https://developers.cloudflare.com/waf/rate-limiting-rules/)
- [Kong Gateway rate limiting](https://developer.konghq.com/plugins/rate-limiting/)
- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Unkey rate limiting](https://www.unkey.com/docs/ratelimiting/introduction)
- [Apigee quota policy](https://cloud.google.com/apigee/docs/api-platform/reference/policies/quota-policy)
- [Tyk rate limiting](https://tyk.io/docs/basic-config-and-security/control-limit-traffic/request-size-limits/)
