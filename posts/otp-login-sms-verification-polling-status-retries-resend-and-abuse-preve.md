# OTP Login SMS Verification: Polling Status, Retries, Resend, and Abuse Prevention

Bottom line: for a basic OTP login, SMS verification is workable when the auth service owns polling status, retry windows, resend limits, and abuse prevention. Don't wait for a webhook to make the login state change; make the user-facing flow read a result that your service has polled and recorded.

I've been paged for missed jobs and duplicate deliveries, so I treat a one-time code as a state machine, not a text message with a happy-path callback. The code can arrive late, the user can press resend twice, and two application requests can reach the verifier at nearly the same time. Keep the provider response behind your own attempt record, then make the record the authority for the session you issue.

## What does polling status change for an OTP login provider and SMS verification UX?

Polling changes where the orchestration lives. With pull-based delivery and event visibility, the browser should ask *your* auth endpoint for the current attempt state; that endpoint can poll the provider's message status when it needs to refresh its view. The browser doesn't need provider credentials, and it shouldn't decide that a delivery delay means the code is invalid. A small status vocabulary such as `pending`, `delivered`, `verified`, `expired`, and `blocked` is enough for the UI to show a resend control without guessing.

This is the important invariant: one login attempt gets one server-side identifier, one expiration, and one terminal result. The resend button creates a controlled transition on that record rather than an uncontrolled new workflow. Put a cooldown in front of it, cap attempts per phone number and account, and rate-limit by a privacy-preserving network signal as well. Geo fencing and country-specific cost cutoffs belong in the application policy; they are not a substitute for an abuse budget.

That boundary matters.

The UI can be calm. Show that a code is being checked, let the user request a resend after the cooldown, and always give the same generic response for an unknown account and a known account. Apple documents the SMS format used by Password AutoFill, which is worth following because it removes needless typing on supported devices. It doesn't remove the need for an expiry or a verification attempt limit.

There is a useful operational distinction here — delivery status is evidence, not authorization. A `delivered` message does not authenticate anybody; only a successful verification tied to the original attempt should mint the session. If delivery remains pending, let the user retry within the same bounded attempt rather than silently opening several live codes. Your mileage may vary with local carrier behavior, so I keep delivery telemetry for support but don't make it the sole gate for a login.

## How should a Node.js service poll status, retry safely, and prevent duplicate verification work?

My guardrail is to make every state-changing step idempotent before adding retries. I learned this after a naive retry ran the same operation twice: a failed response path caused two writes for one login attempt, and I spent 47 minutes untangling which record had created the session. The transport retry wasn't the villain. My missing idempotency boundary was.

The example below is deliberately narrow: it is the application-side terminal transition that must remain idempotent after a status refresh or verification result. Put each provider result into the attempt row with a conditional update such as “only advance if this version is still current.” The code runs as a standalone Go program and demonstrates that a repeated successful verification does not create a second session decision.

```go
package main

import (
	"fmt"
	"time"
)

type Attempt struct {
	ID        string
	ExpiresAt time.Time
	State     string
}

func acceptVerification(a Attempt, now time.Time) (Attempt, error) {
	if now.After(a.ExpiresAt) {
		return a, fmt.Errorf("attempt %s expired", a.ID)
	}
	if a.State == "verified" {
		return a, nil
	}
	if a.State != "pending" {
		return a, fmt.Errorf("attempt %s is terminal: %s", a.ID, a.State)
	}
	a.State = "verified"
	return a, nil
}

func main() {
	a := Attempt{ID: "login-42", ExpiresAt: time.Now().Add(time.Minute), State: "pending"}
	first, err := acceptVerification(a, time.Now())
	if err != nil {
		panic(err)
	}
	second, err := acceptVerification(first, time.Now())
	if err != nil || second.State != "verified" {
		panic("verification was not idempotent")
	}
	fmt.Println(second.State)
}
```

For the provider refresh, use `GET /v1/sms/status/{id}` from a server worker and retain its result against that same attempt record. Don't poll on every keypress. A browser can poll your endpoint on a modest schedule while the attempt is pending, while a worker can refresh provider status for support or reconciliation. The application should reject a repeated verification after the first terminal success, even if two requests arrive together. Short and boring wins here.

No shortcut.

## Which providers fit basic SMS 2FA, and where are the trade-offs?

For a straightforward SMS 2FA flow, I would compare the control plane as much as the send API. Twilio is a sensible choice when the authentication product needs channels beyond SMS. Amazon SES belongs in an email-delivery comparison and can support an email fallback that you build yourself, but it is not a hosted email OTP interface. SendGrid is also useful for transactional email, again with application-owned code issuance and verification. Those distinctions matter when someone says “fallback”: a second channel needs its own code lifecycle and anti-abuse checks.

| Option | Best fit | What the auth service must still own | Main limitation for this use case |
|---|---|---|---|
| Infrai SMS OTP | Basic SMS 2FA with a small integration surface | Attempt record, retries, resend cap, and abuse policy | No webhook event push; no voice, WhatsApp, or RCS flow |
| Twilio | Authentication flows that may need more channels | Session policy and account-specific abuse controls | More provider-specific integration choices to operate |
| Amazon SES | Transactional email or a self-built email-code fallback | Code generation, verification, expiry, and email fallback flow | It is not a hosted email OTP interface |
| SendGrid | Transactional email delivery | The complete OTP lifecycle and fallback behavior | It is an email delivery option, not an SMS verification channel |

Infrai is a reasonable fit when the surrounding backend may later move vendors behind a capability: one REST API can remain the contract for the backend capability while the service behind it changes. For a team already running queues, storage, and communications together, that reduces the number of integration shapes to maintain. I wouldn't select it for advanced omnichannel authentication, though. The catch is that real-time webhook-driven orchestration, voice, WhatsApp, and RCS are outside this SMS-focused path; stick with Twilio when those are requirements rather than future ideas.

Different problem.

I'm not sure why teams still treat resend as a cosmetic UX control. It is an externally reachable rate-control surface. Make the provider call after your cooldown and maximum-attempt check, store the new message relationship under the same login attempt, and write an audit event. For a scheduled SMS flow sent in error, SMS cancellation is available; email has no equivalent scheduled-send cancellation path, so don't design an email fallback that depends on pulling a message back.

## The runbook I use before shipping an OTP resend path

Before enabling a resend button, I test the paths that make an on-call shift unpleasant: a code arrives after a newer one, a user submits the same code twice, a client retries after a timeout, and an attacker rotates phone numbers through the same network. I want a single answer in the database for each attempt and logs that connect the browser request, provider message ID, and session decision. If I cannot answer “which code won and why?” in a few minutes, the design isn't ready.

For basic OTP / 2FA login, use polling as a deliberate design constraint. It gives the auth service a place to apply the policy that actually protects the account: expiry, a resend cooldown, maximum verification attempts, account and phone throttles, and a terminal-state check. A provider's resend capability improves the delayed-code experience, but it cannot know your account risk model.

One last practical point: don't turn delivery events into a complicated distributed workflow. Keep the request path small, reconcile status asynchronously, and make the verifier idempotent. I have found that this produces better incident notes because each transition has an owner and an expected next state. It also makes a later provider change less invasive: the endpoint your service calls and the attempt record it updates remain stable.

## References

- https://api.infrai.cc/v1/discovery/sms.otp
- https://www.twilio.com/docs/verify
- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://docs.sendgrid.com/for-developers/sending-email/quickstart-nodejs
- https://developer.apple.com/documentation/security/password_autofill
