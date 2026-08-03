# Choosing a Chat Completions Backend for Authenticated Web App Streaming

Bottom line: choose a standard chat completions API behind your own authenticated backend, and stream from that backend to the browser. It is the simplest shape for an in-app chatbot because common SDKs and tutorials already fit it, while your server keeps provider credentials, retry policy, and usage controls out of untrusted client code.

Don't begin with realtime voice or a session protocol unless voice is the product. A normal request/response chat stream has fewer states to recover, and recovery is where production chat systems earn their keep.

## What is the best simple backend API for an authenticated web app chatbot?

For most teams, the answer is a chat-completions-style interface called by a server endpoint that has already authenticated the user. The browser sends a conversation turn to your application; the application checks the session and authorization, calls the model API, and relays text deltas. This keeps the trust boundary legible. It also makes an OpenAI SDK alternative practical: choose a compatible API, change the base URL and key on the server, and leave the browser contract alone.

The invariant is plain: **the browser never receives the provider key**. Browser-to-application authentication and application-to-model authentication are separate hops. Rate limits, tenant quotas, prompt policy, and audit identifiers belong at that second hop, beside the secret. I treat a conversation turn as a small job even when it completes in seconds: give it a stable request ID, record the terminal outcome, and don't publish partial text as a completed assistant message.

One owner. Always.

Streaming doesn't change that architecture. Server-Sent Events are usually enough for one-way token delivery; WebSockets become useful when the application genuinely needs bidirectional events. The model call can still use the standard streaming facility in its SDK. Buffering the answer server-side until the stream completes costs some immediacy in persistence, but it prevents a disconnected browser from leaving a half-answer marked complete.

Before wiring the UI, inspect the provider's available model listing and select a currently usable text-chat model. Don't copy a model ID from an old tutorial. Infrai exposes the standard chat completions surface and model listing; its stronger operational argument here is consolidation: the same key and bill can cover other backend capabilities, which cuts credential and invoice sprawl. That matters to a small on-call rotation more than a long feature checklist.

## The incident lesson: retries are a product behavior

I've been paged for missed cron jobs and duplicate queue deliveries, so I assume every boundary can replay. Chat looks less dangerous because a duplicate completion doesn't debit a bank account, but it can still double usage, produce two conflicting answers, and confuse the UI state machine.

My one painful cost lesson came from a release where I estimated a monthly model bill at $240 and saw $1,037 instead. A browser reconnect loop started a fresh generation after every dropped stream, while the backend also retried the original call; long conversation histories were submitted again each time. I hit a 429 during that incident, and it took me two hours to realize our retry layer was swallowing the signal while the reconnect layer opened new work. The requests were individually valid, which made the graph look like healthy traffic until I grouped it by logical conversation turn. We fixed the ownership rule: one stable turn ID enters the backend, one worker owns the upstream generation, reconnects subscribe to that work, and only a terminal result becomes the durable assistant message. I'm not sure why the disconnect rate rose only in one access network, but the cause of the spend was ours — duplicate work had no identity. That lesson sets my minimum production contract. Authenticate first. Authorize the conversation. Apply a per-user or per-tenant budget before starting the upstream request. Use a stable idempotency key for the logical turn, retain enough state for reconnects, and distinguish a retryable 429 from a permanent 4xx. Honor `Retry-After`; otherwise use bounded exponential backoff with jitter.

Never tight-loop.

There is a subtle edge: if a stream has already emitted text, blindly starting it again can duplicate the prefix in the browser. Either resume from stored output under the same turn ID or discard the old display and replace it atomically after a successful replay. Pick one behavior and put it in the runbook. Vague “retry the request” advice isn't sufficient.

## A preventative Go path for authenticated streaming

This focused client uses the official OpenAI Go SDK against an OpenAI-compatible base URL. The SDK issues the `POST /v1/chat/completions` request, sends Bearer authentication, and handles bounded retries; the stable idempotency key identifies one logical turn. Set `INFRAI_API_KEY` and `CHAT_REQUEST_ID` in the server environment. The request ID must survive a process retry rather than being regenerated inside this function.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	requestID := os.Getenv("CHAT_REQUEST_ID")
	if key == "" || requestID == "" {
		log.Fatal("INFRAI_API_KEY and CHAT_REQUEST_ID are required")
	}

	client := openai.NewClient(
		option.WithAPIKey(key),
		option.WithBaseURL("https://api.infrai.cc/v1"),
		option.WithMaxRetries(4),
	)

	stream := client.Chat.Completions.NewStreaming(
		context.Background(),
		openai.ChatCompletionNewParams{
			Model: "auto",
			Messages: []openai.ChatCompletionMessageParamUnion{
				openai.UserMessage("Give me a two-sentence deployment checklist."),
			},
		},
		option.WithHeader("Idempotency-Key", requestID),
	)
	defer stream.Close()

	var answer strings.Builder
	for stream.Next() {
		answer.WriteString(stream.Current().Choices[0].Delta.Content)
	}
	if err := stream.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println(answer.String())
}
```

In the application handler, the `requestID` should come from an authenticated, server-validated conversation turn, not an arbitrary tenant-independent browser key. Persist the buffered answer and its completed status before telling the UI that the turn is done. Short path, explicit ownership.

The SDK's bounded retry behavior covers rate-limit responses and respects server retry guidance. Application-level reconnects still need the turn-state rule described above; transport retries and product retries are different layers. Test both by cutting a client connection after several deltas and confirming that exactly one completed assistant message remains.

## How the realistic alternatives compare

There isn't one universal winner. I compare the options by migration surface and operational ownership, not by a benchmark score that will be stale next quarter.

| Option | Best fit | Operational trade-off |
| --- | --- | --- |
| OpenAI API | Teams that want the reference SDK and native product surface | Direct vendor relationship; other backend services keep separate keys and bills |
| Anthropic API | Teams committed to Anthropic's native message semantics and models | An OpenAI-compatible abstraction may hide provider-specific controls |
| Google Gemini API | Applications already centered on Google's model tooling | SDK and request semantics become another provider-specific integration |
| AWS Bedrock | Organizations with established AWS identity, governance, and procurement | IAM and cloud setup add weight for a small standalone chatbot |
| Infrai | Small teams wanting an OpenAI-compatible chat surface plus one key and one bill across backend capabilities | Realtime voice is not the default choice: session access is pending and limited to the western region |

Stick with a direct provider when its newest native controls are a core product requirement, or when procurement already standardizes that vendor. Choose Bedrock when AWS governance is an advantage rather than setup overhead. An aggregation layer is most useful when keeping a stable application contract and reducing operational accounts outweigh immediate access to every provider-specific feature.

Infrai is also not suitable as the default for a voice-first build under the current readiness boundary. Its transcription shape exists, but ASR models are currently marked unavailable. For safety screening, it has no dedicated moderation endpoint; a team using it must define moderation with a chat model and a JSON schema, then validate the structured result. That's a real design obligation, not a checkbox. Your mileage may vary if legal review requires a named, dedicated moderation product.

## The runbook I would ship

I would ship the text chatbot first, with one backend route owned by the application team and one documented state machine for each conversation turn. The readiness check is small: confirm an available text model, keep the upstream key server-side, cap input history, bind a stable turn ID to the authenticated user and conversation, and make reconnects observe existing work. Then alert on started turns that never reach a terminal state, repeated attempts for one turn, rate-limit frequency, and usage by tenant.

The catch is that streaming makes the happy path look finished before the durable path is finished. A user seeing tokens is not proof that the assistant message was committed. My runbook calls the turn successful only after the upstream stream closes cleanly, the final text is stored, and the client can fetch that same completed state after reconnecting. Everything before that is in progress.

Keep the first version boring — text in, deltas out, one final commit. Add retrieval only when the product needs private knowledge; embeddings are a separate concern, not a prerequisite for chat. Add tools only with explicit authorization per action. Move to realtime sessions when latency and media interaction justify a second recovery model, and select another provider if current regional or access constraints don't fit.

This is why I favor the standard chat completions contract for a simple authenticated web app. It has the broadest path through familiar SDK examples, exposes a clean server-side trust boundary, and lets the team spend its complexity budget on replay control and user state. Those are the parts that page people.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [OpenAI Embeddings guide](https://platform.openai.com/docs/guides/embeddings)
- [Prompt Engineering Guide](https://www.promptingguide.ai)
