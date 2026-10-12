# 🧠 Serverless Event-Driven Quiz

5 questions on the patterns from this guide. Click each answer when you're ready — no peeking!

---

**1. SQS delivers messages at-least-once. What breaks if your Lambda handler isn't idempotent?**

<details>
<summary>Reveal answer</summary>

Duplicate processing. A redelivered message re-runs the handler, so a payment could be charged twice or an order fulfilled twice. The guide's `claim_idempotency()` uses a DynamoDB conditional write to make "process exactly once" safe.
</details>

---

**2. Your Lambda is wired to an SQS queue with batches of 10. One message always throws. Without partial batch responses, what happens to the other 9?**

<details>
<summary>Reveal answer</summary>

They get retried forever too — the whole batch fails, SQS redelivers all 10, and the poison message blocks throughput until it lands in the DLQ (after maxReceiveCount). Returning `{"batchItemFailures": [...]}` with only the bad message's ID lets the 9 good ones succeed.
</details>

---

**3. When would you pick EventBridge over SNS for fan-out?**

<details>
<summary>Reveal answer</summary>

When you need content-based routing, not just broadcast. SNS pushes every message to every subscriber; EventBridge rules filter on event payload (`order.created` → payments, `order.created` + `amount > 1000` → fraud review). EventBridge also gives you schema registry, archive/replay, and SaaS integrations.
</details>

---

**4. Your Lambda p99 latency spikes every morning at 9 AM. What's the likely cause and the two standard mitigations?**

<details>
<summary>Reveal answer</summary>

Cold starts after scale-from-zero overnight. Mitigations: (1) **Provisioned concurrency** — keeps N execution environments warm (costs money, kills the spike); (2) **Lambda SnapStart** (Java) or keeping handlers lean / using lighter runtimes (Python, Node) to shrink init time. Also check whether the spike is actually downstream (e.g., DynamoDB throttling) before paying for warm capacity.
</details>

---

**5. True or false: "With a DLQ configured, I don't need CloudWatch alarms on my queue."**

<details>
<summary>Reveal answer</summary>

False — a DLQ without an alarm is a message graveyard nobody visits. The correct setup: DLQ **plus** an alarm on `ApproximateNumberOfMessagesVisible` in the DLQ (or on the source queue's age-of-oldest-message), paging whoever owns the pipeline. Silent failures are the whole reason DLQs exist.
</details>

---

*Score: 5/5 — ship it. 3–4 — re-read the README's routing table. Below 3 — run the handler locally with `sam local invoke` and watch the logs.*
