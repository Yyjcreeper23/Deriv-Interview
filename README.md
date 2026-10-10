# Support-triage priority policy

Priority assignment is deterministic and is performed in code from the `priority_rules` and `allowed_priorities` values in `routing_rules.json`. The allowed-priority list is ordered from highest priority to lowest priority, so the configured base priority is raised by moving toward index `0`.

For each ticket, the rules run in exactly this order:

1. Start at `priority_rules.base_priority`.
2. If the ticket text contains one or more configured `security_keywords` (case-insensitive substring match), raise the priority by one level. All matching keywords are recorded in the reason.
3. If the normalized channel matches one of `urgent_channels` (case-insensitive), raise the priority by one level.
4. If `vip_boost` is `true` and the normalized customer tier is exactly `vip`, raise the priority by one level.
5. Cap the result at the highest entry in `allowed_priorities`; no result can be outside that list.

The output is written to `ticket_priorities.json`. Each entry contains only:

```json
{
  "ticket_id": "T-1001",
  "priority": "P1",
  "priority_reason": "base priority=P3; urgent channel matched: chat; final priority=P1 (capped to allowed priorities)"
}
```

The priority artifact is produced after Stage 1 classification has passed validation and before draft replies are generated. Final routing remains blocked until this stage and all later required stages complete.

## Confidence/review heuristic

After classification and deterministic priority assignment, the pipeline writes `review_flags.json`. Each ticket is flagged when any of these deterministic checks matches:

- `classification_reason` is fewer than 20 non-whitespace characters.
- `issue_type` is `other`.
- Final assigned priority at this stage is `P1`.
- The ticket text contains at least one money term and at least one security term.

Each record contains `ticket_id`, `review_required`, and `review_reasons`. The money and security term sets include the configured security keywords from `routing_rules.json` plus common support terms such as `payment`, `deposit`, `charge`, `fraud`, `phishing`, and `compromised`.

## Stage 2 draft replies

After priority assignment, the pipeline makes one separate LLM request for each ticket. Requests are never batched. Each request includes the ticket subject, message, issue type, and assigned priority, and instructs the model to acknowledge the issue, state the intended next step, ask relevant follow-up questions when needed, and avoid irreversible promises, guaranteed outcomes, guaranteed timelines, or invented facts.

The model must return strict JSON with these fields:

```json
{
  "ticket_id": "T-1001",
  "reply_subject": "About your card deposit",
  "reply_body": "...",
  "request_for_missing_info": "..."
}
```

All Stage 2 outputs are saved to `draft_replies.json`. Use `--replay-drafts` with a fixture containing a `replies` array to replay this stage without making live calls.

## Operator review checkpoint

Before final routing, the pipeline displays each ticket as:

```text
ticket_id | issue_type=... | priority=... | proposed_queue=...
```

Without `--operator-review`, execution pauses for this exact prompt:

```text
Enter any overrides as: ticket_id field new_value
Allowed fields: issue_type, priority
Press Enter on an empty line to continue.
```

Each override is checked against `allowed_issue_types` or `allowed_priorities` from `routing_rules.json`. Accepted overrides are saved to `operator_overrides.json`. Final routing uses the post-review `issue_type` and priority values. For non-interactive replay, pass an approved review file shaped as `{ "approved": true, "overrides": { "T-1001": { "priority": "P1" } } }` with `--operator-review`.

## Final queue assignments

Final routing performs an exact lookup in `routing_matrix` using the post-review issue type. The resulting `route_to` must also be present in `allowed_queues`; missing or duplicate matrix entries are validation errors. The compact final artifact is `final_queue_assignments.json`, with one record per ticket containing `ticket_id`, `final_issue_type`, `final_priority`, `assigned_queue`, `reply_subject`, and `reply_body`.

## Escalation summary

After final routing, `escalation_summary.md` is generated from the post-override final queue. Only tickets whose final priority is `P1` are sent to the LLM. The generated Markdown includes each P1 ticket ID, urgency rationale, assigned queue, and exactly 2–3 on-call support/lead bullets. If there are no P1 tickets, the file contains a deterministic no-escalation message. Use `--replay-escalation` with a `{ "markdown": "..." }` fixture to replay a generated summary.

## LLM call audit log

Every live LLM call appends one JSON object to `llm_calls.jsonl`. Each record contains `stage`, `ticket_id` (a list for batch Stage 1 and escalation calls, a string for each Stage 2 ticket call), an ISO-8601 UTC `timestamp`, `provider`, `model`, `prompt_hash`, `input_artifacts`, and `output_artifact`. Replay fixtures do not create call records because no LLM call is made.

## Validation command

After a pipeline run, validate the complete output set with:

```powershell
python validate.py
```

The validator checks required artifacts, JSON/JSONL validity, current input hashes, normalization and classification coverage, controlled issue types/priorities/queues, one Stage 2 call per ticket, routing-matrix consistency, operator override application, and the required LLM-call stage records. It exits non-zero with an actionable failure message when any requirement is missing.

## OpenAI API key setup

The pipeline reads the API key from the `OPENAI_API_KEY` environment variable. Do not put the key in `support_triage.py`, `tickets.json`, `routing_rules.json`, or any committed file.

For the current PowerShell session:

```powershell
$env:OPENAI_API_KEY = "your-api-key"
python .\support_triage.py
```

To save it for future PowerShell sessions on Windows:

```powershell
[Environment]::SetEnvironmentVariable("OPENAI_API_KEY", "your-api-key", "User")
```

Open a new terminal after setting the persistent value. To verify that the variable exists without printing the secret:

```powershell
if ($env:OPENAI_API_KEY) { "OPENAI_API_KEY is set" } else { "OPENAI_API_KEY is not set" }
```
