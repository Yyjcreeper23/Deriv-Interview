# Support-triage pipeline

This tool reads raw customer-support tickets from `tickets.json`, normalizes and validates them, classifies each ticket into the controlled taxonomy from `routing_rules.json`, assigns a deterministic priority, drafts a safe agent reply, pauses for operator review, and produces the final routed work queue.

It is designed for replayable, auditable support operations: each major stage writes an artifact, routing is derived from code and the routing matrix, operator overrides are preserved, review heuristics are explicit, and live LLM calls are recorded in `llm_calls.jsonl`.

The pipeline stages are:

`INIT` → `INPUTS_LOADED` → `TICKETS_NORMALISED` → `STAGE1_CLASSIFIED` → `PRIORITISED_ASSIGNED` → `DRAFT_REPLIES_GENERATED` → `OPERATOR_REVIEW_COMPLETE` → `FINAL_ROUTING_APPLIED` → `OPTIONAL_OUTPUTS_GENERATED` → `VALIDATION_COMPLETE` → `RESULTS_FINALISED`

Run the pipeline with:

```powershell
python .\support_triage.py
```

For a complete artifact and policy check after a run:

```powershell
python .\validate.py
```

## Design specifications

### Purpose and operating model

The tool is a file-based, replayable support-triage pipeline. It separates probabilistic language-model work from deterministic control logic:

- LLMs classify tickets, draft replies, and write the P1 escalation summary.
- Python code owns normalization, schema validation, priority assignment, review flags, operator overrides, routing, artifact creation, and final validation.
- Each run is inspectable from its JSON, Markdown, CSV, manifest, and JSONL artifacts.

### Inputs

- `tickets.json`: a JSON array of raw ticket objects. Extra source fields are allowed and ignored by normalization.
- `routing_rules.json`: controlled issue types, priorities, queues, routing matrix, and priority rules.
- Optional replay fixtures: Stage 1 classifications, Stage 2 replies, escalation Markdown, or an approved operator-review file.

### Stage contracts

| Stage | Contract |
| --- | --- |
| Input loading | Read both input files from disk and validate their top-level shapes. |
| Normalization | Produce one stable record per ticket with the required normalized fields and word count before any LLM call. |
| Stage 1 classification | Make one batch LLM call; return exactly one controlled issue type and classification record per normalized ticket. |
| Priority assignment | Apply the configured base, security, urgent-channel, and VIP rules in that order; cap to `allowed_priorities`. |
| Review heuristic | Emit deterministic `review_flags.json` for short reasons, `other`, P1, and money-plus-security cases. |
| Stage 2 drafting | Make one separate LLM call per ticket and save strict JSON reply outputs. |
| Operator review | Display the proposed queue, pause for review, validate `issue_type`/`priority` overrides, and persist them. |
| Final routing | Look up the post-override issue type exactly once in `routing_matrix`; reject missing, duplicate, or unsupported routes. |
| Optional outputs | Generate the P1 escalation summary and CSV output. |
| Validation/finalization | Verify artifacts, controlled values, input hashes, call logs, and final routing before marking results final. |

### Required invariants

1. Final routing cannot run until normalization, Stage 1 classification, priority assignment, Stage 2 drafting, and operator review complete.
2. Every normalized ticket has exactly one classification, priority, draft reply, review flag, and final assignment.
3. No issue type, priority, or queue may be invented outside `routing_rules.json`.
4. Final assignments use post-override values, not the original model output.
5. A live Stage 2 call is made independently for every ticket; calls are not batched.
6. Every live LLM call is auditable in `llm_calls.jsonl` with a timestamp, model, prompt hash, input artifacts, and output artifact.

### Artifact layout

All generated artifacts are written under the configured output directory, `outputs/` by default. The key handoff artifacts are `normalized_tickets.json`, `ticket_classification.json`, `ticket_priorities.json`, `review_flags.json`, `draft_replies.json`, `operator_overrides.json`, `final_queue_assignments.json`, `escalation_summary.md`, and `pipeline_manifest.json`.

## Deterministic priority policy

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
