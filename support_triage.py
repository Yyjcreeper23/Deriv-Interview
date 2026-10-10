"""Replayable customer-support triage pipeline.

The pipeline is intentionally file-oriented: every stage has a JSON artifact,
and the state machine prevents final routing from running out of order.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


class Stage(str, Enum):
    INIT = "INIT"
    INPUTS_LOADED = "INPUTS_LOADED"
    TICKETS_NORMALISED = "TICKETS_NORMALISED"
    STAGE1_CLASSIFIED = "STAGE1_CLASSIFIED"
    PRIORITISED_ASSIGNED = "PRIORITISED_ASSIGNED"
    DRAFT_REPLIES_GENERATED = "DRAFT_REPLIES_GENERATED"
    OPERATOR_REVIEW_COMPLETE = "OPERATOR_REVIEW_COMPLETE"
    FINAL_ROUTING_APPLIED = "FINAL_ROUTING_APPLIED"
    OPTIONAL_OUTPUTS_GENERATED = "OPTIONAL_OUTPUTS_GENERATED"
    VALIDATION_COMPLETE = "VALIDATION_COMPLETE"
    RESULTS_FINALISED = "RESULTS_FINALISED"


STAGE_ORDER = list(Stage)
REQUIRED_NORMALISED_FIELDS = (
    "ticket_id", "customer_tier", "channel", "subject", "message",
    "combined_text", "message_word_count",
)
REQUIRED_CLASSIFICATION_FIELDS = (
    "ticket_id", "issue_type", "classification_reason",
    "customer_intent", "needs_human_attention",
)


@dataclass(frozen=True)
class Inputs:
    tickets: list[Mapping[str, Any]]
    routing_rules: Mapping[str, Any]
    allowed_issue_types: tuple[str, ...]


class Pipeline:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.stage = Stage.INIT

    def advance(self, next_stage: Stage) -> None:
        expected = STAGE_ORDER[STAGE_ORDER.index(self.stage) + 1]
        if next_stage is not expected:
            raise RuntimeError(f"Invalid stage transition: {self.stage} -> {next_stage}; expected {expected}")
        self.stage = next_stage

    def require(self, required: Stage) -> None:
        if STAGE_ORDER.index(self.stage) < STAGE_ORDER.index(required):
            raise RuntimeError(f"Stage {required} is required; current stage is {self.stage}")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    temporary.replace(path)


def log_llm_call(log_path: Path, *, stage: str, ticket_id: str | list[str], model: str,
                 prompt: Mapping[str, Any], input_artifacts: list[str], output_artifact: str) -> None:
    record = {
        "stage": stage,
        "ticket_id": ticket_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "provider": "openai_responses_api",
        "model": model,
        "prompt_hash": hashlib.sha256(json.dumps(prompt, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest(),
        "input_artifacts": input_artifacts,
        "output_artifact": output_artifact,
    }
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def load_inputs(tickets_path: Path, rules_path: Path) -> Inputs:
    tickets = read_json(tickets_path)
    rules = read_json(rules_path)
    if not isinstance(tickets, list):
        raise ValueError("tickets.json must contain a JSON array")
    if not isinstance(rules, dict) or not isinstance(rules.get("allowed_issue_types"), list):
        raise ValueError("routing_rules.json must contain allowed_issue_types as an array")
    allowed = tuple(item for item in rules["allowed_issue_types"] if isinstance(item, str))
    if not allowed:
        raise ValueError("routing_rules.json has no allowed issue types")
    return Inputs(tickets=tickets, routing_rules=rules, allowed_issue_types=allowed)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalise_tickets(tickets: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, source in enumerate(tickets):
        if not isinstance(source, Mapping):
            raise ValueError(f"ticket at index {index} must be an object")
        ticket_id = _text(source.get("ticket_id"))
        if not ticket_id:
            raise ValueError(f"ticket at index {index} has no ticket_id")
        if ticket_id in seen:
            raise ValueError(f"duplicate ticket_id: {ticket_id}")
        seen.add(ticket_id)
        subject = _text(source.get("subject"))
        message = _text(source.get("message"))
        combined = f"{subject}\n{message}".strip()
        result.append({
            "ticket_id": ticket_id,
            "customer_tier": _text(source.get("customer_tier")),
            "channel": _text(source.get("channel")),
            "subject": subject,
            "message": message,
            "combined_text": combined,
            "message_word_count": len(re.findall(r"\S+", message)),
        })
    return result


def _classification_schema(allowed: Sequence[str]) -> dict[str, Any]:
    non_blank = [item for item in allowed if item]
    return {
        "type": "object", "additionalProperties": False,
        "properties": {"tickets": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "properties": {
                "ticket_id": {"type": "string"},
                "issue_type": {"type": "string", "enum": non_blank},
                "classification_reason": {"type": "string"},
                "customer_intent": {"type": "string"},
                "needs_human_attention": {"type": "boolean"},
            }, "required": list(REQUIRED_CLASSIFICATION_FIELDS),
        }}}, "required": ["tickets"]
    }


def classify_once(normalised: Sequence[Mapping[str, Any]], allowed: Sequence[str],
                  *, replay_path: Path | None = None, llm_log_path: Path | None = None) -> list[dict[str, Any]]:
    """Make exactly one live Stage 1 call, or load one replay fixture."""
    if replay_path is not None:
        payload = read_json(replay_path)
    else:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for Stage 1; use --replay-classification for a replay")
        prompt = {
            "allowed_issue_types": list(allowed),
            "tickets": list(normalised),
            "instruction": "Classify every ticket exactly once. Use only a non-empty allowed issue_type. Return no extra tickets.",
        }
        model = os.environ.get("SUPPORT_TRIAGE_MODEL", "gpt-5-mini")
        request_body = {
            "model": model,
            "input": [{"role": "system", "content": "You classify support tickets into a controlled taxonomy."},
                      {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)}],
            "text": {"format": {"type": "json_schema", "name": "ticket_classification",
                                  "strict": True, "schema": _classification_schema(allowed)}}
        }
        request = urllib.request.Request(
            os.environ.get("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses"),
            data=json.dumps(request_body).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                response_json = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Stage 1 LLM call failed ({exc.code}): {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Stage 1 LLM call failed: {exc.reason}") from exc
        output_text = response_json.get("output_text")
        if not output_text:
            parts = [item.get("text", "") for item in response_json.get("output", [])
                     if isinstance(item, dict) for item in item.get("content", [])
                     if isinstance(item, dict)]
            output_text = "".join(parts)
        if not output_text:
            raise ValueError("Stage 1 response contained no JSON output")
        payload = json.loads(output_text)
        if llm_log_path is not None:
            log_llm_call(
                llm_log_path,
                stage="stage1_classification",
                ticket_id=[str(ticket["ticket_id"]) for ticket in normalised],
                model=model,
                prompt=prompt,
                input_artifacts=["normalized_tickets.json", "routing_rules.json"],
                output_artifact="ticket_classification.json",
            )
    classifications = payload.get("tickets") if isinstance(payload, dict) else payload
    if not isinstance(classifications, list):
        raise ValueError("classification response must contain a tickets array")
    return [dict(item) for item in classifications]


def validate_classifications(items: Sequence[Mapping[str, Any]], tickets: Sequence[Mapping[str, Any]], allowed: Sequence[str]) -> None:
    ids = [item.get("ticket_id") for item in items]
    expected = [ticket["ticket_id"] for ticket in tickets]
    if set(ids) != set(expected) or len(ids) != len(expected) or len(set(ids)) != len(ids):
        raise ValueError("classification must contain exactly one result for every ticket")
    allowed_non_blank = set(allowed) - {""}
    for item in items:
        missing = set(REQUIRED_CLASSIFICATION_FIELDS) - set(item)
        if missing or item["issue_type"] not in allowed_non_blank or not isinstance(item["needs_human_attention"], bool):
            raise ValueError(f"invalid classification for {item.get('ticket_id')}: {sorted(missing)}")


def _reply_schema() -> dict[str, Any]:
    return {
        "type": "object", "additionalProperties": False,
        "properties": {
            "ticket_id": {"type": "string"},
            "reply_subject": {"type": "string"},
            "reply_body": {"type": "string"},
            "request_for_missing_info": {"type": "string"},
        },
        "required": ["ticket_id", "reply_subject", "reply_body", "request_for_missing_info"],
    }


def _response_output_text(response_json: Mapping[str, Any]) -> str:
    output_text = response_json.get("output_text")
    if output_text:
        return str(output_text)
    parts = [item.get("text", "") for item in response_json.get("output", [])
             if isinstance(item, dict) for item in item.get("content", [])
             if isinstance(item, dict)]
    return "".join(parts)


def generate_escalation_summary(final_queue: Sequence[Mapping[str, Any]], *, replay_path: Path | None = None,
                                llm_log_path: Path | None = None) -> str:
    """Generate Markdown for the P1 tickets in the final, post-override queue."""
    p1_tickets = [item for item in final_queue if item.get("priority") == "P1"]
    if not p1_tickets:
        return "# Escalation Summary\n\nNo P1 tickets require escalation.\n"
    if replay_path is not None:
        payload = read_json(replay_path)
        markdown = payload.get("markdown") if isinstance(payload, Mapping) else payload
        if not isinstance(markdown, str):
            raise ValueError("escalation replay must contain a markdown string")
    else:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for escalation summary generation; use --replay-escalation for a replay")
        final_state = [{
            "ticket_id": item["ticket_id"],
            "final_issue_type": item["issue_type"],
            "final_priority": item["priority"],
            "assigned_queue": item["route"],
            "classification_reason": item.get("classification_reason", ""),
            "subject": item.get("subject", ""),
            "message": item.get("message", ""),
        } for item in p1_tickets]
        prompt = {
            "final_post_override_state": final_state,
            "instruction": (
                "Generate only Markdown for an on-call escalation summary. Include every P1 ticket exactly once. "
                "For each ticket include its ticket ID, why it is urgent grounded only in the supplied state, "
                "assigned queue, and exactly 2 or 3 concise bullet points for the on-call support lead. "
                "Do not change issue types, priorities, queues, or invent facts."
            ),
        }
        model = os.environ.get("SUPPORT_TRIAGE_MODEL", "gpt-5-mini")
        request_body = {
            "model": model,
            "input": [
                {"role": "system", "content": "You write factual Markdown escalation summaries from a final support queue."},
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            "text": {"format": {"type": "json_schema", "name": "escalation_summary",
                                  "strict": True, "schema": {
                                      "type": "object", "additionalProperties": False,
                                      "properties": {"markdown": {"type": "string"}},
                                      "required": ["markdown"],
                                  }}},
        }
        request = urllib.request.Request(
            os.environ.get("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses"),
            data=json.dumps(request_body).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                response_json = json.load(response)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Escalation summary LLM call failed ({exc.code}): {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Escalation summary LLM call failed: {exc.reason}") from exc
        output_text = _response_output_text(response_json)
        if not output_text:
            raise ValueError("Escalation summary response contained no JSON output")
        response_payload = json.loads(output_text)
        markdown = response_payload.get("markdown") if isinstance(response_payload, Mapping) else None
        if not isinstance(markdown, str):
            raise ValueError("Escalation summary response did not contain markdown")
        if llm_log_path is not None:
            log_llm_call(
                llm_log_path,
                stage="escalation_summary",
                ticket_id=[str(item["ticket_id"]) for item in p1_tickets],
                model=model,
                prompt=prompt,
                input_artifacts=["final_queue_assignments.json"],
                output_artifact="escalation_summary.md",
            )

    for item in p1_tickets:
        if str(item["ticket_id"]) not in markdown or str(item["route"]) not in markdown:
            raise ValueError(f"escalation summary omitted P1 ticket or queue: {item['ticket_id']}")
    return markdown.rstrip() + "\n"


def draft_replies_once(items: Sequence[Mapping[str, Any]], *, replay_path: Path | None = None,
                       llm_log_path: Path | None = None) -> list[dict[str, str]]:
    """Generate one Stage 2 reply per ticket; never send multiple tickets in one request."""
    if replay_path is not None:
        payload = read_json(replay_path)
        replies = payload.get("replies") if isinstance(payload, Mapping) else payload
        if not isinstance(replies, list):
            raise ValueError("draft replay must contain a replies array")
    else:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required for Stage 2; use --replay-drafts for a replay")
        replies = []
        for item in items:
            prompt = {
                "ticket_id": item["ticket_id"],
                "subject": item["subject"],
                "message": item["message"],
                "issue_type": item["issue_type"],
                "assigned_priority": item["priority"],
                "instruction": (
                    "Draft a short, safe support reply for this one ticket only. "
                    "Acknowledge the issue, state the intended next step, and ask relevant follow-up "
                    "questions when information is missing. Do not make irreversible promises, guarantee "
                    "an outcome or timeline, claim an action has already happened, or invent facts."
                ),
            }
            model = os.environ.get("SUPPORT_TRIAGE_MODEL", "gpt-5-mini")
            request_body = {
                "model": model,
                "input": [
                    {"role": "system", "content": "You draft safe customer-support replies. Reply for exactly one ticket per request; never batch tickets."},
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
                "text": {"format": {"type": "json_schema", "name": "support_reply",
                                      "strict": True, "schema": _reply_schema()}},
            }
            request = urllib.request.Request(
                os.environ.get("OPENAI_RESPONSES_URL", "https://api.openai.com/v1/responses"),
                data=json.dumps(request_body).encode("utf-8"),
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    response_json = json.load(response)
            except urllib.error.HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"Stage 2 LLM call failed for {item['ticket_id']} ({exc.code}): {detail}") from exc
            except urllib.error.URLError as exc:
                raise RuntimeError(f"Stage 2 LLM call failed for {item['ticket_id']}: {exc.reason}") from exc
            output_text = _response_output_text(response_json)
            if not output_text:
                raise ValueError(f"Stage 2 response contained no JSON output for {item['ticket_id']}")
            replies.append(json.loads(output_text))
            if llm_log_path is not None:
                log_llm_call(
                    llm_log_path,
                    stage="stage2_draft_reply",
                    ticket_id=str(item["ticket_id"]),
                    model=model,
                    prompt=prompt,
                    input_artifacts=["normalized_tickets.json", "ticket_classification.json", "ticket_priorities.json"],
                    output_artifact="draft_replies.json",
                )

    expected_ids = [str(item["ticket_id"]) for item in items]
    actual_ids = [reply.get("ticket_id") for reply in replies if isinstance(reply, Mapping)]
    if actual_ids != expected_ids:
        raise ValueError("Stage 2 must return exactly one reply per ticket in input order")
    for reply in replies:
        if not isinstance(reply, Mapping) or any(not isinstance(reply.get(field), str) for field in _reply_schema()["required"]):
            raise ValueError(f"invalid Stage 2 reply for {reply.get('ticket_id') if isinstance(reply, Mapping) else 'unknown ticket'}")
    return [dict(reply) for reply in replies]


def assign_priority(ticket: Mapping[str, Any], routing_rules: Mapping[str, Any]) -> tuple[str, int, str]:
    """Apply the explicit priority policy in routing_rules.json in fixed order."""
    priority_rules = routing_rules.get("priority_rules")
    allowed = routing_rules.get("allowed_priorities")
    if not isinstance(priority_rules, Mapping) or not isinstance(allowed, list) or not allowed:
        raise ValueError("routing_rules.json must define priority_rules and allowed_priorities")
    if any(not isinstance(priority, str) or not priority for priority in allowed):
        raise ValueError("allowed_priorities must contain non-empty strings")
    base = priority_rules.get("base_priority")
    if base not in allowed:
        raise ValueError("priority_rules.base_priority must be in allowed_priorities")

    # The rules file lists priorities from highest to lowest (P1, P2, P3).
    current_index = allowed.index(base)
    reasons = [f"base priority={base}"]
    text = str(ticket["combined_text"]).lower()
    security_keywords = priority_rules.get("security_keywords", [])
    if not isinstance(security_keywords, list) or any(not isinstance(keyword, str) for keyword in security_keywords):
        raise ValueError("priority_rules.security_keywords must be a list of strings")
    matched_keywords = [keyword for keyword in security_keywords if keyword.lower() in text]
    if matched_keywords:
        current_index = max(0, current_index - 1)
        reasons.append(f"security keyword(s) matched: {', '.join(matched_keywords)}")

    urgent_channels = priority_rules.get("urgent_channels", [])
    if not isinstance(urgent_channels, list) or any(not isinstance(channel, str) for channel in urgent_channels):
        raise ValueError("priority_rules.urgent_channels must be a list of strings")
    channel = str(ticket["channel"]).strip().lower()
    if channel in {item.lower() for item in urgent_channels}:
        current_index = max(0, current_index - 1)
        reasons.append(f"urgent channel matched: {channel}")

    if priority_rules.get("vip_boost") is True and str(ticket["customer_tier"]).strip().lower() == "vip":
        current_index = max(0, current_index - 1)
        reasons.append("VIP boost enabled")

    priority = allowed[current_index]
    reasons.append(f"final priority={priority} (capped to allowed priorities)")
    return priority, current_index, "; ".join(reasons)


def compute_review_flags(items: Sequence[Mapping[str, Any]], routing_rules: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Apply deterministic confidence/review checks after classification and priority assignment."""
    security_terms = {"security", "fraud", "hacked", "phishing", "compromised", "unauthorized", "unauthorised", "password"}
    configured_security = routing_rules.get("priority_rules", {}).get("security_keywords", [])
    security_terms.update(str(term).lower().replace("_", " ") for term in configured_security)
    money_terms = {"money", "payment", "deposit", "withdrawal", "refund", "charge", "card", "funds", "fee", "balance"}
    flags: list[dict[str, Any]] = []
    for item in items:
        reasons: list[str] = []
        if len(str(item.get("classification_reason", "")).strip()) < 20:
            reasons.append("classification_reason is shorter than 20 characters")
        if item.get("issue_type") == "other":
            reasons.append("issue_type is other")
        if item.get("priority") == "P1":
            reasons.append("priority is P1")
        text = str(item.get("combined_text", "")).lower()
        mentions_money = any(term in text for term in money_terms)
        mentions_security = any(term in text for term in security_terms)
        if mentions_money and mentions_security:
            reasons.append("ticket mentions money and security together")
        flags.append({
            "ticket_id": item["ticket_id"],
            "review_required": bool(reasons),
            "review_reasons": reasons,
        })
    return flags


def routing_for(item: Mapping[str, Any], rules: Mapping[str, Any]) -> str:
    matrix = rules.get("routing_matrix", [])
    allowed_queues = set(rules.get("allowed_queues", []))
    if not isinstance(matrix, list) or not allowed_queues:
        raise ValueError("routing_rules.json must define routing_matrix and allowed_queues")
    matches = [route for route in matrix
               if isinstance(route, Mapping) and route.get("issue_type") == item["issue_type"]]
    if len(matches) != 1:
        raise ValueError(f"routing matrix must contain exactly one route for {item['issue_type']}")
    queue = _text(matches[0].get("route_to"))
    if queue not in allowed_queues:
        raise ValueError(f"routing matrix assigned unsupported queue: {queue}")
    return queue


def collect_operator_overrides(items: Sequence[Mapping[str, Any]], rules: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Display the proposed queue and pause for the operator's typed overrides."""
    allowed_issue_types = {value for value in rules.get("allowed_issue_types", []) if value}
    allowed_priorities = set(rules.get("allowed_priorities", []))
    print("\nProposed routing for operator review:")
    for item in items:
        print(f"{item['ticket_id']} | issue_type={item['issue_type']} | priority={item['priority']} | proposed_queue={routing_for(item, rules)}")
    print("\nEnter any overrides as: ticket_id field new_value")
    print("Allowed fields: issue_type, priority")
    print("Press Enter on an empty line to continue.")

    overrides: dict[str, dict[str, str]] = {}
    item_ids = {str(item["ticket_id"]) for item in items}
    while True:
        try:
            line = input()
        except EOFError as exc:
            raise RuntimeError("operator review ended before approval") from exc
        if not line.strip():
            return overrides
        parts = line.split(maxsplit=2)
        if len(parts) != 3:
            print("Invalid override. Use: ticket_id field new_value")
            continue
        ticket_id, field, new_value = parts
        if ticket_id not in item_ids:
            print(f"Unknown ticket_id: {ticket_id}")
            continue
        if field not in {"issue_type", "priority"}:
            print("Allowed fields: issue_type, priority")
            continue
        if field == "issue_type" and new_value not in allowed_issue_types:
            print(f"Invalid issue_type. Allowed values: {', '.join(sorted(allowed_issue_types))}")
            continue
        if field == "priority" and new_value not in allowed_priorities:
            print(f"Invalid priority. Allowed values: {', '.join(sorted(allowed_priorities))}")
            continue
        overrides.setdefault(ticket_id, {})[field] = new_value
        print(f"Override recorded: {ticket_id} {field}={new_value}")


def run(args: argparse.Namespace) -> Path:
    output_dir = Path(args.output_dir)
    pipeline = Pipeline(output_dir)
    inputs = load_inputs(Path(args.tickets), Path(args.routing_rules))
    pipeline.advance(Stage.INPUTS_LOADED)
    normalised = normalise_tickets(inputs.tickets)
    write_json(output_dir / "normalized_tickets.json", normalised)
    pipeline.advance(Stage.TICKETS_NORMALISED)
    llm_log_path = output_dir / "llm_calls.jsonl"
    classifications = classify_once(
        normalised,
        inputs.allowed_issue_types,
        replay_path=Path(args.replay_classification) if args.replay_classification else None,
        llm_log_path=llm_log_path,
    )
    validate_classifications(classifications, normalised, inputs.allowed_issue_types)
    write_json(output_dir / "ticket_classification.json", classifications)
    pipeline.advance(Stage.STAGE1_CLASSIFIED)
    by_id = {item["ticket_id"]: item for item in classifications}
    priorities: list[dict[str, str]] = []
    enriched: list[dict[str, Any]] = []
    for ticket in normalised:
        classification = by_id[ticket["ticket_id"]]
        priority, rank, reason = assign_priority(ticket, inputs.routing_rules)
        priorities.append({"ticket_id": ticket["ticket_id"], "priority": priority, "priority_reason": reason})
        enriched.append({**ticket, **classification, "priority": priority, "priority_rank": rank})
    write_json(output_dir / "ticket_priorities.json", priorities)
    write_json(output_dir / "prioritised_tickets.json", enriched)
    write_json(output_dir / "review_flags.json", compute_review_flags(enriched, inputs.routing_rules))
    pipeline.advance(Stage.PRIORITISED_ASSIGNED)
    stage2_replies = draft_replies_once(
        enriched,
        replay_path=Path(args.replay_drafts) if args.replay_drafts else None,
        llm_log_path=llm_log_path,
    )
    write_json(output_dir / "draft_replies.json", stage2_replies)
    drafted = [{**item, **reply, "draft_reply": reply["reply_body"]}
               for item, reply in zip(enriched, stage2_replies)]
    pipeline.advance(Stage.DRAFT_REPLIES_GENERATED)
    if args.operator_review:
        review = read_json(Path(args.operator_review))
        if not isinstance(review, Mapping) or review.get("approved") is not True:
            raise RuntimeError("operator review must explicitly set approved=true")
        overrides = review.get("overrides", {})
    else:
        overrides = collect_operator_overrides(drafted, inputs.routing_rules)
    if not isinstance(overrides, Mapping):
        raise ValueError("operator review overrides must be an object")
    write_json(output_dir / "operator_overrides.json", overrides)
    reviewed = []
    for item in drafted:
        override = overrides.get(item["ticket_id"], {})
        if not isinstance(override, Mapping):
            raise ValueError(f"invalid override for {item['ticket_id']}")
        if any(field not in {"issue_type", "priority"} for field in override):
            raise ValueError(f"invalid override field for {item['ticket_id']}")
        reviewed.append({**item, **dict(override), "operator_reviewed": True})
    for item in reviewed:
        if item["issue_type"] not in set(inputs.allowed_issue_types) - {""}:
            raise ValueError(f"operator override used an invalid issue_type for {item['ticket_id']}")
        if item["priority"] not in set(inputs.routing_rules.get("allowed_priorities", [])):
            raise ValueError(f"operator override used an invalid priority for {item['ticket_id']}")
        item["priority_rank"] = list(inputs.routing_rules["allowed_priorities"]).index(item["priority"])
    write_json(output_dir / "operator_review.json", reviewed)
    pipeline.advance(Stage.OPERATOR_REVIEW_COMPLETE)
    final_queue = [{**item, "route": routing_for(item, inputs.routing_rules)} for item in reviewed]
    final_queue.sort(key=lambda item: (item["priority_rank"], item["ticket_id"]))
    final_assignments = [{
        "ticket_id": item["ticket_id"],
        "final_issue_type": item["issue_type"],
        "final_priority": item["priority"],
        "assigned_queue": item["route"],
        "reply_subject": item["reply_subject"],
        "reply_body": item["reply_body"],
    } for item in final_queue]
    write_json(output_dir / "final_queue_assignments.json", final_assignments)
    write_json(output_dir / "final_work_queue.json", final_queue)
    pipeline.advance(Stage.FINAL_ROUTING_APPLIED)
    with (output_dir / "final_work_queue.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticket_id", "issue_type", "priority", "route", "draft_reply"])
        writer.writeheader()
        writer.writerows({field: item[field] for field in writer.fieldnames} for item in final_queue)
    escalation_summary = generate_escalation_summary(
        final_queue,
        replay_path=Path(args.replay_escalation) if args.replay_escalation else None,
        llm_log_path=llm_log_path,
    )
    (output_dir / "escalation_summary.md").write_text(escalation_summary, encoding="utf-8", newline="\n")
    pipeline.advance(Stage.OPTIONAL_OUTPUTS_GENERATED)
    write_json(output_dir / "validation.json", {"valid": True, "ticket_count": len(final_queue), "allowed_issue_types": list(inputs.allowed_issue_types)})
    pipeline.advance(Stage.VALIDATION_COMPLETE)
    manifest = {"stage": pipeline.stage.value, "ticket_count": len(final_queue), "input_sha256": {
        "tickets.json": hashlib.sha256(Path(args.tickets).read_bytes()).hexdigest(),
        "routing_rules.json": hashlib.sha256(Path(args.routing_rules).read_bytes()).hexdigest(),
    }}
    write_json(output_dir / "pipeline_manifest.json", manifest)
    pipeline.advance(Stage.RESULTS_FINALISED)
    manifest["stage"] = pipeline.stage.value
    write_json(output_dir / "pipeline_manifest.json", manifest)
    return output_dir / "final_work_queue.json"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickets", default="tickets.json")
    parser.add_argument("--routing-rules", default="routing_rules.json")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--replay-classification", help="JSON fixture containing the single Stage 1 response")
    parser.add_argument("--replay-drafts", help="JSON fixture containing one Stage 2 reply per ticket")
    parser.add_argument("--replay-escalation", help="JSON fixture containing {\"markdown\": \"...\"} for escalation output")
    parser.add_argument("--operator-review", help="JSON file with {\"approved\": true, \"overrides\": {...}}")
    return parser


if __name__ == "__main__":
    try:
        result = run(build_parser().parse_args())
    except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
        print(f"support-triage failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print(result)
