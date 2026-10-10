"""Validate a completed support-triage run.

Run with: python validate.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


def fail(message: str) -> None:
    raise ValueError(message)


def load_json(path: Path) -> Any:
    if not path.is_file():
        fail(f"missing required artifact: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        fail(f"invalid JSON in {path}: {exc}")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require_fields(item: Mapping[str, Any], fields: set[str], context: str) -> None:
    missing = fields - set(item)
    if missing:
        fail(f"{context} missing fields: {sorted(missing)}")


def validate(output_dir: Path, tickets_path: Path, rules_path: Path) -> None:
    tickets = load_json(tickets_path)
    rules = load_json(rules_path)
    if not isinstance(tickets, list) or not isinstance(rules, Mapping):
        fail("tickets.json must be an array and routing_rules.json must be an object")

    required_files = [
        "normalized_tickets.json", "ticket_classification.json", "ticket_priorities.json",
        "prioritised_tickets.json", "review_flags.json", "draft_replies.json",
        "operator_overrides.json", "final_queue_assignments.json", "final_work_queue.json",
        "escalation_summary.md", "pipeline_manifest.json", "llm_calls.jsonl",
    ]
    for name in required_files:
        if not (output_dir / name).is_file():
            fail(f"missing required artifact: {output_dir / name}")

    normalized = load_json(output_dir / "normalized_tickets.json")
    classifications = load_json(output_dir / "ticket_classification.json")
    priorities = load_json(output_dir / "ticket_priorities.json")
    prioritised = load_json(output_dir / "prioritised_tickets.json")
    review_flags = load_json(output_dir / "review_flags.json")
    drafts = load_json(output_dir / "draft_replies.json")
    overrides = load_json(output_dir / "operator_overrides.json")
    final_assignments = load_json(output_dir / "final_queue_assignments.json")
    final_work_queue = load_json(output_dir / "final_work_queue.json")
    manifest = load_json(output_dir / "pipeline_manifest.json")

    if not all(isinstance(value, list) for value in (normalized, classifications, priorities, prioritised, review_flags, drafts, final_assignments, final_work_queue)):
        fail("all ticket artifacts must contain arrays")
    if not isinstance(overrides, Mapping) or not isinstance(manifest, Mapping):
        fail("operator_overrides.json and pipeline_manifest.json have invalid shapes")
    raw_ids = [item.get("ticket_id") for item in tickets if isinstance(item, Mapping)]
    ids = [item.get("ticket_id") for item in normalized]
    if len(raw_ids) != len(tickets) or ids != raw_ids or len(set(ids)) != len(ids):
        fail("tickets were not normalized deterministically or ticket IDs changed")
    normalized_fields = {"ticket_id", "customer_tier", "channel", "subject", "message", "combined_text", "message_word_count"}
    for item in normalized:
        require_fields(item, normalized_fields, f"normalized ticket {item.get('ticket_id')}")
        if not isinstance(item["message_word_count"], int):
            fail(f"invalid message_word_count for {item['ticket_id']}")

    allowed_issue_types = set(rules.get("allowed_issue_types", [])) - {""}
    allowed_priorities = set(rules.get("allowed_priorities", []))
    allowed_queues = set(rules.get("allowed_queues", []))
    classification_by_id = {item.get("ticket_id"): item for item in classifications}
    if set(classification_by_id) != set(ids) or len(classification_by_id) != len(ids):
        fail("every normalized ticket must have exactly one classification")
    classification_fields = {"ticket_id", "issue_type", "classification_reason", "customer_intent", "needs_human_attention"}
    for item in classifications:
        require_fields(item, classification_fields, f"classification {item.get('ticket_id')}")
        if item["issue_type"] not in allowed_issue_types:
            fail(f"classification uses disallowed issue type: {item['issue_type']}")

    priorities_by_id = {item.get("ticket_id"): item for item in priorities}
    if set(priorities_by_id) != set(ids):
        fail("every ticket must have one priority record")
    for item in priorities:
        require_fields(item, {"ticket_id", "priority", "priority_reason"}, f"priority {item.get('ticket_id')}")
        if item["priority"] not in allowed_priorities:
            fail(f"priority uses disallowed value: {item['priority']}")

    for collection_name, collection in (("review flag", review_flags), ("draft reply", drafts)):
        collection_ids = [item.get("ticket_id") for item in collection]
        if set(collection_ids) != set(ids) or len(collection_ids) != len(ids):
            fail(f"every ticket must have exactly one {collection_name}")
    for item in review_flags:
        require_fields(item, {"ticket_id", "review_required", "review_reasons"}, f"review flag {item.get('ticket_id')}")
        if not isinstance(item["review_required"], bool) or not isinstance(item["review_reasons"], list):
            fail(f"invalid review flag shape for {item['ticket_id']}")
    for item in drafts:
        require_fields(item, {"ticket_id", "reply_subject", "reply_body", "request_for_missing_info"}, f"draft {item.get('ticket_id')}")

    matrix = rules.get("routing_matrix", [])
    route_map: dict[str, str] = {}
    for route in matrix:
        if not isinstance(route, Mapping) or route.get("issue_type") in route_map:
            fail("routing_matrix contains an invalid or duplicate issue type")
        route_map[str(route["issue_type"])] = str(route.get("route_to"))
    if set(route_map) != allowed_issue_types or not set(route_map.values()).issubset(allowed_queues):
        fail("routing_matrix does not cover allowed issue types and queues")

    final_ids = [item.get("ticket_id") for item in final_assignments]
    if set(final_ids) != set(ids) or len(final_ids) != len(ids):
        fail("final queue must contain every ticket exactly once")
    final_by_id = {item["ticket_id"]: item for item in final_assignments}
    final_fields = {"ticket_id", "final_issue_type", "final_priority", "assigned_queue", "reply_subject", "reply_body"}
    for item in final_assignments:
        require_fields(item, final_fields, f"final assignment {item.get('ticket_id')}")
        if item["final_issue_type"] not in allowed_issue_types or item["final_priority"] not in allowed_priorities:
            fail(f"final assignment uses a disallowed value for {item['ticket_id']}")
        if item["assigned_queue"] != route_map[item["final_issue_type"]]:
            fail(f"final routing does not match routing_matrix for {item['ticket_id']}")
    for ticket_id, override in overrides.items():
        if ticket_id not in final_by_id or not isinstance(override, Mapping):
            fail(f"invalid operator override for {ticket_id}")
        if "issue_type" in override and final_by_id[ticket_id]["final_issue_type"] != override["issue_type"]:
            fail(f"issue_type override was not applied for {ticket_id}")
        if "priority" in override and final_by_id[ticket_id]["final_priority"] != override["priority"]:
            fail(f"priority override was not applied for {ticket_id}")

    manifest_hashes = manifest.get("input_sha256", {})
    if manifest_hashes.get("tickets.json") != sha256(tickets_path) or manifest_hashes.get("routing_rules.json") != sha256(rules_path):
        fail("pipeline inputs were not read from the current tickets.json and routing_rules.json")

    log_path = output_dir / "llm_calls.jsonl"
    records = []
    for line_number, line in enumerate(log_path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            fail(f"invalid JSONL at {log_path}:{line_number}: {exc}")
        require_fields(record, {"stage", "ticket_id", "timestamp", "provider", "model", "prompt_hash", "input_artifacts", "output_artifact"}, f"LLM log line {line_number}")
        try:
            datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
        except (TypeError, ValueError):
            fail(f"invalid ISO-8601 timestamp at LLM log line {line_number}")
        if not re.fullmatch(r"[0-9a-f]{64}", record["prompt_hash"]):
            fail(f"invalid prompt hash at LLM log line {line_number}")
        records.append(record)
    stages = [record["stage"] for record in records]
    if stages.count("stage1_classification") != 1:
        fail("llm_calls.jsonl must contain exactly one Stage 1 record")
    if stages.count("stage2_draft_reply") != len(ids):
        fail("llm_calls.jsonl must contain one Stage 2 record per ticket")
    p1_ids = [item["ticket_id"] for item in final_assignments if item["final_priority"] == "P1"]
    if stages.count("escalation_summary") != (1 if p1_ids else 0):
        fail("llm_calls.jsonl escalation record count does not match final P1 tickets")
    if stages.index("stage1_classification") != 0:
        fail("normalization must happen before the first LLM call")
    if not set(["normalized_tickets.json", "routing_rules.json"]).issubset(records[0]["input_artifacts"]):
        fail("Stage 1 log does not identify normalized tickets and routing rules as inputs")

    print(f"VALIDATION PASSED: {len(ids)} tickets, {len(records)} logged LLM calls")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--tickets", default="tickets.json")
    parser.add_argument("--routing-rules", default="routing_rules.json")
    args = parser.parse_args()
    try:
        validate(Path(args.output_dir), Path(args.tickets), Path(args.routing_rules))
    except (OSError, ValueError) as exc:
        print(f"VALIDATION FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
