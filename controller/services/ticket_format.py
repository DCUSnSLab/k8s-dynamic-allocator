import logging
from datetime import datetime
from typing import Dict, Iterable, Optional

from config import settings
from config.settings import build_request_label, set_request_label

from .queue import parse_datetime, safe_int

logger = logging.getLogger(__name__)


def _format_log_value(value: object) -> str:
    text = str(value)
    if text == "":
        return '""'
    if not any(char.isspace() for char in text) and '"' not in text and "'" not in text:
        return text
    if "'" not in text:
        return f"'{text}'"
    if '"' not in text:
        return f'"{text}"'
    return f'"{text.replace(chr(34), chr(39))}"'


def ticket_short(ticket_id: Optional[str]) -> str:
    return str(ticket_id or "")[:10]


def ticket_request_label(ticket: Optional[Dict]) -> str:
    if not ticket:
        return "-"

    label = (ticket.get("request_label") or "").strip()
    if label:
        return label

    username = (ticket.get("username") or "unknown").strip() or "unknown"
    short_value = (ticket.get("ticket_short") or ticket_short(ticket.get("ticket_id"))).strip()
    return build_request_label(username, short_value or None)


def set_ticket_context(ticket: Optional[Dict]) -> str:
    label = ticket_request_label(ticket)
    set_request_label(label)
    return label


DEFAULT_TICKET_EVENT_FIELDS = (
    "ticket_id",
    "backend_type",
    "frontend_pod",
    "frontend_ip",
    "username",
    "claimed_by",
    "backend_pod",
    "backend_ip",
    "queue_position",
    "retry_count",
    "ticket_short",
    "ingress_ts_ms",
)


def ticket_event_fields(
    ticket: Optional[Dict],
    *,
    include_ticket_fields: Optional[Iterable[str]] = DEFAULT_TICKET_EVENT_FIELDS,
    **fields,
) -> Dict[str, object]:
    payload = {}
    if ticket and include_ticket_fields is not None:
        for key in include_ticket_fields:
            value = ticket.get(key)
            if value not in (None, ""):
                payload[key] = value
    for key, value in fields.items():
        if value not in (None, ""):
            payload[key] = value
    return payload


def log_queue_event(
    level: str,
    event: str,
    ticket: Optional[Dict] = None,
    *,
    component: str = "QUEUE",
    include_ticket_fields: Optional[Iterable[str]] = DEFAULT_TICKET_EVENT_FIELDS,
    **fields,
) -> None:
    if ticket:
        set_ticket_context(ticket)

    parts = [f"[{event}]"]
    payload = ticket_event_fields(
        ticket,
        include_ticket_fields=include_ticket_fields,
        **fields,
    )
    if payload:
        ordered_keys = [
            "ticket_id",
            "operation",
            "status",
            "source",
            "error_type",
            "backend_type",
            "username",
            "frontend_pod",
            "frontend_ip",
            "queue_position",
            "claimed_by",
            "backend_pod",
            "backend_ip",
            "retry_count",
            "ticket_short",
            "ingress_ts_ms",
            "reason",
            "available_ready_backends",
            "leader",
            "attempt",
            "max_attempts",
            "queue_wait_ms",
            "backend_ready_to_claim_ms",
            "allocation_ms",
            "session_ms",
            "release_ms",
        ]
        used = set()
        for key in ordered_keys:
            if key in payload:
                parts.append(f"{key}={_format_log_value(payload[key])}")
                used.add(key)
        for key in sorted(payload):
            if key in used:
                continue
            parts.append(f"{key}={_format_log_value(payload[key])}")

    message = " ".join(parts)
    getattr(logger, level, logger.info)(message)


def datetime_to_epoch_ms(value: object) -> int:
    """Convert datetime or ISO string to epoch milliseconds."""
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if isinstance(value, str):
        dt = parse_datetime(value)
        if dt:
            return int(dt.timestamp() * 1000)
    return 0


def ingress_epoch_ms(ticket: Optional[Dict]) -> int:
    """
    Get request ingress timestamp in epoch milliseconds.

    Prefers explicit ingress_ts_ms field, falls back to created_at timestamp.
    Returns 0 if neither is available.
    """
    if not ticket:
        return 0

    ingress_ts_ms = safe_int(ticket.get("ingress_ts_ms"), 0)
    if ingress_ts_ms:
        return ingress_ts_ms
    return datetime_to_epoch_ms(ticket.get("created_at"))


def assignment_timing_fields(ticket: Optional[Dict]) -> Dict[str, int]:
    """
    Calculate timing metrics for ticket assignment flow.

    Returns dict with timing fields (only includes fields where all required timestamps are available):
    - queue_wait_ms: Time from ingress to claim
    - backend_ready_to_claim_ms: Time from selected backend Ready to claim
    - allocation_ms: Time from claim to assignment

    Missing timestamps result in omitted fields, not zero values, for clearer logs.
    """
    if not ticket:
        return {}

    ingress_ts_ms = ingress_epoch_ms(ticket)
    claimed_at_ms = datetime_to_epoch_ms(ticket.get("claimed_at"))
    assigned_at_ms = datetime_to_epoch_ms(ticket.get("assigned_at"))
    fields: Dict[str, int] = {}

    # Only calculate metrics where all required timestamps are available
    if ingress_ts_ms and claimed_at_ms:
        fields["queue_wait_ms"] = max(0, claimed_at_ms - ingress_ts_ms)
    backend_ready_at_ms = datetime_to_epoch_ms(ticket.get("backend_ready_at"))
    if backend_ready_at_ms and claimed_at_ms:
        ready_or_ingress_ms = max(backend_ready_at_ms, ingress_ts_ms or 0)
        fields["backend_ready_to_claim_ms"] = max(
            0,
            claimed_at_ms - ready_or_ingress_ms,
        )
    if claimed_at_ms and assigned_at_ms:
        fields["allocation_ms"] = max(0, assigned_at_ms - claimed_at_ms)
    return fields


def elapsed_since_ingress_ms(ticket: Optional[Dict], field_name: str) -> Optional[int]:
    if not ticket:
        return None

    ingress_ts_ms = ingress_epoch_ms(ticket)
    end_ms = datetime_to_epoch_ms(ticket.get(field_name))
    if not ingress_ts_ms or not end_ms:
        return None
    return max(0, end_ms - ingress_ts_ms)


def assigned_request_context(ticket: Optional[Dict]) -> Dict[str, object]:
    if not ticket:
        return {}

    return {
        "request_label": ticket.get("request_label") or ticket_request_label(ticket),
        "ticket_id": ticket.get("ticket_id") or "",
        "ticket_short": ticket.get("ticket_short") or ticket_short(ticket.get("ticket_id")),
        "username": ticket.get("username") or "",
        "frontend_pod": ticket.get("frontend_pod") or "",
        "frontend_ip": ticket.get("frontend_ip") or "",
        "backend_type": ticket.get("backend_type") or "",
        "assigned_at_ms": datetime_to_epoch_ms(ticket.get("assigned_at")),
    }


def ticket_response(ticket: Optional[Dict], message: Optional[str] = None) -> Dict:
    if not ticket:
        return {
            "status": "error",
            "message": message or "Ticket not found",
        }

    status = str(ticket.get("status") or "queued").lower()
    ticket_id = ticket.get("ticket_id")
    response = {
        "status": status,
        "ticket_status": status,
        "message": message or "",
        "ticket_id": ticket_id,
        "backend_type": ticket.get("backend_type"),
        "ticket": ticket,
        "poll_url": f"/api/ticket/{ticket_id}/" if ticket_id else "",
        "cancel_url": f"/api/ticket/{ticket_id}/cancel/" if ticket_id else "",
        "release_url": "/api/pool/release/",
    }

    if ticket.get("backend_pod"):
        response["backend_pod"] = ticket.get("backend_pod")
    if ticket.get("backend_ip"):
        response["backend_ip"] = ticket.get("backend_ip")
    if status in {"queued", "allocating"}:
        response["retry_after_ms"] = int(max(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS, 0.2) * 1000)
    return response
