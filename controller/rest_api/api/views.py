import ipaddress
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict

from django.http import HttpRequest, HttpResponse
from django.views.decorators.csrf import csrf_exempt

from config.settings import DEFAULT_BACKEND_TYPE, build_request_label, set_request_label

logger = logging.getLogger(__name__)


def _get_orchestrator():
    from api.apps import orchestrator_instance

    return orchestrator_instance


def json_response(data: Dict[str, Any], status: int = 200) -> HttpResponse:
    json_str = json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n"
    return HttpResponse(
        json_str,
        content_type="application/json; charset=utf-8",
        status=status,
    )


def _error_response(message: str, status: int = 400) -> HttpResponse:
    return json_response({"status": "error", "message": message}, status=status)


def _method_not_allowed(expected: str) -> HttpResponse:
    return _error_response(f"Only {expected} method is allowed", status=405)


def _is_valid_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _extract_frontend_ip(request: HttpRequest, data: Dict[str, Any]) -> str:
    frontend_ip = _clean_optional_string(data.get("frontend_ip"))
    if frontend_ip and _is_valid_ip(frontend_ip):
        return frontend_ip

    forwarded = _clean_optional_string(request.META.get("HTTP_X_FORWARDED_FOR", ""))
    if forwarded:
        first_hop = forwarded.split(",")[0].strip()
        if first_hop and _is_valid_ip(first_hop):
            return first_hop

    remote_addr = _clean_optional_string(request.META.get("REMOTE_ADDR"))
    if remote_addr and _is_valid_ip(remote_addr):
        return remote_addr

    return ""


def _clean_optional_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


@csrf_exempt
def execute_command(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return _method_not_allowed("POST")

    try:
        data = json.loads(request.body)
        username = _clean_optional_string(data.get("username"))
        command = _clean_optional_string(data.get("command"))
        frontend_pod = _clean_optional_string(data.get("frontend_pod"))
        frontend_ip = _extract_frontend_ip(request, data)
        backend_type = _clean_optional_string(data.get("backend_type")) or DEFAULT_BACKEND_TYPE

        if not username:
            return _error_response("username is required")
        if not command:
            return _error_response("command is required")
        if not frontend_ip:
            return _error_response("frontend_ip is required")

        ticket_id = uuid.uuid4().hex
        ingress_ts_ms = int(time.time() * 1000)
        set_request_label(build_request_label(username, ticket_id[:10]))
        logger.info(
            "Execute request: ticket_id=%s, frontend=%s, frontend_ip=%s, backend_type=%s, ingress_ts_ms=%s, command=%s",
            ticket_id,
            frontend_pod or frontend_ip,
            frontend_ip,
            backend_type,
            ingress_ts_ms,
            command,
        )

        result = _get_orchestrator().execute_command(
            username=username,
            command=command,
            frontend_ip=frontend_ip,
            frontend_pod=frontend_pod,
            backend_type=backend_type,
            ingress_ts_ms=ingress_ts_ms,
            ticket_id=ticket_id,
        )

        ticket = result.get("ticket") or {}
        post_ticket_label = ticket.get("request_label") or ""
        if post_ticket_label:
            set_request_label(post_ticket_label)

        if result.get("error_type") == "invalid_backend_type":
            return json_response(result, status=400)

        status_code = {
            "assigned": 200,
            "queued": 202,
            "allocating": 202,
            "cancelled": 409,
            "failed": 500,
        }.get(result["status"], 500)
        return json_response(result, status=status_code)

    except json.JSONDecodeError as exc:
        logger.error("JSON parse error: %s", exc)
        return _error_response(f"Invalid JSON format: {exc}")
    except Exception as exc:
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return _error_response("Internal server error", status=500)


@csrf_exempt
def health_check(request: HttpRequest) -> HttpResponse:
    try:
        result = _get_orchestrator().health_check()
    except Exception as exc:
        result = "[error] " + str(exc)

    return json_response(
        {
            "status": "healthy",
            "service": "Controller Pod REST API",
            "timestamp": datetime.now().isoformat(),
            "result": result,
        }
    )


@csrf_exempt
def queue_status(request: HttpRequest) -> HttpResponse:
    if request.method != "GET":
        return _method_not_allowed("GET")

    try:
        backend_type = (request.GET.get("backend_type") or "").strip()
        if not backend_type:
            return _error_response("backend_type is required")
        result = _get_orchestrator().get_queue_status(
            backend_type=backend_type,
        )
        return json_response(result)
    except ValueError as exc:
        return _error_response(str(exc))
    except Exception as exc:
        logger.error("Queue status error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def pool_status(request: HttpRequest) -> HttpResponse:
    if request.method != "GET":
        return _method_not_allowed("GET")

    try:
        result = _get_orchestrator().get_pool_status()
        return json_response(result)
    except Exception as exc:
        logger.error("Pool status error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def ticket_detail(request: HttpRequest, ticket_id: str) -> HttpResponse:
    if request.method != "GET":
        return _method_not_allowed("GET")

    try:
        result = _get_orchestrator().get_ticket(ticket_id)
        if result.get("status") == "error":
            return json_response(result, status=404)
        return json_response(result)
    except Exception as exc:
        logger.error("Ticket detail error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def cancel_ticket(request: HttpRequest, ticket_id: str) -> HttpResponse:
    if request.method != "POST":
        return _method_not_allowed("POST")

    try:
        reason = ""
        if request.body:
            try:
                data = json.loads(request.body)
                reason = _clean_optional_string(data.get("reason"))
            except json.JSONDecodeError:
                reason = ""

        result = _get_orchestrator().cancel_ticket(ticket_id, reason=reason)
        if result.get("status") == "error":
            message = (result.get("message") or "").lower()
            if "already assigned" in message:
                return json_response(result, status=409)
            if "not found" in message:
                return json_response(result, status=404)
            if "busy" in message:
                return json_response(result, status=503)
            return json_response(result, status=500)
        return json_response(result)
    except Exception as exc:
        logger.error("Ticket cancel error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def initialize_pool(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return _method_not_allowed("POST")

    try:
        result = _get_orchestrator().initialize_pool()
        return json_response({"status": "success", "message": "Pool initialized", "result": result})
    except Exception as exc:
        logger.error("Pool init error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def release_backend(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return _method_not_allowed("POST")

    try:
        data = json.loads(request.body)
        backend_pod = _clean_optional_string(data.get("backend_pod"))
        orchestrator = _get_orchestrator()

        if not backend_pod:
            return _error_response("backend_pod is required")

        try:
            request_context = orchestrator.get_assigned_request_context(backend_pod)
        except Exception as exc:
            logger.warning("Release request context unavailable for %s: %s", backend_pod, exc)
            request_context = None

        caller_hints = {}
        for hint_key in ("ticket_id", "backend_type", "frontend_pod", "request_label", "reason", "source"):
            hint_value = _clean_optional_string(data.get(hint_key))
            if hint_value:
                caller_hints[hint_key] = hint_value
        if caller_hints:
            if request_context is None:
                request_context = caller_hints
            else:
                for key, value in caller_hints.items():
                    if not request_context.get(key):
                        request_context[key] = value

        if request_context is not None and not request_context.get("request_label"):
            synthesized_label = (
                _clean_optional_string(request_context.get("frontend_pod"))
                or _clean_optional_string(request_context.get("ticket_id"))
                or backend_pod
            )
            if synthesized_label:
                request_context["request_label"] = synthesized_label

        request_label = _clean_optional_string((request_context or {}).get("request_label"))
        if request_label:
            set_request_label(request_label)

        result = orchestrator.release_backend(backend_pod, request_context=request_context)
        status_code = 200 if result["status"] == "success" else 500
        return json_response(result, status=status_code)

    except json.JSONDecodeError:
        return _error_response("Invalid JSON format")
    except Exception as exc:
        logger.error("Release error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)


@csrf_exempt
def check_stale(request: HttpRequest) -> HttpResponse:
    if request.method != "POST":
        return _method_not_allowed("POST")

    try:
        result = _get_orchestrator().check_stale_allocations()
        released_count = len(result.get("released", []))
        if released_count > 0:
            logger.info(
                "Stale check: %s checked, %s released",
                result.get("checked", 0),
                released_count,
            )
        else:
            logger.debug(
                "Stale check: %s checked, %s released",
                result.get("checked", 0),
                released_count,
            )
        return json_response({"status": "success", **result})
    except Exception as exc:
        logger.error("Stale check error: %s", exc, exc_info=True)
        return _error_response(str(exc), status=500)
