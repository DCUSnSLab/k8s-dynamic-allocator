import json
import logging
import time
from datetime import datetime

from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from config.settings import DEFAULT_BACKEND_TYPE, set_request_label

logger = logging.getLogger(__name__)


def _get_orchestrator():
    from api.apps import orchestrator_instance

    return orchestrator_instance


def json_response(data, status=200):
    json_str = json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n"
    return HttpResponse(
        json_str,
        content_type="application/json; charset=utf-8",
        status=status,
    )


def _extract_frontend_ip(request, data):
    frontend_ip = (data.get("frontend_ip") or "").strip()
    if frontend_ip:
        return frontend_ip

    forwarded = request.META.get("HTTP_X_FORWARDED_FOR", "")
    if forwarded:
        first_hop = forwarded.split(",")[0].strip()
        if first_hop:
            return first_hop

    return (request.META.get("REMOTE_ADDR") or "").strip()


def _clean_optional_string(value):
    if not isinstance(value, str):
        return ""
    return value.strip()


@csrf_exempt
def execute_command(request):
    if request.method != "POST":
        return json_response({"status": "error", "message": "Only POST method is allowed"}, status=405)

    try:
        data = json.loads(request.body)
        username = data.get("username", "")
        command = data.get("command", "")
        frontend_pod = data.get("frontend_pod", "")
        frontend_ip = _extract_frontend_ip(request, data)
        backend_type = data.get("backend_type", DEFAULT_BACKEND_TYPE)

        if not username:
            return json_response({"status": "error", "message": "username is required"}, status=400)
        if not command:
            return json_response({"status": "error", "message": "command is required"}, status=400)
        if not frontend_ip:
            return json_response({"status": "error", "message": "frontend_ip is required"}, status=400)

        ingress_ts_ms = int(time.time() * 1000)
        set_request_label(username)
        logger.info(
            "Execute request: frontend=%s, frontend_ip=%s, backend_type=%s, user=%s, ingress_ts_ms=%s, command=%s",
            frontend_pod or frontend_ip,
            frontend_ip,
            backend_type,
            username,
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
        logger.error("JSON parse error: %s", str(exc))
        return json_response({"status": "error", "message": "Invalid JSON format", "detail": str(exc)}, status=400)
    except Exception as exc:
        logger.error("Unexpected error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": "Internal server error", "detail": str(exc)}, status=500)


@csrf_exempt
def health_check(request):
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
def queue_status(request):
    if request.method != "GET":
        return json_response({"status": "error", "message": "Only GET method is allowed"}, status=405)

    try:
        backend_type = (request.GET.get("backend_type") or "").strip()
        if not backend_type:
            return json_response(
                {"status": "error", "message": "backend_type is required"},
                status=400,
            )
        result = _get_orchestrator().get_queue_status(
            backend_type=backend_type,
        )
        return json_response(result)
    except ValueError as exc:
        return json_response({"status": "error", "message": str(exc)}, status=400)
    except Exception as exc:
        logger.error("Queue status error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def pool_status(request):
    if request.method != "GET":
        return json_response({"status": "error", "message": "Only GET method is allowed"}, status=405)

    try:
        result = _get_orchestrator().get_pool_status()
        return json_response(result)
    except Exception as exc:
        logger.error("Pool status error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def ticket_detail(request, ticket_id):
    if request.method != "GET":
        return json_response({"status": "error", "message": "Only GET method is allowed"}, status=405)

    try:
        result = _get_orchestrator().get_ticket(ticket_id)
        if result.get("status") == "error":
            return json_response(result, status=404)
        return json_response(result)
    except Exception as exc:
        logger.error("Ticket detail error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def cancel_ticket(request, ticket_id):
    if request.method != "POST":
        return json_response({"status": "error", "message": "Only POST method is allowed"}, status=405)

    try:
        reason = ""
        if request.body:
            try:
                data = json.loads(request.body)
                reason = data.get("reason", "")
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
        logger.error("Ticket cancel error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def initialize_pool(request):
    if request.method != "POST":
        return json_response({"status": "error", "message": "Only POST method is allowed"}, status=405)

    try:
        result = _get_orchestrator().initialize_pool()
        return json_response({"status": "success", "message": "Pool initialized", "result": result})
    except Exception as exc:
        logger.error("Pool init error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def release_backend(request):
    if request.method != "POST":
        return json_response({"status": "error", "message": "Only POST method is allowed"}, status=405)

    try:
        data = json.loads(request.body)
        backend_pod = _clean_optional_string(data.get("backend_pod"))
        orchestrator = _get_orchestrator()

        if not backend_pod:
            return json_response({"status": "error", "message": "backend_pod is required"}, status=400)

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
        return json_response({"status": "error", "message": "Invalid JSON format"}, status=400)
    except Exception as exc:
        logger.error("Release error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)


@csrf_exempt
def check_stale(request):
    if request.method != "POST":
        return json_response({"status": "error", "message": "Only POST method is allowed"}, status=405)

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
        logger.error("Stale check error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)
