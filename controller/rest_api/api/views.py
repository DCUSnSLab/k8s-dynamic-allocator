import json
import logging
from datetime import datetime

from django.core.cache import cache
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

from config.settings import DEFAULT_BACKEND_TYPE, get_request_id, next_conn_id, set_request_id

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

        set_request_id(next_conn_id())
        logger.info(
            "Execute request: frontend=%s, frontend_ip=%s, backend_type=%s, user=%s, command=%s",
            frontend_pod or frontend_ip,
            frontend_ip,
            backend_type,
            username,
            command,
        )

        result = _get_orchestrator().execute_command(
            username=username,
            command=command,
            frontend_ip=frontend_ip,
            frontend_pod=frontend_pod,
            backend_type=backend_type,
        )

        if result["status"] == "assigned":
            backend_pod = result.get("backend_pod")
            if backend_pod:
                cache.set(f"conn_map_{backend_pod}", get_request_id(), timeout=86400)

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
        backend_pod = data.get("backend_pod", "")

        if not backend_pod:
            return json_response({"status": "error", "message": "backend_pod is required"}, status=400)

        conn_id = cache.get(f"conn_map_{backend_pod}")
        if conn_id:
            set_request_id(conn_id)
            cache.delete(f"conn_map_{backend_pod}")

        result = _get_orchestrator().release_backend(backend_pod)
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
        logger.info(
            "Stale check: %s checked, %s released",
            result.get("checked", 0),
            len(result.get("released", [])),
        )
        return json_response({"status": "success", **result})
    except Exception as exc:
        logger.error("Stale check error: %s", str(exc), exc_info=True)
        return json_response({"status": "error", "message": str(exc)}, status=500)
