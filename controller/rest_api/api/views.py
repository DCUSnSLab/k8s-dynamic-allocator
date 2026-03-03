from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from datetime import datetime

from config.settings import _conn_map, get_request_id, set_request_id, next_conn_id

logger = logging.getLogger(__name__)


def _get_orchestrator():
    from api.apps import orchestrator_instance
    return orchestrator_instance


def json_response(data, status=200):
    """
    들여쓰기된 JSON 응답 생성 (끝에 줄바꿈 포함)
    """
    json_str = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    return HttpResponse(
        json_str,
        content_type='application/json; charset=utf-8',
        status=status
    )


@csrf_exempt
def execute_command(request):
    """
    Frontend Pod로부터 Backend Pod 실행 요청을 받는 API
    """
    if request.method != 'POST':
        return json_response({
            'status': 'error',
            'message': 'Only POST method is allowed'
        }, status=405)
    
    try:
        data = json.loads(request.body)
        username = data.get('username', '')
        command = data.get('command', '')
        frontend_pod = data.get('frontend_pod', '')
        
        # 입력 검증
        if not username:
            return json_response({
                'status': 'error',
                'message': 'username is required'
            }, status=400)
        
        if not command:
            return json_response({
                'status': 'error',
                'message': 'command is required'
            }, status=400)
        
        set_request_id(next_conn_id())
        logger.info(f"Execute request: frontend={frontend_pod}, user={username}, command={command}")

        # Backend Pod 할당 및 실행 요청
        result = _get_orchestrator().execute_command(username, command, frontend_pod)

        # conn mapping
        if result['status'] == 'success':
            _conn_map[result.get('backend_pod')] = get_request_id()
        
        # HTTP 상태 코드 결정
        status_code = 200 if result['status'] == 'success' else 503
        
        return json_response(result, status=status_code)
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {str(e)}")
        return json_response({
            'status': 'error',
            'message': 'Invalid JSON format',
            'detail': str(e)
        }, status=400)
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return json_response({
            'status': 'error',
            'message': 'Internal server error',
            'detail': str(e)
        }, status=500)


@csrf_exempt
def health_check(request):
    """
    컨트롤러 Pod 헬스 체크 엔드포인트
    """
    try:
        result = _get_orchestrator().health_check()
    except Exception as e:
        result = "[error] " + str(e)

    return json_response({
        'status': 'healthy',
        'service': 'Controller Pod REST API',
        'timestamp': datetime.now().isoformat(),
        'result': result
    })


@csrf_exempt
def pool_status(request):
    """
    Backend Pool 상태 조회 엔드포인트
    """
    if request.method != 'GET':
        return json_response({
            'status': 'error',
            'message': 'Only GET method is allowed'
        }, status=405)
    
    try:
        result = _get_orchestrator().get_pool_status()
        return json_response(result)
    except Exception as e:
        logger.error(f"Pool status error: {str(e)}", exc_info=True)
        return json_response({
            'status': 'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
def initialize_pool(request):
    """
    Backend Pool 수동 초기화 엔드포인트
    """
    if request.method != 'POST':
        return json_response({
            'status': 'error',
            'message': 'Only POST method is allowed'
        }, status=405)
    
    try:
        result = _get_orchestrator().initialize_pool()
        return json_response({
            'status': 'success',
            'message': 'Pool initialized',
            'result': result
        })
    except Exception as e:
        logger.error(f"Pool init error: {str(e)}", exc_info=True)
        return json_response({
            'status': 'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
def release_backend(request):
    """
    Backend Pod 할당 해제 엔드포인트
    """
    if request.method != 'POST':
        return json_response({
            'status': 'error',
            'message': 'Only POST method is allowed'
        }, status=405)
    
    try:
        data = json.loads(request.body)
        backend_pod = data.get('backend_pod', '')
        
        if not backend_pod:
            return json_response({
                'status': 'error',
                'message': 'backend_pod is required'
            }, status=400)

        # execute 때 저장한 conn 복원
        conn_id = _conn_map.pop(backend_pod, None)
        if conn_id:
            set_request_id(conn_id)

        result = _get_orchestrator().release_backend(backend_pod)
        
        status_code = 200 if result['status'] == 'success' else 500
        return json_response(result, status=status_code)
        
    except json.JSONDecodeError as e:
        return json_response({
            'status': 'error',
            'message': 'Invalid JSON format'
        }, status=400)
    except Exception as e:
        logger.error(f"Release error: {str(e)}", exc_info=True)
        return json_response({
            'status': 'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
def check_stale(request):
    """
    비정상 할당 감지 및 자동 해제

    K8s API로 할당된 Backend의 Frontend Pod 상태를 확인하여
    존재하지 않거나 Running이 아닌 Frontend에 할당된 Backend를 해제
    """
    if request.method != 'POST':
        return json_response({
            'status': 'error',
            'message': 'Only POST method is allowed'
        }, status=405)

    try:
        result = _get_orchestrator().check_stale_allocations()
        logger.info(
            f"Stale check: {result['checked']} checked, "
            f"{len(result['released'])} released"
        )
        return json_response({
            'status': 'success',
            **result
        })
    except Exception as e:
        logger.error(f"Stale check error: {str(e)}", exc_info=True)
        return json_response({
            'status': 'error',
            'message': str(e)
        }, status=500)

