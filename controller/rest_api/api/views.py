from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from datetime import datetime

from services import Orchestrator

logger = logging.getLogger(__name__)

# Orchestrator 인스턴스
orchestrator = Orchestrator()


def json_response(data, status=200):
    """들여쓰기된 JSON 응답 생성 (끝에 줄바꿈 포함)"""
    json_str = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    return HttpResponse(
        json_str,
        content_type='application/json; charset=utf-8',
        status=status
    )


@csrf_exempt
def health_check(request):
    """컨트롤러 헬스 체크"""
    try:
        result = orchestrator.health_check()
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
    """Backend Pool 상태 조회"""
    if request.method != 'GET':
        return json_response({'status': 'error', 'message': 'GET 메서드만 지원합니다'}, status=405)
    
    try:
        result = orchestrator.get_pool_status()
        return json_response(result)
    except Exception as e:
        logger.error(f"Pool 상태 조회 에러: {e}", exc_info=True)
        return json_response({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
def initialize_pool(request):
    """Backend Pool 수동 초기화"""
    if request.method != 'POST':
        return json_response({'status': 'error', 'message': 'POST 메서드만 지원합니다'}, status=405)
    
    try:
        result = orchestrator.initialize_pool()
        return json_response({'status': 'success', 'message': 'Pool 초기화 완료', 'result': result})
    except Exception as e:
        logger.error(f"Pool 초기화 에러: {e}", exc_info=True)
        return json_response({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
def interactive_start(request):
    """
    Interactive 세션 시작
    
    Request Body:
        {"frontend_pod": "ssh-ami159"}
    
    Response:
        {"status": "success", "backend_pod": "...", "backend_ip": "..."}
    """
    if request.method != 'POST':
        return json_response({'status': 'error', 'message': 'POST 메서드만 지원합니다'}, status=405)
    
    try:
        data = json.loads(request.body)
        frontend_pod = data.get('frontend_pod', '')
        
        if not frontend_pod:
            return json_response({'status': 'error', 'message': 'frontend_pod는 필수입니다'}, status=400)
        
        logger.info(f"[Controller] Interactive start: frontend={frontend_pod}")
        
        result = orchestrator.interactive_start(frontend_pod)
        status_code = 200 if result['status'] == 'success' else 500
        
        return json_response(result, status=status_code)
        
    except json.JSONDecodeError:
        return json_response({'status': 'error', 'message': '잘못된 JSON 형식입니다'}, status=400)
    except Exception as e:
        logger.error(f"Interactive start 에러: {e}", exc_info=True)
        return json_response({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
def interactive_end(request):
    """
    Interactive 세션 종료
    
    Request Body:
        {"backend_pod": "backend-pool-0"}
    """
    if request.method != 'POST':
        return json_response({'status': 'error', 'message': 'POST 메서드만 지원합니다'}, status=405)
    
    try:
        data = json.loads(request.body)
        backend_pod = data.get('backend_pod', '')
        
        if not backend_pod:
            return json_response({'status': 'error', 'message': 'backend_pod는 필수입니다'}, status=400)
        
        logger.info(f"[Controller] Interactive end: backend={backend_pod}")
        
        result = orchestrator.interactive_end(backend_pod)
        status_code = 200 if result['status'] == 'success' else 500
        
        return json_response(result, status=status_code)
        
    except json.JSONDecodeError:
        return json_response({'status': 'error', 'message': '잘못된 JSON 형식입니다'}, status=400)
    except Exception as e:
        logger.error(f"Interactive end 에러: {e}", exc_info=True)
        return json_response({'status': 'error', 'message': str(e)}, status=500)


@csrf_exempt
def release_backend(request):
    """Backend Pod 할당 해제 (interactive_end와 동일)"""
    return interactive_end(request)
