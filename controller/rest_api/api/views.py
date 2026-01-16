from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from datetime import datetime

from services import Orchestrator

logger = logging.getLogger(__name__)

# Orchestrator 인스턴스
orchestrator = Orchestrator()


@csrf_exempt
def execute_command(request):
    """
    Frontend Pod로부터 Backend Pod 실행 요청을 받는 API
    
    Request Body:
        {
            "username": "사용자명",
            "command": "실행할 명령어",
            "frontend_pod": "Frontend Pod 이름"
        }
    
    Response:
        {
            "status": "success" | "error",
            "message": "응답 메시지",
            "backend_pod": "할당된 Pod 이름",
            "submitted_at": "2025-10-28T..."
        }
    """
    if request.method != 'POST':
        return JsonResponse({
            'status': 'error',
            'message': 'POST 메서드만 지원합니다'
        }, status=405)
    
    try:
        data = json.loads(request.body)
        username = data.get('username', '')
        command = data.get('command', '')
        frontend_pod = data.get('frontend_pod', '')
        
        # 입력 검증
        if not username:
            return JsonResponse({
                'status': 'error',
                'message': 'username은 필수입니다'
            }, status=400)
        
        if not command:
            return JsonResponse({
                'status': 'error',
                'message': 'command는 필수입니다'
            }, status=400)
        
        logger.info("=" * 60)
        logger.info("[Controller] 새로운 명령 실행 요청 수신")
        logger.info(f"  Username: {username}")
        logger.info(f"  Command: {command}")
        logger.info(f"  Frontend Pod: {frontend_pod}")
        logger.info(f"  Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        # Backend Pod 할당 및 실행 요청
        result = orchestrator.execute_command(username, command, frontend_pod)
        
        # 로그 출력
        if result['status'] == 'success':
            logger.info(f"[Controller] 명령 전달 성공: {result.get('backend_pod')}")
        else:
            logger.error(f"[Controller] 명령 전달 실패: {result.get('message')}")
        
        logger.info("=" * 60)
        
        # HTTP 상태 코드 결정
        status_code = 200 if result['status'] == 'success' else 500
        
        return JsonResponse(result, status=status_code)
        
    except json.JSONDecodeError as e:
        logger.error(f"[Controller] JSON 파싱 에러: {str(e)}")
        return JsonResponse({
            'status': 'error',
            'message': '잘못된 JSON 형식입니다',
            'detail': str(e)
        }, status=400)
        
    except Exception as e:
        logger.error(f"[Controller] 예상치 못한 에러: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': '서버 내부 에러가 발생했습니다',
            'detail': str(e)
        }, status=500)


@csrf_exempt
def health_check(request):
    """
    컨트롤러 Pod 헬스 체크 엔드포인트
    """
    try:
        result = orchestrator.health_check()
    except Exception as e:
        result = "[error] " + str(e)

    return JsonResponse({
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
        return JsonResponse({
            'status': 'error',
            'message': 'GET 메서드만 지원합니다'
        }, status=405)
    
    try:
        result = orchestrator.get_pool_status()
        return JsonResponse(result)
    except Exception as e:
        logger.error(f"[Controller] Pool 상태 조회 에러: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
def initialize_pool(request):
    """
    Backend Pool 수동 초기화 엔드포인트
    
    참고: Controller 시작 시 자동으로 초기화되지만,
    수동 재초기화가 필요한 경우 사용
    """
    if request.method != 'POST':
        return JsonResponse({
            'status': 'error',
            'message': 'POST 메서드만 지원합니다'
        }, status=405)
    
    try:
        result = orchestrator.initialize_pool()
        return JsonResponse({
            'status': 'success',
            'message': 'Pool 초기화 완료',
            'result': result
        })
    except Exception as e:
        logger.error(f"[Controller] Pool 초기화 에러: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)


@csrf_exempt
def release_backend(request):
    """
    Backend Pod 할당 해제 엔드포인트
    
    Request Body:
        {
            "backend_pod": "backend-pool-0"
        }
    """
    if request.method != 'POST':
        return JsonResponse({
            'status': 'error',
            'message': 'POST 메서드만 지원합니다'
        }, status=405)
    
    try:
        data = json.loads(request.body)
        backend_pod = data.get('backend_pod', '')
        
        if not backend_pod:
            return JsonResponse({
                'status': 'error',
                'message': 'backend_pod는 필수입니다'
            }, status=400)
        
        result = orchestrator.release_backend(backend_pod)
        
        status_code = 200 if result['status'] == 'success' else 500
        return JsonResponse(result, status=status_code)
        
    except json.JSONDecodeError as e:
        return JsonResponse({
            'status': 'error',
            'message': '잘못된 JSON 형식입니다'
        }, status=400)
    except Exception as e:
        logger.error(f"[Controller] Backend 해제 에러: {str(e)}", exc_info=True)
        return JsonResponse({
            'status': 'error',
            'message': str(e)
        }, status=500)
