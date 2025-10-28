from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from datetime import datetime

from services import BackendOrchestrator

logger = logging.getLogger(__name__)


@csrf_exempt
def execute_command(request):
    """
    Frontend Pod로부터 Backend Pod 실행 요청을 받는 API
    
    Request Body:
        {
            "username": "사용자명",
            "command": "실행할 명령어"
        }
    
    Response:
        {
            "status": "success" | "error",
            "message": "응답 메시지",
            "pod_name": "생성된 Pod 이름",
            "namespace": "네임스페이스",
            "created_at": "2025-10-28T..."
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
        logger.info("[Controller] 새로운 Backend Pod 생성 요청 수신")
        logger.info(f"  Username: {username}")
        logger.info(f"  Command: {command}")
        logger.info(f"  Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        # Backend Pod 생성
        orchestrator = BackendOrchestrator()
        result = orchestrator.create_backend_pod(username, command)
        
        # 로그 출력
        if result['status'] == 'success':
            logger.info(f"[Controller] Pod 생성 성공: {result.get('pod_name')}")
        else:
            logger.error(f"[Controller] Pod 생성 실패: {result.get('message')}")
        
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
        orchestrator = BackendOrchestrator()
        result = orchestrator.health_check()

    except Exception as e:
        result = "[error] " + str(e)

    return JsonResponse({
        'status': 'healthy',
        'service': 'Controller Pod REST API',
        'timestamp': datetime.now().isoformat(),
        'result': result
    })
