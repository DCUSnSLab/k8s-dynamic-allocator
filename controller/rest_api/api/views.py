from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


@csrf_exempt
def execute_command(request):
    """
    Frontend Pod로부터 Backend Pod 실행 요청을 받는 API
    
    현재 단계: 요청 수신 및 로깅만 수행 (테스트용)
    향후: services/pod_manager.py의 로직 호출 예정
    
    Request Body:
        {
            "username": "사용자명",
            "command": "실행할 명령어"
        }
    
    Response:
        {
            "status": "success" | "error",
            "message": "응답 메시지",
            "received": {...},
            "timestamp": "2025-10-22T..."
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
        
        logger.info("=" * 60)
        logger.info("[Controller Pod] 새로운 요청 수신")
        logger.info(f"  Username: {username}")
        logger.info(f"  Command: {command}")
        logger.info(f"  Timestamp: {datetime.now().isoformat()}")
        logger.info("=" * 60)
        
        # TODO: services/pod_manager.py 구현 후 호출
        # from services.pod_manager import PodManager
        # pod_manager = PodManager()
        # result = pod_manager.create_backend_pod(username, command)
        # return JsonResponse(result)
        
        response_data = {
            'status': 'success',
            'message': '요청을 정상적으로 수신했습니다 (현재는 테스트 모드)',
            'received': {
                'username': username,
                'command': command
            },
            'timestamp': datetime.now().isoformat(),
            'note': 'services/pod_manager.py 구현 후 실제 Pod 생성 예정'
        }
        
        logger.info(f"[Controller Pod] 응답 전송: {response_data['status']}")
        return JsonResponse(response_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"[Controller Pod] JSON 파싱 에러: {str(e)}")
        return JsonResponse({
            'status': 'error',
            'message': '잘못된 JSON 형식입니다',
            'detail': str(e)
        }, status=400)
        
    except Exception as e:
        logger.error(f"[Controller Pod] 예상치 못한 에러: {str(e)}", exc_info=True)
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
    return JsonResponse({
        'status': 'healthy',
        'service': 'Controller Pod REST API',
        'timestamp': datetime.now().isoformat()
    })
