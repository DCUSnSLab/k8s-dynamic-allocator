from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'
    
    def ready(self):
        """
        앱 시작 시 호출됨
        
        Backend Pool 자동 초기화
        """
        # Django 개발 서버의 reloader 중복 실행 방지
        import os
        if os.environ.get('RUN_MAIN', None) != 'true':
            return
        
        try:
            from services import Orchestrator
            
            logger.info("=" * 60)
            logger.info("[Controller] 앱 시작 - Backend Pool 초기화 중...")
            
            orchestrator = Orchestrator()
            result = orchestrator.initialize_pool()
            
            logger.info(f"[Controller] Pool 초기화 결과: {result}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"[Controller] Pool 초기화 실패: {e}")
            # 초기화 실패해도 앱은 시작하도록 함
