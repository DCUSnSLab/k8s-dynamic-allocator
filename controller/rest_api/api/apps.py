from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

# apps.py에서 생성한 인스턴스를 views.py에서 import
orchestrator_instance = None


class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'
    
    def ready(self):
        """
        Called on app startup.
        Auto-initializes Backend Pool.
        """
        global orchestrator_instance
        
        # Django 개발 서버의 reloader 중복 실행 방지
        import os
        if os.environ.get('RUN_MAIN', None) != 'true':
            return
        
        try:
            from services import Orchestrator
            
            logger.info("Controller starting - initializing backend pool...")
            
            orchestrator_instance = Orchestrator()
            result = orchestrator_instance.initialize_pool()
            
            created = len(result.get('created', []))
            existing = len(result.get('existing', []))
            failed = len(result.get('failed', []))
            logger.info(f"Pool init complete: {created} created, {existing} existing, {failed} failed")
            
        except Exception as e:
            logger.error(f"Pool init failed: {e}")

