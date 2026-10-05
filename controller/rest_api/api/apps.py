import logging
import os
import sys

from django.apps import AppConfig

logger = logging.getLogger(__name__)

orchestrator_instance = None
startup_completed = False


#: 요청을 받는 프로세스라고 진입점이 직접 선언하는 변수. wsgi.py 가 올리고,
#: manage.py 는 runserver --noreload 일 때만 올린다.
START_ENV = "KDA_START_ORCHESTRATOR"


def _should_start() -> bool:
    """이 프로세스에서 오케스트레이터(버퍼·리컨실러·리더 선출)를 띄울지.

    서버로 뜬 프로세스에서만 띄워야 한다. 판정을 추론하지 않고 진입점이 선언한다 -
    argv 로 짐작하면 shell·migrate 같은 관리 명령에서도 리컨실러가 돌아버린다.

      WSGI 서버    wsgi.py 가 KDA_START_ORCHESTRATOR 를 올린다.
      runserver    자동 리로더가 자식에만 RUN_MAIN=true 를 넣는다. 부모에서 띄우면
                   워처와 리스가 두 벌이 되므로 자식만 띄운다.
      --noreload   리로더가 없어 RUN_MAIN 이 없으므로 manage.py 가 선언해 준다.

    전에는 'RUN_MAIN != true 면 건너뛴다' 였다. 그러면 리로더가 없는 실행에서는
    오케스트레이터가 한 번도 시작되지 않는다 - 즉 runserver 의 자동 리로더에
    기능 전체가 매여 있었다.
    """
    if os.environ.get(START_ENV) == "true":
        return True
    return os.environ.get("RUN_MAIN") == "true"


class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'

    def ready(self):
        global orchestrator_instance, startup_completed

        if not _should_start():
            return

        if startup_completed:
            return

        try:
            from services.orchestrator import Orchestrator

            logger.info("Controller starting - initializing the warm buffer...")
            orchestrator = Orchestrator()
            result = orchestrator.start()
            orchestrator_instance = orchestrator
            startup_completed = orchestrator.startup_completed

            created = len(result.get("created", []))
            existing = len(result.get("existing", []))
            failed = len(result.get("failed", []))
            logger.info(
                "Buffer init complete: %s created, %s existing, %s failed",
                created,
                existing,
                failed,
            )

        except Exception as exc:
            logger.exception("[Failed] operation=controller_startup reason=%r", str(exc))
            raise
