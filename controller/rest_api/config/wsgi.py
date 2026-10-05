"""
WSGI config for config project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/5.2/howto/deployment/wsgi/
"""

import os

from .bootstrap import load_from_env

# Django 설정을 읽기 전에 kda_config.py 값을 환경변수로 올린다. manage.py 가
# --config-file 로 하던 일을 WSGI 경로에서도 해야 한다.
load_from_env()

# 이 프로세스가 요청을 받는 서버라고 알린다. api/apps.py 가 이것을 보고 버퍼와
# 리컨실러를 띄운다 - 관리 명령에서는 뜨지 않아야 하므로 진입점이 선언한다.
os.environ.setdefault('KDA_START_ORCHESTRATOR', 'true')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

from django.core.wsgi import get_wsgi_application  # noqa: E402

application = get_wsgi_application()
