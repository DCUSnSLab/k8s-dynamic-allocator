"""
Django settings for Controller Pod REST API
"""

import itertools
import logging
import os
import sys
import threading
from pathlib import Path

# BASE_DIR: rest_api/
BASE_DIR = Path(__file__).resolve().parent.parent

# CONTROLLER_DIR: controller/
CONTROLLER_DIR = BASE_DIR.parent

# Python 경로에 controller/ 추가
if str(CONTROLLER_DIR) not in sys.path:
    sys.path.insert(0, str(CONTROLLER_DIR))

SECRET_KEY = 'django-insecure-controller-pod-secret-key-change-in-production'

DEBUG = True

ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'rest_framework',
    'api',
]

_local = threading.local()
_counter = itertools.count()


def get_request_id():
    return getattr(_local, 'request_id', '-')


def set_request_id(conn_id):
    _local.request_id = conn_id


class RequestIdFilter(logging.Filter):
    def filter(self, record):
        record.request_id = get_request_id()
        return True


def next_conn_id():
    """Generate next conn=N id (called only from execute view)"""
    return f"conn={next(_counter)}"


def _env_int(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def _env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _env_first(names, default):
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def _env_int_any(names, default):
    try:
        return int(_env_first(names, default))
    except (TypeError, ValueError):
        return int(default)


def _env_float_any(names, default):
    try:
        return float(_env_first(names, default))
    except (TypeError, ValueError):
        return float(default)


class RequestIdMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _local.request_id = '-'
        return self.get_response(request)


MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.middleware.common.CommonMiddleware',
    'config.settings.RequestIdMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = []

WSGI_APPLICATION = 'config.wsgi.application'

DATABASES = {}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'controller-conn-cache',
    }
}

REDIS_URL = _env_first(('REDIS_URL',), 'redis://localhost:6379/0')
WAIT_QUEUE_PREFIX = _env_first(('WAIT_QUEUE_PREFIX',), 'kda:waitq')
DEFAULT_BACKEND_TYPE = _env_first(('DEFAULT_BACKEND_TYPE',), 'general')
WAIT_QUEUE_TIMEOUT_SECONDS = _env_int_any(('WAIT_QUEUE_TIMEOUT_SECONDS',), 1800)
WAIT_QUEUE_LOCK_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_LOCK_TTL_SECONDS', 'ALLOCATOR_LOCK_TIMEOUT_SECONDS'),
    60,
)
WAIT_QUEUE_TICKET_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_TICKET_TTL_SECONDS', 'WAIT_TICKET_TTL_SECONDS', 'TICKET_TTL_SECONDS'),
    7200,
)
WAIT_QUEUE_ALLOCATING_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_ALLOCATING_TTL_SECONDS', 'ALLOCATING_STALE_TIMEOUT_SECONDS'),
    60,
)
WAIT_QUEUE_MAX_RETRIES = _env_int_any(('WAIT_QUEUE_MAX_RETRIES',), 3)
WAIT_QUEUE_WORKER_INTERVAL_SECONDS = _env_float_any(
    ('WAIT_QUEUE_WORKER_INTERVAL_SECONDS', 'QUEUE_WORKER_INTERVAL_SECONDS'),
    1.0,
)
WAIT_QUEUE_BACKEND_REFRESH_SECONDS = _env_float_any(
    ('WAIT_QUEUE_BACKEND_REFRESH_SECONDS', 'BACKEND_REGISTRY_REFRESH_SECONDS'),
    15.0,
)
BACKEND_AGENT_TIMEOUT_SECONDS = _env_float_any(('BACKEND_AGENT_TIMEOUT_SECONDS',), 30.0)
BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS = _env_float_any(
    ('BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS',),
    BACKEND_AGENT_TIMEOUT_SECONDS,
)
BACKEND_AGENT_UNMOUNT_TIMEOUT_SECONDS = _env_float_any(
    ('BACKEND_AGENT_UNMOUNT_TIMEOUT_SECONDS',),
    BACKEND_AGENT_TIMEOUT_SECONDS,
)

LANGUAGE_CODE = 'ko-kr'
TIME_ZONE = 'Asia/Seoul'
USE_I18N = True
USE_TZ = True

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': [
        'rest_framework.renderers.JSONRenderer',
    ],
    'DEFAULT_PARSER_CLASSES': [
        'rest_framework.parsers.JSONParser',
    ],
}

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '[{asctime}] [{levelname}] [{request_id}] {message}',
            'style': '{',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'filters': {
        'request_id': {
            '()': 'config.settings.RequestIdFilter',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'detailed',
            'filters': ['request_id'],
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'django.server': {
            'handlers': ['console'],
            'level': 'CRITICAL',
            'propagate': False,
        },
        'django.request': {
            'handlers': ['console'],
            'level': 'CRITICAL',
            'propagate': False,
        },
        'api': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'services': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'httpx': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False,
        },
    },
}
