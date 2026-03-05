"""
Django settings for Controller Pod REST API
"""

import itertools
import logging
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
