"""
Django settings for Controller Pod REST API
"""

import logging
import os
import sys
import threading
from contextlib import contextmanager
from pathlib import Path

# BASE_DIR: rest_api/
BASE_DIR = Path(__file__).resolve().parent.parent

# CONTROLLER_DIR: controller/
CONTROLLER_DIR = BASE_DIR.parent

# Python 경로에 controller/ 추가
if str(CONTROLLER_DIR) not in sys.path:
    sys.path.insert(0, str(CONTROLLER_DIR))

SECRET_KEY = os.getenv('DJANGO_SECRET_KEY', 'django-insecure-controller-pod-secret-key-change-in-production')

DEBUG = os.getenv('DJANGO_DEBUG', 'true').lower() in ('true', '1', 'yes')

ALLOWED_HOSTS = ['*']

INSTALLED_APPS = [
    'rest_framework',
    'api',
]

_local = threading.local()


def get_request_label():
    return getattr(_local, 'request_label', '-')


def set_request_label(request_label):
    _local.request_label = request_label
    _local.request_id = request_label


@contextmanager
def request_label_scope(request_label=None):
    """Restore this thread's request label on exit, optionally setting one for the block.

    The label lives in a thread-local, so a long-lived thread keeps whatever the
    last caller set. Code that changes it on behalf of one request wraps itself
    in this so the next log line on the same thread is not attributed to it.
    """
    previous = get_request_label()
    if request_label is not None:
        set_request_label(request_label)
    try:
        yield
    finally:
        set_request_label(previous)


def build_request_label(username, ticket_short=None):
    user = (username or "-").strip() or "-"
    ticket_short_value = (ticket_short or "").strip()
    if ticket_short_value:
        return f"{user}-{ticket_short_value[:10]}"
    return user


class RequestLabelFilter(logging.Filter):
    def filter(self, record):
        request_label = get_request_label()
        record.request_label = request_label
        record.request_id = request_label
        return True


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


def _env_bool_any(names, default):
    value = _env_first(names, default)
    if isinstance(value, bool):
        return value
    return str(value).lower() in ("true", "1", "yes", "on")


class RequestLabelMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        _local.request_label = '-'
        _local.request_id = '-'
        return self.get_response(request)



MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.middleware.common.CommonMiddleware',
    'config.settings.RequestLabelMiddleware',
]

ROOT_URLCONF = 'config.urls'

TEMPLATES = []

WSGI_APPLICATION = 'config.wsgi.application'

DATABASES = {}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        'LOCATION': 'controller-request-cache',
    }
}

REDIS_URL = _env_first(('REDIS_URL',), 'redis://localhost:6379/0')
WAIT_QUEUE_PREFIX = _env_first(('WAIT_QUEUE_PREFIX',), 'kda:waitq')
DEFAULT_COMPUTE_TYPE = _env_first(('DEFAULT_COMPUTE_TYPE',), 'general')
# 이 브랜치(exp-cold-start)는 요청마다 파드를 만들고 반납 때 지우는 비교군이다.
# 브랜치가 곧 arm 이므로 모드는 상수다 - 환경변수로 읽으면 오타 하나로 다른 arm 이
# 조용히 배포된다. 상태 API 와 평가 도구가 이 이름으로 arm 을 기록한다.
COMPUTE_ALLOCATION_MODE = 'cold_start'
COMPUTE_POD_IMAGE = _env_first(
    ('COMPUTE_POD_IMAGE',),
    'harbor.cu.ac.kr/k8s_dynamic_allocator/compute_pod:latest',
).strip()
# Counted from the client's last poll, not from enqueue: a polling client waits
# as long as the queue takes, and a ticket silent this long is failed. 0 disables.
WAIT_QUEUE_TIMEOUT_SECONDS = _env_int_any(('WAIT_QUEUE_TIMEOUT_SECONDS',), 1800)
WAIT_QUEUE_LOCK_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_LOCK_TTL_SECONDS', 'ALLOCATOR_LOCK_TIMEOUT_SECONDS'),
    60,
)
WAIT_QUEUE_LOCK_RENEW_SECONDS = _env_int_any(
    ('WAIT_QUEUE_LOCK_RENEW_SECONDS',),
    20,
)
# Renewed on every poll while a ticket waits, so it only expires abandoned tickets.
WAIT_QUEUE_TICKET_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_TICKET_TTL_SECONDS', 'WAIT_TICKET_TTL_SECONDS', 'TICKET_TTL_SECONDS'),
    7200,
)
WAIT_QUEUE_ALLOCATING_TTL_SECONDS = _env_int_any(
    ('WAIT_QUEUE_ALLOCATING_TTL_SECONDS', 'ALLOCATING_STALE_TIMEOUT_SECONDS'),
    60,
)
# A queued client polls its ticket about once a second. After this much silence
# the allocator skips the ticket instead of handing it a Compute Pod; the ticket
# keeps its queue position, so a client that reconnects loses nothing. 0 disables.
WAIT_QUEUE_CLIENT_TIMEOUT_SECONDS = _env_int_any(
    ('WAIT_QUEUE_CLIENT_TIMEOUT_SECONDS',),
    60,
)
# How long a Compute Pod may stay NotReady before cleanup deletes it (the buffer
# reconciler then creates a usable one if the target still asks for it). 30s matches Kubernetes' own default
# livenessProbe verdict (periodSeconds 10 x failureThreshold 3). 0 disables.
COMPUTE_NOT_READY_GRACE_SECONDS = _env_int_any(
    ('COMPUTE_NOT_READY_GRACE_SECONDS',),
    30,
)
ASSIGNED_CONTEXT_TTL_SECONDS = _env_int_any(
    ('ASSIGNED_CONTEXT_TTL_SECONDS',),
    2592000,
)
WAIT_QUEUE_MAX_RETRIES = _env_int_any(('WAIT_QUEUE_MAX_RETRIES',), 3)
WAIT_QUEUE_WORKER_INTERVAL_SECONDS = _env_float_any(
    ('WAIT_QUEUE_WORKER_INTERVAL_SECONDS', 'QUEUE_WORKER_INTERVAL_SECONDS'),
    1.0,
)
WAIT_QUEUE_COMPUTE_REFRESH_SECONDS = _env_float_any(
    ('WAIT_QUEUE_COMPUTE_REFRESH_SECONDS',),
    15.0,
)
WAIT_QUEUE_BATCH_LIMIT = _env_int_any(('WAIT_QUEUE_BATCH_LIMIT',), 10)
WAIT_QUEUE_MOUNT_CONCURRENCY = _env_int_any(
    ('WAIT_QUEUE_MOUNT_CONCURRENCY',),
    WAIT_QUEUE_BATCH_LIMIT,
)
COMPUTE_AVAILABILITY_WATCH_ENABLED = _env_bool_any(
    ('COMPUTE_AVAILABILITY_WATCH_ENABLED',),
    True,
)
COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS = _env_int_any(
    ('COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS', 'COMPUTE_AVAILABILITY_WATCH_RESYNC_SECONDS'),
    60,
)
COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS = _env_float_any(
    ('COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS',),
    1.0,
)
# 리더의 주기 정리(만료된 claim 복구, 고아 파드 회수, 남은 Deployment 버퍼 내리기) 간격.
BUFFER_RECONCILE_RESYNC_SECONDS = _env_int_any(
    ('BUFFER_RECONCILE_RESYNC_SECONDS',),
    60,
)

# ---- cold_start (요청마다 파드를 만든다)
#
# N(노드 위 compute 파드 상한). 컨트롤러 환경변수로 받고 deploy.sh 가 빌드 파라미터를
# 넣는다. 실험 중 바꿀 일이 없어 시작 시 한 번 읽는다(값은 status API 와 로그에 남는다).
# N 이 비면 파드를 하나도 만들지 않는다(시작 로그에 오류가 남는다). 설정 누락으로
# 상한 없는 run 이 조용히 생기지 않게 한다.
WARM_BUFFER_CAPACITY = _env_first(('BUFFER_CAPACITY', 'WARM_BUFFER_CAPACITY'), '').strip()
# 리더가 남은 idle 파드(다른 arm 의 것)를 확인하는 주기의 상한. idle 파드 이벤트로
# 바로 깨어나므로 이 값은 놓친 이벤트를 메우는 바닥일 뿐이다.
WARM_BUFFER_RESYNC_SECONDS = _env_float_any(('WARM_BUFFER_RESYNC_SECONDS',), 2.0)
# 생성 요청을 보냈지만 목록에 아직 안 보이는 파드를 N 에 세는 시간의 상한. 응답을 못
# 받은 생성 요청이 서버에 남아 있을 수 있는 시간은 apiserver 요청 timeout(기본
# --request-timeout 60s)이 상한이라 그 값을 쓴다. 그 뒤에도 목록에 없으면 만들어지지
# 않은 것이다.
WARM_BUFFER_PENDING_CREATE_SECONDS = _env_float_any(('WARM_BUFFER_PENDING_CREATE_SECONDS',), 60.0)
# Ready 를 기다리는 상한과 폴링 간격. cold start 와 같은 값이다.
COLD_START_POD_READY_TIMEOUT_SECONDS = _env_float_any(
    ('COLD_START_POD_READY_TIMEOUT_SECONDS',),
    300.0,
)
COLD_START_POD_READY_POLL_SECONDS = _env_float_any(
    ('COLD_START_POD_READY_POLL_SECONDS',),
    0.5,
)

COMPUTE_AGENT_TIMEOUT_SECONDS = _env_float_any(('COMPUTE_AGENT_TIMEOUT_SECONDS',), 30.0)
COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS = _env_float_any(
    ('COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS',),
    COMPUTE_AGENT_TIMEOUT_SECONDS,
)
COMPUTE_AGENT_UNMOUNT_TIMEOUT_SECONDS = _env_float_any(
    ('COMPUTE_AGENT_UNMOUNT_TIMEOUT_SECONDS',),
    COMPUTE_AGENT_TIMEOUT_SECONDS,
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

_LOG_FORMAT = os.getenv('LOG_FORMAT', 'detailed').lower()
_CONSOLE_FORMATTER = 'json' if _LOG_FORMAT == 'json' else 'detailed'
_APP_LOG_LEVEL = os.getenv('APP_LOG_LEVEL', 'INFO').upper()

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '[{asctime}] [{levelname}] [{request_label}] {message}',
            'style': '{',
            'datefmt': '%Y-%m-%d %H:%M:%S %z',
        },
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(asctime)s %(levelname)s %(name)s %(request_label)s %(message)s',
            'rename_fields': {'asctime': 'ts', 'levelname': 'level', 'name': 'logger'},
            'datefmt': '%Y-%m-%dT%H:%M:%S%z',
        },
    },
    'filters': {
        'request_label': {
            '()': 'config.settings.RequestLabelFilter',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': _CONSOLE_FORMATTER,
            'filters': ['request_label'],
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
            'level': _APP_LOG_LEVEL,
            'propagate': False,
        },
        'services': {
            'handlers': ['console'],
            'level': _APP_LOG_LEVEL,
            'propagate': False,
        },
        'httpx': {
            'handlers': ['console'],
            'level': 'WARNING',
            'propagate': False,
        },
    },
}
