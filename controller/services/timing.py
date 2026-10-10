"""실험 계측 전용. KDA_TIMING_LOG=1 일 때만 동작하며 제어 흐름은 바꾸지 않는다.

왜 필요한가: 포화에서 슬롯 하나를 다시 채우는 세 단계(replicas 증가 -> 게이트
해제 -> 할당)가 리더의 reconcile 바퀴와 allocator 락 하나에 묶여 줄을 선다.
실측으로 replicas 가 목표보다 낮은 샘플이 10/9 에는 0% 였는데 게이트 도입 후
40.3% 가 됐고, Ready 후 할당 대기는 1.0 -> 2.8초가 됐다. 그런데 지금 로그로는
그 지연이 세 단계 중 어디서 생겼는지 나눌 수 없다. 이 모듈은 그 몫을 재기만 한다.

수집 로그의 시각이 1초 단위로 잘리므로 모든 줄에 at_ms(epoch ms)를 직접 남긴다.
"""

import contextvars
import functools
import logging
import os
import threading
import time
from contextlib import contextmanager

logger = logging.getLogger(__name__)

ENABLED = os.environ.get("KDA_TIMING_LOG", "").strip().lower() in (
    "1", "true", "yes", "on",
)
FLUSH_SECONDS = max(5.0, float(os.environ.get("KDA_TIMING_FLUSH_SECONDS", "30")))
POD = os.environ.get("HOSTNAME", "-")

# 락을 누가 잡았는지는 호출 깊이가 달라 인자로 넘기기 어렵다. 스레드/태스크별
# 문맥으로 두면 획득 지점과 반납 지점이 떨어져 있어도 같은 값을 읽는다.
_purpose = contextvars.ContextVar("kda_lock_purpose", default="other")
_trigger = contextvars.ContextVar("kda_drain_trigger", default="direct")
_timer = contextvars.ContextVar("kda_phase_timer", default=None)

_leader_probe = None


def set_leader_probe(fn):
    global _leader_probe
    _leader_probe = fn


def _is_leader():
    try:
        return bool(_leader_probe()) if _leader_probe else False
    except Exception:
        return False


def now_ms():
    return int(time.time() * 1000)


def emit(event, **fields):
    if not ENABLED:
        return
    _ensure_flusher()
    parts = [
        "at_ms=%d" % now_ms(),
        "pod=%s" % POD,
        "leader=%d" % int(_is_leader()),
        "thread=%s" % threading.current_thread().name,
    ]
    for key, value in fields.items():
        if isinstance(value, float):
            value = "%.1f" % value
        parts.append("%s=%s" % (key, value))
    logger.info("[%s] %s", event, " ".join(parts))


# ---------------------------------------------------------------- 문맥

@contextmanager
def scope(purpose=None, trigger=None):
    tokens = []
    if purpose:
        tokens.append((_purpose, _purpose.set(purpose)))
    if trigger:
        tokens.append((_trigger, _trigger.set(trigger)))
    try:
        yield
    finally:
        for var, token in reversed(tokens):
            var.reset(token)


def current_purpose():
    return _purpose.get()


def current_trigger():
    return _trigger.get()


class PhaseTimer:
    """구간별 소요를 누적한다. mark 를 부른 지점까지의 시간이 그 이름으로 쌓인다."""

    def __init__(self):
        self.t0 = time.monotonic()
        self.last = self.t0
        self.phases = {}
        self.notes = {}

    def mark(self, name):
        now = time.monotonic()
        self.phases[name] = self.phases.get(name, 0.0) + (now - self.last) * 1000
        self.last = now

    def total_ms(self):
        return (time.monotonic() - self.t0) * 1000

    def fields(self):
        return {"%s_ms" % name: value for name, value in self.phases.items()}


@contextmanager
def phase_timer():
    timer = PhaseTimer()
    token = _timer.set(timer)
    try:
        yield timer
    finally:
        _timer.reset(token)


def phase(name):
    timer = _timer.get()
    if timer is not None:
        timer.mark(name)


def note(**kwargs):
    timer = _timer.get()
    if timer is not None:
        timer.notes.update(kwargs)


# ---------------------------------------------------------------- 집계

class _Stats:
    def __init__(self):
        self._guard = threading.Lock()
        self._data = {}

    def add(self, key, ms):
        with self._guard:
            self._data.setdefault(key, []).append(ms)

    def take(self):
        with self._guard:
            data, self._data = self._data, {}
        return data


_stats = _Stats()


def timed(kind):
    """호출 건수와 소요를 모아 30초마다 한 줄로 낸다. 호출마다 찍으면 너무 많다."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not ENABLED:
                return fn(*args, **kwargs)
            started = time.monotonic()
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                _stats.add("err.%s.%s" % (kind, type(exc).__name__), 0.0)
                raise
            finally:
                _stats.add(kind, (time.monotonic() - started) * 1000)
        return wrapper
    return decorator


# ---------------------------------------------------------------- 할당 락

_lock_meta = {}
_lock_guard = threading.Lock()


def lock_acquired(token):
    if ENABLED and token:
        with _lock_guard:
            _lock_meta[token] = (time.monotonic(), _purpose.get())


def lock_missed():
    if ENABLED:
        _stats.add("lock_miss.%s" % _purpose.get(), 0.0)


def lock_released(token):
    if not ENABLED or not token:
        return
    with _lock_guard:
        meta = _lock_meta.pop(token, None)
    if meta is None:
        return
    emit("LockHold", purpose=meta[1], hold_ms=(time.monotonic() - meta[0]) * 1000)


# ---------------------------------------------------------------- 주기 출력

_flusher_started = False
_flusher_guard = threading.Lock()


def start():
    if ENABLED:
        _ensure_flusher()


def _ensure_flusher():
    global _flusher_started
    if _flusher_started:
        return
    with _flusher_guard:
        if _flusher_started:
            return
        _flusher_started = True
        threading.Thread(
            target=_flush_loop, name="kda-timing-flush", daemon=True
        ).start()


def _read_cgroup_cpu():
    for path in (
        "/sys/fs/cgroup/cpu.stat",
        "/sys/fs/cgroup/cpu,cpuacct/cpu.stat",
        "/sys/fs/cgroup/cpu/cpu.stat",
    ):
        try:
            with open(path, encoding="utf-8") as handle:
                values = {}
                for line in handle:
                    parts = line.split()
                    if len(parts) == 2 and parts[1].isdigit():
                        values[parts[0]] = int(parts[1])
                return values
        except OSError:
            continue
    return {}


def _pct(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


def _flush_loop():
    prev_cpu = time.process_time()
    prev_wall = time.monotonic()
    prev_cg = _read_cgroup_cpu()
    while True:
        time.sleep(FLUSH_SECONDS)
        try:
            for kind, values in sorted(_stats.take().items()):
                emit(
                    "TimingStats",
                    kind=kind,
                    n=len(values),
                    p50_ms=float(_pct(values, 0.5)),
                    p90_ms=float(_pct(values, 0.9)),
                    max_ms=float(max(values)),
                    sum_ms=float(sum(values)),
                )
            cpu = time.process_time()
            wall = time.monotonic()
            cg = _read_cgroup_cpu()
            delta = {k: cg[k] - prev_cg.get(k, 0) for k in cg}
            emit(
                "ProcStat",
                window_s=wall - prev_wall,
                proc_cpu_ms=(cpu - prev_cpu) * 1000,
                threads=threading.active_count(),
                nr_periods=delta.get("nr_periods", "-"),
                nr_throttled=delta.get("nr_throttled", "-"),
                throttled_usec=delta.get(
                    "throttled_usec", delta.get("throttled_time", "-")
                ),
                usage_usec=delta.get("usage_usec", "-"),
            )
            prev_cpu, prev_wall, prev_cg = cpu, wall, cg
        except Exception as exc:  # noqa: BLE001 - 계측이 본체를 멈추면 안 된다
            logger.warning("[Warning] operation=timing_flush reason=%r", str(exc))
