from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from kda_simulator.config import KUBERNETES_NAMESPACE
    from kda_simulator.reset import ResetError, reset_server

    parser = argparse.ArgumentParser(
        description=(
            f"Reset the {KUBERNETES_NAMESPACE} test server before an experiment: stop controllers, "
            "delete kda:* Redis keys and leftover compute pods, restart controller/swlabssh/compute-general."
        )
    )
    parser.add_argument("--clear-logs", action="store_true", help="Also delete collected JSONL logs on logs-pvc.")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt.")
    args = parser.parse_args()

    if not args.yes:
        extra = " and all collected logs" if args.clear_logs else ""
        answer = input(f"Reset namespace {KUBERNETES_NAMESPACE}: Redis kda:* keys, compute pods{extra}. Type 'yes': ")
        if answer.strip() != "yes":
            raise SystemExit("Aborted")

    try:
        reset_server(clear_logs=args.clear_logs)
    except ResetError as exc:
        raise SystemExit(f"Reset failed: {exc}") from exc


if __name__ == "__main__":
    main()
