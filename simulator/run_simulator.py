from __future__ import annotations

import asyncio
import sys
from pathlib import Path


def _add_project_path() -> None:
    here = Path(__file__).resolve()
    sys.path.insert(0, str(here.parent))


def main() -> None:
    _add_project_path()
    from kda_simulator.main import main as simulator_main

    asyncio.run(simulator_main())


if __name__ == "__main__":
    main()
