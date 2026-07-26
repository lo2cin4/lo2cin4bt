import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if __name__ == "__main__":
    from app.server import run_server

    run_server(REPO_ROOT, host="127.0.0.1", port=2424, no_browser=True)
