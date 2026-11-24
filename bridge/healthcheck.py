"""Docker healthcheck: healthy iff the main loop touched the heartbeat file < MAX_AGE s ago."""
import os
import sys
import time

path = os.environ.get("HEARTBEAT_FILE", "/tmp/bridge-heartbeat")
max_age = float(os.environ.get("HEARTBEAT_MAX_AGE", "30"))
try:
    age = time.time() - os.path.getmtime(path)
except OSError as e:
    print(f"no heartbeat: {e}")
    sys.exit(1)
print(f"heartbeat age {age:.1f}s (max {max_age:.0f}s)")
sys.exit(0 if age < max_age else 1)
