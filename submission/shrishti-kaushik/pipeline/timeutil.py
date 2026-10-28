"""UTC timestamps in one fixed format, so text comparison equals time comparison."""

from datetime import datetime, timezone


def utc_now() -> str:
    """Current UTC time as YYYY-MM-DDTHH:MM:SS.sssZ (millisecond precision)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
