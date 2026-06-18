"""UTC timestamps and TTL for universe documents."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def utc_now() -> datetime:
    return datetime.now(tz=UTC)


def to_iso_z(dt: datetime) -> str:
    """Format as ISO-8601 with Z suffix (UTC)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def add_ttl_seconds(dt: datetime, ttl_seconds: int) -> datetime:
    return dt + timedelta(seconds=ttl_seconds)


def parse_iso_z(s: str) -> datetime:
    """Parse ISO-8601 UTC string with Z suffix to aware datetime."""
    t = s.strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    return datetime.fromisoformat(t)


def age_sec_from_iso_z(s: str, *, now: datetime | None = None) -> int:
    """Seconds from an ISO-8601 UTC timestamp to now, floored at 0."""
    end = now if now is not None else utc_now()
    start = parse_iso_z(s)
    return max(0, int((end - start.astimezone(UTC)).total_seconds()))
