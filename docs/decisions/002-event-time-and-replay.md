# ADR 002: Keep LIVE bounded and make corrections explicit

Status: accepted

Waiting forever for mobile and partner events makes freshness unbounded. Dropping late data makes
history silently wrong. PulseStream uses a versioned event-time watermark for LIVE, records events
beyond it as OPEN corrections, and applies them through a controlled REPLAY generation pinned to a
published frontier.

The trade-off is temporary divergence between LIVE and corrected history, made observable through
the correction table and replay SLO.
