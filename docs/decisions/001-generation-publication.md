# ADR 001: Publish generation bundles, not tables

Status: accepted

Independent table updates can expose a new aggregate with old sessions or corrected sessions with
old exceptions. PulseStream therefore makes one manifest the release unit. The manifest names exact
snapshots for all required tables; a conditional pointer update makes the bundle active.

This adds candidate lifecycle and cleanup work, but it makes rollback metadata-only, rejects stale
writers and gives every visible result one reproducible lineage record.
