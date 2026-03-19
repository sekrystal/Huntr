# audit_freshness

Use this skill when evaluating whether stale or expired roles are leaking into surfaced results.

## Goal

Prove that fresh active listings surface by default and stale or expired records do not.

## Validation Checklist

1. reset demo data
2. inspect seeded listings and note any expired or stale examples
3. compare:
   - default `GET /leads`
   - expanded `GET /leads?include_hidden=true&include_unqualified=true`
4. confirm expired patterns are present in hidden seed data
5. confirm default results exclude:
   - expired
   - suspected expired
   - stale
   - underqualified
   - overqualified

## Required Evidence

- command output or API response snippet showing hidden stale rows do not appear by default

