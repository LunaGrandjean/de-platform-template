-- Read-only inventory. Keep the live slot; never drop another team's slot.
SELECT slot_name, plugin, active, wal_status, restart_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_wal_bytes
FROM pg_replication_slots ORDER BY slot_name;

-- Once the operator has stopped the corresponding connector and verified
-- ownership, drop ONLY its confirmed unused slot, for example:
-- SELECT pg_drop_replication_slot('marketplace_bronze_slot');
-- Do not execute this while the group's CDC connection still needs the slot.
