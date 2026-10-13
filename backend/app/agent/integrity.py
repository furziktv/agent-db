"""Isolated committed write/read probes, without storing payloads in telemetry."""
import uuid
from app.agent.generator import payload

PROBE_TABLE = 'workload_agent_integrity'
PROBE_DDL = f'''CREATE TABLE IF NOT EXISTS `{PROBE_TABLE}` (
    probe_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    payload LONGTEXT NOT NULL,
    revision INT UNSIGNED NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB'''

async def probe(run, read, counters: dict, workload, rng, limiter, first_payload: str) -> None:
    probe_id = uuid.uuid4().hex
    counters.update(integrity_checks=0, integrity_lost_writes=0,
                    integrity_missing=0, integrity_mismatches=0, integrity_details=[])
    inserted = await run(f'INSERT INTO `{PROBE_TABLE}` (probe_id,payload,revision) VALUES (%s,%s,1)',
              (probe_id, first_payload))
    counters['rows_inserted'] += inserted
    counters['bytes_generated'] += len(first_payload.encode())

    async def verify(expected: str, revision: int, stage: str) -> None:
        await limiter.acquire()
        result = await read(f'SELECT payload,revision FROM `{PROBE_TABLE}` WHERE probe_id=%s', (probe_id,))
        counters['integrity_checks'] += 1
        if not result:
            reason = 'missing'
        else:
            actual, actual_revision = result[0]
            if isinstance(actual, bytes):
                actual = actual.decode('utf-8')
            if actual == expected and actual_revision == revision:
                return
            reason = 'mismatch'
        counters['integrity_lost_writes'] += 1
        counters['integrity_'+('missing' if reason == 'missing' else 'mismatches')] += 1
        counters['integrity_details'].append(dict(probe_id=probe_id, stage=stage, reason=reason))

    await verify(first_payload, 1, 'after_insert')
    updated = payload(workload, rng)
    await limiter.acquire()
    changed = await run(f'UPDATE `{PROBE_TABLE}` SET payload=%s,revision=2 WHERE probe_id=%s LIMIT 1', (updated, probe_id))
    counters['rows_updated'] += changed
    counters['bytes_generated'] += len(updated.encode())
    await verify(updated, 2, 'after_update')
