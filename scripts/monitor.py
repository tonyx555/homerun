"""20-minute live monitoring script for homerun workers."""
import urllib.request, json, time, re, sys

start = time.time()
totals = {'errors':0,'pool_exhaust':0,'conn_hold':0,'sla':0,'skips':0,'crashes':0,'ws':0,'telegram':0,'fak':0}

print(f"=== MONITORING STARTED {time.strftime('%H:%M:%S')} ===")
print("Watching for 20 minutes...")
print(flush=True)

while time.time() - start < 1200:
    elapsed = int(time.time() - start)
    mins, secs = divmod(elapsed, 60)
    try:
        resp = urllib.request.urlopen('http://localhost:8000/api/worker-logs?lines=200&since_seconds=16', timeout=10)
        lines = resp.read().decode()
    except Exception:
        time.sleep(15)
        continue

    counts = {
        'errors': len(re.findall(r'\[ERROR\]', lines)),
        'pool_exhaust': len(re.findall(r'Pool near exhaustion|POOL RECOVERY', lines)),
        'conn_hold': len(re.findall(r'Connection held for', lines)),
        'sla': len(re.findall(r'Execution latency SLA breached', lines)),
        'skips': len(re.findall(r'Trader cycle skipped', lines)),
        'crashes': len(re.findall(r'Worker heartbeat stale|Worker task exited unexpectedly', lines)),
        'ws': len(re.findall(r'WS heartbeat pong timeout|WS connection lost|WS disconnected', lines)),
        'telegram': len(re.findall(r'Telegram settings reload timed out', lines)),
        'fak': len(re.findall(r'no orders found to match', lines)),
    }

    for k in totals:
        totals[k] += counts[k]

    pool_str = 'N/A'
    try:
        pr = urllib.request.urlopen('http://localhost:8000/api/pool-status', timeout=5)
        pd = json.loads(pr.read())
        pool_str = f"{pd.get('checked_out','?')}/{pd.get('total_capacity','?')}"
    except Exception:
        pass

    has_issues = any(counts[k] > 0 for k in ['errors','pool_exhaust','crashes','conn_hold'])
    if has_issues:
        print(f"[{mins}m{secs:02d}s] pool={pool_str} | err={counts['errors']} pool={counts['pool_exhaust']} crash={counts['crashes']} conn={counts['conn_hold']} sla={counts['sla']} skip={counts['skips']} ws={counts['ws']} tg={counts['telegram']}", flush=True)

    time.sleep(15)

print(flush=True)
print(f"=== 20 MINUTE MONITORING COMPLETE {time.strftime('%H:%M:%S')} ===")
for k, v in totals.items():
    print(f"  {k}: {v}")
verdict = 'ALL CLEAN' if totals['pool_exhaust'] == 0 and totals['crashes'] == 0 and totals['conn_hold'] == 0 else 'ISSUES DETECTED'
print(f"VERDICT: {verdict}")
