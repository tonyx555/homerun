# Workers: separate processes that use DB as shared state.
# Run from backend/ with:
#   python -m workers.scanner_worker
#   python -m workers.news_worker
#   python -m workers.weather_worker
#   python -m workers.tracked_traders_worker
#   python -m workers.trader_orchestrator_worker
#   python -m workers.trader_reconciliation_worker
#   python -m workers.events_worker
