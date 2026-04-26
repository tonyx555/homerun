"""
V2 balance diagnostic.

Run from the backend dir with:
    venv\Scripts\python.exe diagnose_v2_balance.py

Reports which SDK is loaded, which CLOB URL is configured, and pings
/balance-allowance against BOTH V1 and V2 endpoints with your stored
credentials so we can see exactly where your funds are.
"""
import asyncio


def fmt_amt(raw: object) -> str:
    if raw is None:
        return "None"
    try:
        v = int(str(raw))
        return f"{v / 1_000_000:.4f} ({v} raw)"
    except Exception:
        return str(raw)


async def main() -> None:
    print("=" * 70)
    print("Polymarket V2 balance diagnostic")
    print("=" * 70)

    # Step 1: which SDK is installed
    try:
        import py_clob_client_v2
        print(f"[OK] py_clob_client_v2 loaded from: {py_clob_client_v2.__file__}")
    except ImportError as exc:
        print(f"[FAIL] py_clob_client_v2 NOT installed: {exc}")
        print("       Run: venv\\Scripts\\python.exe -m pip install -r requirements-trading.txt")
        return

    try:
        import py_clob_client
        print(f"[INFO] Old py_clob_client also installed at: {py_clob_client.__file__}")
        print("       (harmless but consider: pip uninstall -y py-clob-client)")
    except ImportError:
        print("[OK]   Old py_clob_client is gone (clean V2-only environment)")

    # Step 2: load runtime settings
    from config import settings
    print()
    print(f"CLOB_API_URL configured = {settings.CLOB_API_URL}")
    print(f"CHAIN_ID                = {settings.CHAIN_ID}")
    print(f"POLYMARKET_SIGNATURE_TYPE = {settings.POLYMARKET_SIGNATURE_TYPE}")
    print(f"POLYMARKET_FUNDER         = {settings.POLYMARKET_FUNDER}")
    print(f"POLYMARKET_PRIVATE_KEY    = {'<set>' if settings.POLYMARKET_PRIVATE_KEY else '<MISSING>'}")
    print(f"POLYMARKET_API_KEY        = {'<set>' if settings.POLYMARKET_API_KEY else '<MISSING>'}")
    print(f"POLYMARKET_BUILDER_CODE   = {settings.POLYMARKET_BUILDER_CODE or '<unset>'}")

    # Step 3: load DB credentials if env is empty (matches live_execution_service path)
    private_key = settings.POLYMARKET_PRIVATE_KEY
    api_key = settings.POLYMARKET_API_KEY
    api_secret = settings.POLYMARKET_API_SECRET
    api_passphrase = settings.POLYMARKET_API_PASSPHRASE

    if not all([private_key, api_key, api_secret, api_passphrase]):
        print("\n[INFO] env credentials incomplete; loading from DB stored_credentials...")
        try:
            from sqlalchemy import select
            from models.database import AsyncSessionLocal, AppSettings
            from utils.secrets import decrypt_secret
            async with AsyncSessionLocal() as session:
                result = await session.execute(select(AppSettings).where(AppSettings.id == "default"))
                row = result.scalar_one_or_none()
                if row:
                    private_key = private_key or decrypt_secret(getattr(row, "polymarket_private_key", None))
                    api_key = api_key or decrypt_secret(getattr(row, "polymarket_api_key", None))
                    api_secret = api_secret or decrypt_secret(getattr(row, "polymarket_api_secret", None))
                    api_passphrase = api_passphrase or decrypt_secret(getattr(row, "polymarket_api_passphrase", None))
                    print("[OK]   Loaded credentials from DB")
        except Exception as exc:
            print(f"[WARN] DB credential load failed: {exc}")

    if not all([private_key, api_key, api_secret, api_passphrase]):
        print("\n[FAIL] Missing credentials. Cannot proceed.")
        return

    from eth_account import Account
    eoa = Account.from_key(private_key).address
    print(f"\nEOA address  = {eoa}")
    print(f"Funder/proxy = {settings.POLYMARKET_FUNDER or '(will be derived)'}")

    # Step 4: hit both endpoints
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, AssetType, BalanceAllowanceParams

    creds = ApiCreds(
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
    )

    sig_type = int(settings.POLYMARKET_SIGNATURE_TYPE or 1)
    funder = settings.POLYMARKET_FUNDER

    for url_label, url in [("V2", "https://clob-v2.polymarket.com"),
                           ("V1 (legacy)", "https://clob.polymarket.com")]:
        print()
        print("-" * 70)
        print(f"Querying {url_label}: {url}")
        print("-" * 70)
        try:
            client = ClobClient(
                host=url,
                key=private_key,
                chain_id=settings.CHAIN_ID,
                creds=creds,
                signature_type=sig_type,
                funder=funder,
            )

            for st in (0, 1, 2):
                try:
                    params = BalanceAllowanceParams(
                        asset_type=AssetType.COLLATERAL,
                        signature_type=st,
                    )
                    payload = await asyncio.to_thread(client.get_balance_allowance, params)
                    if isinstance(payload, dict):
                        bal = payload.get("balance")
                        allow = payload.get("allowance")
                        print(f"  sig_type={st}: balance={fmt_amt(bal)}  allowance={fmt_amt(allow)}")
                        if "allowances" in payload:
                            print(f"               allowances={payload.get('allowances')}")
                    else:
                        print(f"  sig_type={st}: unexpected response: {payload!r}")
                except Exception as exc:
                    print(f"  sig_type={st}: ERROR -> {type(exc).__name__}: {exc}")
        except Exception as exc:
            print(f"  client construction failed: {type(exc).__name__}: {exc}")

    # Step 5: get_open_orders against V2
    print()
    print("-" * 70)
    print("V2 open orders (should be 0 if you haven't placed any V2 orders yet)")
    print("-" * 70)
    try:
        v2 = ClobClient(
            host="https://clob-v2.polymarket.com",
            key=private_key,
            chain_id=settings.CHAIN_ID,
            creds=creds,
            signature_type=sig_type,
            funder=funder,
        )
        orders = await asyncio.to_thread(v2.get_open_orders)
        if isinstance(orders, list):
            print(f"  V2 open orders: {len(orders)} returned")
        else:
            print(f"  V2 open orders raw: {orders!r}")
    except Exception as exc:
        print(f"  V2 get_open_orders ERROR: {type(exc).__name__}: {exc}")

    print()
    print("=" * 70)
    print("Interpretation:")
    print("  - If V2 balance is 0 across all sig_types but V1 shows your cash,")
    print("    pUSD wrap hasn't happened yet. Log into polymarket.com.")
    print("  - If both V1 and V2 show 0, your cash is somewhere else (check funder)")
    print("  - If V2 is non-zero, the homerun reading is wrong (cache?)")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
