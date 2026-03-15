"""Check the Strategy.config column directly from DB."""
import asyncio, json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.path.insert(0, r"C:\homerun\.claude\worktrees\charming-rosalind\backend")

async def main():
    from models.database import init_database, AsyncSessionLocal
    from sqlalchemy import select, text
    await init_database()

    async with AsyncSessionLocal() as session:
        rows = (await session.execute(
            text("SELECT slug, config FROM strategies WHERE slug = 'tail_end_carry'")
        )).all()
        for row in rows:
            slug, config = row
            print(f"=== Strategy: {slug} ===")
            if isinstance(config, dict):
                for k, v in sorted(config.items()):
                    print(f"  {k}: {v}")
            elif config:
                parsed = json.loads(config) if isinstance(config, str) else config
                if isinstance(parsed, dict):
                    for k, v in sorted(parsed.items()):
                        print(f"  {k}: {v}")
                else:
                    print(f"  raw: {config}")
            else:
                print("  config: NULL/empty")

asyncio.run(main())
