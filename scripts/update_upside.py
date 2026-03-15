"""Update min_upside_percent from 10 to 5 in DB."""
import asyncio, json, sys
sys.path.insert(0, r"C:\homerun\.claude\worktrees\charming-rosalind\backend")

async def main():
    from models.database import init_database, AsyncSessionLocal
    from sqlalchemy import text
    await init_database()

    async with AsyncSessionLocal() as session:
        # Update Strategy.config
        row = (await session.execute(
            text("SELECT slug, config FROM strategies WHERE slug = 'tail_end_carry'")
        )).first()
        if row:
            config = row[1] if isinstance(row[1], dict) else json.loads(row[1]) if isinstance(row[1], str) else {}
            if config.get("min_upside_percent") != 5.0:
                config["min_upside_percent"] = 5.0
                await session.execute(
                    text("UPDATE strategies SET config = :cfg WHERE slug = 'tail_end_carry'"),
                    {"cfg": json.dumps(config)}
                )
                print(f"Updated Strategy.config min_upside_percent to 5.0")
            else:
                print("Strategy.config already at 5.0")

        # Update trader source_configs_json
        rows = (await session.execute(
            text("SELECT id, name, source_configs_json FROM traders WHERE source_configs_json::text ILIKE '%tail_end_carry%'")
        )).all()
        for tid, name, configs in rows:
            configs_list = configs if isinstance(configs, list) else json.loads(configs) if isinstance(configs, str) else []
            changed = False
            for cfg in configs_list:
                if not isinstance(cfg, dict):
                    continue
                if cfg.get("strategy_key") != "tail_end_carry":
                    continue
                params = cfg.get("strategy_params", {})
                if params.get("min_upside_percent") != 5:
                    params["min_upside_percent"] = 5
                    changed = True
            if changed:
                await session.execute(
                    text("UPDATE traders SET source_configs_json = :cfg WHERE id = :tid"),
                    {"cfg": json.dumps(configs_list), "tid": tid}
                )
                print(f"Updated trader '{name}' min_upside_percent to 5")

        await session.commit()
        print("Done")

asyncio.run(main())
