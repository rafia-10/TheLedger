import asyncio
import asyncpg
import sys

async def run():
    dsn = "postgresql://postgres:supabase1224@db.dxscaeckamkplshxqkae.supabase.co:5432/postgres?sslmode=require"
    try:
        conn = await asyncpg.connect(dsn)
        print("SUCCESS: Connected to Supabase!")
        await conn.close()
    except Exception as e:
        print(f"FAILURE: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run())
