import psycopg
import sys

def run():
    dsn = "postgresql://postgres:supabase1224@db.dxscaeckamkplshxqkae.supabase.co:5432/postgres?sslmode=require"
    try:
        conn = psycopg.connect(dsn, connect_timeout=10)
        print("SUCCESS: Psycopg connected to Supabase!")
        conn.close()
    except Exception as e:
        print(f"FAILURE: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()
