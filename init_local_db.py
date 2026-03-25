import asyncio
import asyncpg
import sys

async def init_db():
    # Try common local connection strings
    dsns = [
        "postgresql://postgres@localhost:5432/postgres",
        "postgresql://postgres:postgres@localhost:5432/postgres",
        "postgresql://localhost:5432/postgres"
    ]
    
    conn = None
    for dsn in dsns:
        try:
            conn = await asyncpg.connect(dsn)
            print(f"Connected using {dsn}")
            break
        except Exception as e:
            print(f"Failed with {dsn}: {e}")
            
    if not conn:
        print("Could not connect to PostgreSQL.")
        sys.exit(1)
        
    try:
        await conn.execute("CREATE DATABASE eventledger")
        print("Database 'eventledger' created.")
    except asyncpg.DuplicateDatabaseError:
        print("Database 'eventledger' already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        await conn.close()

    # Now connect to eventledger and run schema
    try:
        conn = await asyncpg.connect("postgresql://postgres:postgres@localhost:5432/eventledger")
    except:
        try:
            conn = await asyncpg.connect("postgresql://postgres@localhost:5432/eventledger")
        except Exception as e:
            print(f"Could not connect to 'eventledger': {e}")
            sys.exit(1)
            
    try:
        with open("src/schema.sql", "r") as f:
            schema = f.read()
            await conn.execute(schema)
            print("Schema initialized.")
    except Exception as e:
        print(f"Error initializing schema: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(init_db())
