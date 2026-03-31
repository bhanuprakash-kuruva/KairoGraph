
"""
Verify database connection and sample data
"""

from sqlalchemy import create_engine, text
import config

def verify():
    engine = create_engine(config.DATABASE_URL)
    
    with engine.connect() as conn:
        # Check if tables exist
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """))
        
        tables = [row[0] for row in result]
        print(f"📋 Tables found: {tables}")
        
        # Check for our specific tables
        expected_tables = ['users', 'orders', 'products', 'order_items']
        for table in expected_tables:
            if table in tables:
                # Count rows
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"   ✅ {table}: {count} rows")
            else:
                print(f"   ❌ {table}: Not found")
        
        # Check functions
        result = conn.execute(text("""
            SELECT proname, prokind
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = 'public'
            AND proname IN ('calculate_discount', 'log_order')
        """))
        
        functions = list(result)
        if functions:
            print(f"\n📦 Functions/Procedures found:")
            for func in functions:
                print(f"   ✅ {func[0]} (type: {func[1]})")
        else:
            print("\n⚠️  No functions/procedures found")

if __name__ == "__main__":
    verify()
