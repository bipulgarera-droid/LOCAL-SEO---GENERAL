import psycopg2

try:
    conn = psycopg2.connect("postgresql://postgres:postgres@localhost:54322/postgres")
    cur = conn.cursor()
    
    with open('migration_photoshoots.sql', 'r') as f:
        sql = f.read()
        
    cur.execute(sql)
    conn.commit()
    print("Migration successful!")
    
    cur.close()
    conn.close()
except Exception as e:
    print(f"Migration failed: {e}")
