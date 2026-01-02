import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

res = supabase.table('projects').select('id').limit(1).execute()
if res.data:
    print(res.data[0]['id'])
else:
    print("No projects found")
