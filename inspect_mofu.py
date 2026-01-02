import os
from dotenv import load_dotenv
from supabase import create_client
import json

load_dotenv('.env.local')
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

supabase = create_client(url, key)

# Get ONE MoFu page to inspect its full structure
res = supabase.table('pages').select('*').eq('funnel_stage', 'MoFu').limit(1).execute()

if res.data:
    page = res.data[0]
    print("Single MoFu Page Structure:")
    print(json.dumps(page, indent=2, default=str))
else:
    print("No MoFu pages found")
