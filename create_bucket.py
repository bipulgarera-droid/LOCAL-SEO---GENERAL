import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Supabase credentials missing")
    exit(1)

supabase: Client = create_client(url, key)

try:
    # Try to create the bucket
    res = supabase.storage.create_bucket('photoshoots', options={'public': True})
    print("Bucket created:", res)
except Exception as e:
    print(f"Error creating bucket (might already exist): {e}")
