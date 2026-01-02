"""
Get a Category page URL for debugging
"""
import os
from supabase import create_client

# Manual .env parsing
for env_file in ['.env', '.env.local']:
    env_path = f'/Users/bipul/Downloads/seo-saas-brain/{env_file}'
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, val = line.strip().split('=', 1)
                    val = val.strip('"').strip("'")
                    os.environ[key] = val

supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))

# Try to find a category page
print("Searching for Category pages...")
pages = supabase.table('pages').select('*').eq('page_type', 'Category').limit(5).execute()

if pages.data:
    print(f"Found {len(pages.data)} Category pages:")
    for p in pages.data:
        print(f"- {p['url']} (Project: {p['project_id']})")
else:
    print("No pages explicitly classified as 'Category' found.")
    # Try to find one that looks like a category from rhodeskin
    print("\nSearching for potential category pages in rhodeskin...")
    rhode = supabase.table('projects').select('id').ilike('domain', '%rhodeskin%').single().execute()
    if rhode.data:
        pid = rhode.data['id']
        # Look for 'collections' in URL which is common for Shopify
        candidates = supabase.table('pages').select('*').eq('project_id', pid).ilike('url', '%collections%').limit(5).execute()
        for p in candidates.data:
             print(f"- {p['url']} (Type: {p.get('page_type')})")
