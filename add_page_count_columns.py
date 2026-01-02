"""
Add page_count and classified_count columns to projects table
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

print("Adding page_count column to projects table...")
print("=" * 60)

# Get all projects and update their page_count
projects = supabase.table('projects').select('*').execute()

for project in projects.data:
    project_id = project['id']
    
    # Count pages for this project
    page_count_res = supabase.table('pages').select('*', count='exact').eq('project_id', project_id).execute()
    page_count = page_count_res.count
    
    # Count classified pages
    classified_res = supabase.table('pages').select('*', count='exact').eq('project_id', project_id).not_.is_('page_type', 'null').execute()
    classified_count = classified_res.count
    
    print(f"\nProject: {project.get('domain', 'Unknown')}")
    print(f"  Total pages: {page_count}")
    print(f"  Classified pages: {classified_count}")
    
    # For now, just print what we would update
    # The column needs to be added via Supabase dashboard first
    print(f"  Would update: page_count={page_count}, classified_count={classified_count}")

print("\n" + "=" * 60)
print("⚠️  IMPORTANT: You need to add these columns via Supabase Dashboard first:")
print("   1. Go to Supabase Dashboard > Table Editor > projects")
print("   2. Add column: page_count (type: int4, default: 0)")
print("   3. Add column: classified_count (type: int4, default: 0)")
print("   4. Then run this script again to populate the values")
