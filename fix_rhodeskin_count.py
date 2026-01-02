"""
Manually fix rhodeskin project's page_count field
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

print("Fixing rhodeskin project's page_count...")
print("=" * 60)

# Get rhodeskin project
project = supabase.table('projects').select('*').ilike('domain', '%rhodeskin%').single().execute()

if project.data:
    project_id = project.data['id']
    
    # Count actual pages
    actual_count = supabase.table('pages').select('*', count='exact').eq('project_id', project_id).execute()
    
    print(f"Project: {project.data['domain']}")
    print(f"Current page_count: {project.data.get('page_count', 0)}")
    print(f"Actual pages in DB: {actual_count.count}")
    
    # Update page_count
    supabase.table('projects').update({'page_count': actual_count.count}).eq('id', project_id).execute()
    
    print(f"\n✅ Updated page_count to {actual_count.count}")
    print("The project should now show 'Done (253)' in agency.html!")
else:
    print("❌ Project not found!")
