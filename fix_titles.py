import os
from dotenv import load_dotenv
from supabase import create_client, Client
from urllib.parse import urlparse

# Load env
load_dotenv('.env.local')
load_dotenv()

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    print("Error: Supabase credentials not found.")
    exit(1)

supabase: Client = create_client(url, key)

def get_title_from_url(url):
    try:
        path = urlparse(url).path
        # Get last non-empty segment
        segments = [s for s in path.split('/') if s]
        if not segments: return "Home"
        slug = segments[-1]
        # Convert slug to title (e.g., "my-page-title" -> "My Page Title")
        return slug.replace('-', ' ').replace('_', ' ').title()
    except:
        return "Untitled Page"

def fix_titles():
    print("Fetching pages...")
    # Fetch all pages (limit to 1000 for safety, though pagination is better for huge sets)
    res = supabase.table('pages').select('*').execute()
    pages = res.data
    
    print(f"Found {len(pages)} pages. Checking for bad titles...")
    
    count = 0
    for page in pages:
        tech_data = page.get('tech_audit_data') or {}
        current_title = tech_data.get('title', '')
        
        # Check for bad title
        is_bad_title = not current_title or 'pending' in current_title.lower() or 'untitled' in current_title.lower() or 'scan' in current_title.lower()
        
        if is_bad_title:
            new_title = get_title_from_url(page['url'])
            print(f"Fixing: {page['url']}")
            print(f"  Old: '{current_title}'")
            print(f"  New: '{new_title}'")
            
            # Update DB
            tech_data['title'] = new_title
            supabase.table('pages').update({
                'tech_audit_data': tech_data
            }).eq('id', page['id']).execute()
            count += 1
            
    print(f"Done! Fixed {count} pages.")

if __name__ == "__main__":
    fix_titles()
