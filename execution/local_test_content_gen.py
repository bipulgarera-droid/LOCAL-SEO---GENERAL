import os
import time
import json
import requests
from supabase import create_client
from dotenv import load_dotenv

# Load env vars
load_dotenv()
load_dotenv('.env.local')

# Setup Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Supabase credentials missing in .env")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def generate_content_via_rest(prompt, api_key, model="gemini-2.5-pro"):
    print(f"Generating with {model}...")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code != 200:
        print(f"API Error: {response.text}")
        return None
        
    try:
        return response.json()['candidates'][0]['content']['parts'][0]['text']
    except Exception as e:
        print(f"Parse Error: {e}")
        return None

def test_run():
    print("--- Verifying Prod Data ---")
    project_id = "3d258982-b6ca-401c-b3b5-06f0c2444cad"
    
    # Fetch the most recently updated page
    res = supabase.table('pages').select('*').eq('project_id', project_id).order('updated_at', desc=True).limit(1).execute()
    
    if not res.data:
        print("No pages found.")
        return

    page = res.data[0]
    print(f"Latest Page: {page.get('url')}")
    print(f"Updated At: {page.get('updated_at')}")
    print(f"Status: {page.get('status')}")
    
    content_len = len(page.get('content') or '')
    body_len = len(page.get('tech_audit_data', {}).get('body_content') or '')
    
    print(f"Content Column Len: {content_len}")
    print(f"Body Content Len: {body_len}")
    
    if content_len > 0 or body_len > 0:
        print("CONCLUSION: Data IS in the DB. Frontend is not showing it.")
    else:
        print("CONCLUSION: Data is NOT in the DB. Save failed.")

if __name__ == "__main__":
    test_run()
