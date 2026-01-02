import os
import json
import requests
import argparse
from dotenv import load_dotenv

try:
    from .verify_url import verify_url
except ImportError:
    from verify_url import verify_url

# Load environment variables
load_dotenv()

def find_urls(directories, business_name, doctor_name, city):
    """
    Uses Perplexity to find profile URLs for the given directories.
    """
    perplexity_key = os.environ.get('PERPLEXITY_API_KEY')
    if not perplexity_key:
        return {"error": "PERPLEXITY_API_KEY not found"}

    # Prepare batch for Perplexity
    dir_list = []
    for d in directories:
        name = d.get('name')
        if "google" in name.lower() and ("business" in name.lower() or "maps" in name.lower()):
            dir_list.append(f"- {name} (SEARCH FOR GOOGLE MAPS URL)")
        else:
            dir_list.append(f"- {name}")
            
    dir_list_str = "\n".join(dir_list)
    
    clean_name = business_name.strip()
    clean_doctor = doctor_name.strip() if doctor_name else ''
    
    prompt = f"""
For each directory below, search Google and find the DIRECT profile page URL.

**Business:** {business_name}
**Doctor:** {doctor_name if doctor_name else 'N/A'}
**City:** {city}

**Directories:**
{dir_list_str}

**HOW TO SEARCH:**
For each directory, you MUST perform a targeted Google search to find the specific profile page.
Recommended Search Queries:
1. `site:DIRECTORY_DOMAIN {clean_doctor} {clean_name}` (e.g., `site:healthgrades.com Dr. Andrew Jacono New York Center`)
2. `{clean_doctor} {name} profile`

**CRITICAL RULES:**
1. **Analyze Search Results:** Look at the snippets. Pick the one that is clearly the **specific profile page** for this doctor/business.
2. **Verify the Name:** The page title MUST contain "{clean_doctor}" or "{clean_name}".
3. **Google Maps:** Return the long URL (google.com/maps/place/...), not the short `cid` link.
4. **If not found:** If the search results only show generic lists or "search results for...", return "NOT FOUND".

**OUTPUT JSON:**
{{
    "directories": [
        {{"name": "Directory Name", "homepage_url": "FOUND_PROFILE_URL_OR_NOT_FOUND"}}
    ]
}}
"""

    payload = {
        "model": "sonar-pro",
        "messages": [
            {
                "role": "system",
                "content": "You are a precise research assistant. You MUST return ONLY valid JSON. Do not include markdown formatting like ```json or ```."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1
    }
    
    headers = {
        "Authorization": f"Bearer {perplexity_key}",
        "Content-Type": "application/json"
    }
    
    print(f"DEBUG: Calling Perplexity...", flush=True)
    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            json=payload,
            headers=headers,
            timeout=120
        )
        
        if response.status_code != 200:
            return {"error": f"Perplexity API failed: {response.text}"}
            
        result = response.json()
        content = result['choices'][0]['message']['content']
        
        # Parse Response
        clean = content.strip()
        if clean.startswith('```json'): clean = clean[7:]
        if clean.startswith('```'): clean = clean[3:]
        if clean.endswith('```'): clean = clean[:-3]
        
        data = json.loads(clean.strip())
        
        # Verify URLs
        verified_results = []
        required_text = doctor_name if doctor_name else business_name
        
        for item in data.get('directories', []):
            url = item.get('homepage_url', '')
            name = item.get('name', '')
            
            status = "not_found"
            final_url = ""
            verification_log = []
            
            if url and url.lower() != "not found" and url.startswith("http"):
                print(f"DEBUG: Verifying {url} for {name}...", flush=True)
                v_result = verify_url(url, required_text)
                verification_log = v_result.get('log', [])
                
                if v_result['verified']:
                    status = "found"
                    final_url = url
                    print(f"DEBUG: VERIFIED {url}", flush=True)
                else:
                    print(f"DEBUG: REJECTED {url}: {v_result.get('reason')}", flush=True)
            
            verified_results.append({
                "name": name,
                "url": final_url,
                "status": status,
                "log": verification_log
            })
            
        return {"results": verified_results}

    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--directories", required=True, help="Path to JSON file with directories list")
    parser.add_argument("--business", required=True)
    parser.add_argument("--doctor", default="")
    parser.add_argument("--city", required=True)
    args = parser.parse_args()
    
    with open(args.directories, 'r') as f:
        dirs = json.load(f)
        
    result = find_urls(dirs, args.business, args.doctor, args.city)
    print(json.dumps(result, indent=2))
