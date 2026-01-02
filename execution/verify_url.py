import requests
import sys
import json
import argparse
from urllib.parse import urlparse

def is_soft_404(content):
    """
    Checks for common 'Not Found' phrases in the content.
    """
    content = content.lower()
    
    # Remove Jina metadata (URL Source, Title) to prevent matching the URL itself
    lines = content.split('\n')
    filtered_lines = [line for line in lines if not line.startswith('url source:') and not line.startswith('title:')]
    content = '\n'.join(filtered_lines)

    error_phrases = [
        "page not found",
        "provider not found",
        "we couldn't find",
        "we could not find",
        "no results for",
        "doesn't exist",
        "does not exist",
        "error 404",
        "404 error",
        "page unavailable",
        "profile not found",
        "find a doctor - doctor reviews" # Generic Healthgrades home
    ]
    
    for phrase in error_phrases:
        if phrase in content:
            return True
    return False

def is_search_result_page(content, title=""):
    """
    Detects if the page is likely a search result or listing page, not a specific profile.
    """
    content = content.lower()
    title = title.lower()
    
    search_indicators = [
        "search results for",
        "results for",
        "find a doctor",
        "best doctors in",
        "top rated",
        "list of",
        "directory of",
        "matching results",
        "providers found"
    ]
    
    # Strong signal: Title starts with "Search" or "Find"
    if title.startswith("search") or title.startswith("find") or "search results" in title:
        return True

    # Content check
    # We need to be careful not to flag "Back to Search Results" links
    # So we look for these phrases in headings or main text, which is hard with raw text.
    # But if "Search results for" appears, it's usually a bad sign for a profile page.
    for indicator in search_indicators:
        if indicator in content[:1000]: # Check top of page
            return True
            
    return False

def check_text(content, required_text, name_parts):
    """
    Checks if required_text is in content (fuzzy match).
    Also checks for Soft 404s and Search Result pages.
    """
    if is_soft_404(content):
        return False
        
    if is_search_result_page(content):
        return False
        
    content = content.lower()
    clean_req = required_text.lower()
    
    if clean_req in content:
        return True
    
    # Check if all significant parts of the name are present
    if all(part in content for part in name_parts if len(part) > 2):
        return True
        
    return False

def verify_url(url, required_text):
    """
    Verifies a URL using Jina, Requests, and Slug matching.
    """
    if not url or not required_text:
        return {"verified": False, "reason": "Missing input"}

    # Normalize required text
    clean_req = required_text.lower().replace('dr.', '').replace('dr ', '').strip()
    name_parts = clean_req.split()
    
    log = []

    # 0. Pre-check Status Code (Crucial for 404s that Jina misses)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        # Use stream=True to avoid downloading large files, just check headers
        pre_resp = requests.get(url, headers=headers, timeout=10, stream=True)
        
        if pre_resp.status_code in [404, 410]:
            log.append(f"Pre-check: Status {pre_resp.status_code} (Dead Link)")
            return {"verified": False, "reason": "Dead Link (404/410)", "log": log}
        
        pre_resp.close() # Close connection
    except Exception as e:
        log.append(f"Pre-check Warning: {str(e)}")
        # Continue to Jina if pre-check fails (might be blocked, but Jina could work)

    # 1. Try Jina Reader
    jina_success_but_failed_verify = False
    try:
        jina_url = f"https://r.jina.ai/{url}"
        jina_response = requests.get(jina_url, timeout=10, headers={"Accept": "text/markdown"})
        
        if jina_response.status_code == 200:
            if check_text(jina_response.text, clean_req, name_parts):
                return {"verified": True, "method": "jina", "log": log}
            else:
                log.append(f"Jina: Text not found (Content fetched)")
                jina_success_but_failed_verify = True
        else:
            log.append(f"Jina: Status {jina_response.status_code}")
    except Exception as e:
        log.append(f"Jina Error: {str(e)}")

    # If Jina fetched content but verification failed, we TRUST it and reject.
    # Unless it was a Captcha/Block that Jina didn't catch? 
    # For now, let's be strict. If Jina sees the page and says "Nope", we believe it.
    if jina_success_but_failed_verify:
         return {"verified": False, "reason": "Text not found (Jina)", "log": log}

    # 2. Fallback to Requests/BS4
    request_failed_but_not_404 = False
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=15)
        
        if resp.status_code == 200:
            if check_text(resp.text, clean_req, name_parts):
                return {"verified": True, "method": "requests", "log": log}
            else:
                log.append(f"Requests: Text not found")
                # If requests worked but text missing, it MIGHT be JS rendered.
                # So we CAN fallback to slug match here, unlike Jina which renders JS/Markdown.
                # But if Jina also failed... then it's likely not there.
                request_failed_but_not_404 = True 
        elif resp.status_code in [404, 410]:
            log.append(f"Requests: Status {resp.status_code} (Dead Link)")
            return {"verified": False, "reason": "Dead Link (404/410)", "log": log}
        else:
            log.append(f"Requests: Status {resp.status_code}")
            request_failed_but_not_404 = True # Blocked or Error
            
    except Exception as e:
        log.append(f"Requests Error: {str(e)}")
        request_failed_but_not_404 = True

    # 3. Last Resort: Slug Match
    # Only if Requests failed (Blocked/JS) AND Jina failed (Error/Status).
    if request_failed_but_not_404:
        try:
            slug = url.lower().split('?')[0]
            
            # Full name match
            if clean_req.replace(' ', '-') in slug or clean_req.replace(' ', '') in slug:
                return {"verified": True, "method": "slug_match", "log": log}
            
            # Parts match
            if all(part in slug for part in name_parts if len(part) > 2):
                return {"verified": True, "method": "slug_parts_match", "log": log}
                
            log.append("Slug: No match")
        except Exception as e:
            log.append(f"Slug Error: {str(e)}")

    return {"verified": False, "reason": "Verification failed", "log": log}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--text", required=True)
    args = parser.parse_args()
    
    result = verify_url(args.url, args.text)
    print(json.dumps(result, indent=2))
