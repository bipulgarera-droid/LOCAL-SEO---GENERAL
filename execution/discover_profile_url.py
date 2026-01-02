"""
discover_profile_url.py

Step 2: Find profile URLs for a business on a directory.

Simple approach:
1. Search Google CSE: site:{directory} {business_name} {city}
2. Get top 5 results
3. Check if title/snippet contains business or doctor name
4. Return the best matching URL (or "not_found")

No page scraping here - NAP extraction happens in Step 3.
"""

import os
import re
import json
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()


def normalize_name(name):
    """Normalize a name for comparison."""
    if not name:
        return ""
    # Remove common prefixes
    name = name.lower()
    name = re.sub(r'^dr\.?\s*', '', name)
    name = re.sub(r'\s+(md|dds|dmd|do|phd|facs)\.?$', '', name)
    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)
    return name.strip()


def name_in_text(name, text):
    """Check if name (or significant parts) appears in text.
    
    For business names, we require strong matching:
    - Full name match, OR
    - First 2-3 significant words appearing together (not scattered)
    """
    if not name or not text:
        return False
    
    name_norm = normalize_name(name)
    text_lower = text.lower()
    
    # Full name match - strongest signal
    if name_norm in text_lower:
        return True
    
    # Get significant parts (>2 chars)
    parts = [p for p in name_norm.split() if len(p) > 2]
    
    if len(parts) >= 2:
        # For multi-word names, require the first 2 words to appear TOGETHER
        # This prevents matching "Northwestern Medicine" when text has
        # "Dr. Smith trained at Northwestern in internal medicine"
        first_two = ' '.join(parts[:2])
        if first_two in text_lower:
            return True
        
        # Also check first 3 words together if available
        if len(parts) >= 3:
            first_three = ' '.join(parts[:3])
            if first_three in text_lower:
                return True
    elif len(parts) == 1:
        # For single-word names, exact match required
        if parts[0] in text_lower.split():
            return True
    
    return False


def search_serper(query, gl="us"):
    """
    Search using Serper.dev (real Google results).
    Returns: List of { url, title, snippet }
    """
    api_key = os.environ.get('SERPER_API_KEY')
    
    if not api_key:
        print("DEBUG: Serper API key not found (SERPER_API_KEY)", flush=True)
        return []
    
    url = "https://google.serper.dev/search"
    
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "q": query,
        "gl": gl,
        "num": 5
    }
    
    try:
        print(f"DEBUG: Serper search: {query}", flush=True)
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if response.status_code != 200:
            error_text = response.text[:200] if response.text else "No error details"
            print(f"DEBUG: Serper search failed: {response.status_code} {error_text}", flush=True)
            return []
        
        data = response.json()
        
        # Get organic results
        organic = data.get('organic', [])
        if not organic:
            print(f"DEBUG: Serper returned no organic results. Keys: {list(data.keys())}", flush=True)
            return []
        
        results = []
        for item in organic:
            result_url = item.get('link', '')
            
            # Only skip homepage URLs - allow everything else
            # Since Serper search uses exact business name in quotes, all results are relevant
            # Camoufox can scrape any page and NAP verifier will check if business is present
            bad_patterns = [
                # Homepage patterns only
                '/webhp', '/home/', '/index',
            ]
            
            # Skip if URL is just the homepage
            from urllib.parse import urlparse
            parsed = urlparse(result_url)
            if parsed.path in ['', '/', '/index.html', '/index.php', '/home']:
                print(f"DEBUG: Skipping homepage URL: {result_url}", flush=True)
                continue
            
            if any(pattern in result_url.lower() for pattern in bad_patterns):
                print(f"DEBUG: Skipping pure search page: {result_url}", flush=True)
                continue
                
            results.append({
                "url": result_url,
                "title": item.get('title', ''),
                "snippet": item.get('snippet', '')
            })
        
        print(f"DEBUG: Serper returned {len(results)} valid results", flush=True)
        return results
        
    except Exception as e:
        print(f"DEBUG: Serper search error: {e}", flush=True)
        return []


def validate_and_extract_profile(url, business_name, directory_domain):
    """
    BeautifulSoup validation: Visit a URL and check if business exists.
    If it's a search page, try to extract the actual profile link.
    
    Returns:
        - {'valid': True, 'profile_url': '...'} if business found (with profile if extractable)
        - {'valid': False} if business not found on page
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print(f"DEBUG: BeautifulSoup not installed, skipping validation", flush=True)
        return {'valid': True, 'profile_url': url}  # Assume valid if can't check
    
    try:
        print(f"DEBUG: Validating URL with BeautifulSoup: {url}", flush=True)
        # Full realistic browser headers to bypass anti-bot protection
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"DEBUG: Validation failed - status {response.status_code}", flush=True)
            # If blocked, assume it's valid (Google found it, so business likely exists)
            return {'valid': True, 'profile_url': url, 'blocked': True}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text().lower()
        
        # Check if business name appears on page
        name_norm = normalize_name(business_name)
        name_parts = [p for p in name_norm.split() if len(p) > 2]
        
        # Check if at least 2 significant words appear together
        found_on_page = False
        if len(name_parts) >= 2:
            first_two = ' '.join(name_parts[:2])
            if first_two in page_text:
                found_on_page = True
        elif len(name_parts) == 1 and name_parts[0] in page_text:
            found_on_page = True
        
        if not found_on_page:
            print(f"DEBUG: Business name NOT found on page: {business_name}", flush=True)
            return {'valid': False}
        
        print(f"DEBUG: Business name confirmed on page!", flush=True)
        
        # Try to extract a more specific profile link if this looks like a search page
        is_search_page = any(x in url.lower() for x in ['/search', '/find', '/results', '?q=', '?query='])
        
        if is_search_page:
            print(f"DEBUG: This is a search page, looking for profile link...", flush=True)
            # Look for links that contain business name
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                link_text = a.get_text().lower()
                
                # Skip search/category links
                if any(x in href.lower() for x in ['/search', '/category', '?q=', '/find/']):
                    continue
                
                # Check if this link is for our business
                if name_in_text(business_name, link_text) or name_in_text(business_name, href):
                    # Build full URL if relative
                    if href.startswith('/'):
                        profile_url = f"https://{directory_domain}{href}"
                    elif href.startswith('http'):
                        profile_url = href
                    else:
                        profile_url = f"https://{directory_domain}/{href}"
                    
                    print(f"DEBUG: Extracted profile URL: {profile_url}", flush=True)
                    return {'valid': True, 'profile_url': profile_url}
        
        # Business is on page, but couldn't extract a better profile link
        return {'valid': True, 'profile_url': url}
        
    except Exception as e:
        print(f"DEBUG: Validation error: {e}", flush=True)
        return {'valid': True, 'profile_url': url}  # Assume valid on error


def search_directory_directly(directory_domain, business_name, city, country=''):
    """
    BeautifulSoup fallback: Search the directory's website directly.
    When Google doesn't surface the profile, we search on the directory itself.
    
    Returns: { url, title } or None
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("DEBUG: BeautifulSoup not installed, skipping direct search", flush=True)
        return None
    
    # Map directories to their search URL patterns
    DIRECTORY_SEARCH_URLS = {
        'yelp.com': 'https://www.yelp.com/search?find_desc={query}&find_loc={location}',
        'yelp.com.au': 'https://www.yelp.com.au/search?find_desc={query}&find_loc={location}',
        'yellowpages.com.au': 'https://www.yellowpages.com.au/search/listings?clue={query}&locationClue={location}',
        'truelocal.com.au': 'https://www.truelocal.com.au/search/{query}/{location}',
        'hotfrog.com.au': 'https://www.hotfrog.com.au/search/{location}/{query}',
        'localsearch.com.au': 'https://www.localsearch.com.au/find/{query}/{location}',
        'startlocal.com.au': 'https://www.startlocal.com.au/search/?q={query}&loc={location}',
        'yellowpages.com': 'https://www.yellowpages.com/search?search_terms={query}&geo_location_terms={location}',
        # Added Cylex variants
        'cylex-australia.com': 'https://www.cylex-australia.com/search/{query}.html',
        'cylex.com.au': 'https://www.cylex.com.au/search/{query}.html',
        # Product Review
        'productreview.com.au': 'https://www.productreview.com.au/search?q={query}',
        # Hipages (AU trades)
        'hipages.com.au': 'https://hipages.com.au/find/{query}',
    }
    
    # Find matching search URL
    search_url_template = None
    for domain_key, url_template in DIRECTORY_SEARCH_URLS.items():
        if domain_key in directory_domain or directory_domain in domain_key:
            search_url_template = url_template
            break
    
    if not search_url_template:
        print(f"DEBUG: No direct search pattern for {directory_domain}", flush=True)
        return None
    
    # Build location string
    location = f"{city}, {country}".strip(', ') if country else city
    
    # Format the search URL
    search_url = search_url_template.format(
        query=requests.utils.quote(business_name),
        location=requests.utils.quote(location)
    )
    
    print(f"DEBUG: Direct search on {directory_domain}: {search_url}", flush=True)
    
    try:
        # Full realistic browser headers to bypass anti-bot protection
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        }
        response = requests.get(search_url, headers=headers, timeout=15, allow_redirects=True)
        
        if response.status_code != 200:
            print(f"DEBUG: Direct search failed with status {response.status_code}", flush=True)
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for profile links - common patterns across directories
        profile_links = []
        
        # Pattern 1: Links with business name in text or href
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            text = a.get_text().lower()
            
            # Skip search/category links
            if any(x in href.lower() for x in ['/search', '/category', '/browse', '?q=', '/find/']):
                continue
                
            # Check if business name mentioned
            if name_in_text(business_name, text) or name_in_text(business_name, href):
                full_url = href if href.startswith('http') else f"https://{directory_domain}{href}"
                profile_links.append({'url': full_url, 'title': a.get_text().strip()})
        
        if profile_links:
            best = profile_links[0]
            print(f"DEBUG: Direct search found: {best['url']}", flush=True)
            return best
        
        print(f"DEBUG: Direct search found no matching profiles", flush=True)
        return None
        
    except Exception as e:
        print(f"DEBUG: Direct search error: {e}", flush=True)
        return None

def discover_profile_url(directory_name, directory_domain, business_name, doctor_name, city, state='', country=''):
    """
    Find the profile URL for a business on a specific directory.
    
    Args:
        directory_name: e.g., "Yelp"
        directory_domain: e.g., "yelp.com"
        business_name: e.g., "209 NYC Dental"
        doctor_name: e.g., "Dr. John Smith" (optional)
        city: e.g., "New York"
        state: e.g., "NY" (optional)
        country: e.g., "Australia" (optional, VERY IMPORTANT for international)
    """
    # Skip domains that can't be searched via Google CSE
    skip_domains = ['google.com', 'business.google.com', 'google.com/maps', 'facebook.com', 'caredash.com']
    if directory_domain in skip_domains:
        print(f"DEBUG: Skipping {directory_name} - not searchable via Google CSE", flush=True)
        return {
            "status": "not_searchable",
            "url": "",
            "title": "",
            "match_reason": "gbp_not_searchable"
        }
    
    # Known directory domain corrections (when Perplexity gives wrong URLs)
    # Only applies when directory NAME doesn't match the provided URL domain
    # E.g., "American Dental Association" with URL "1800dentist.com" → corrects to "ada.org"
    domain_corrections = {
        'american dental association': 'ada.org',
        'ada find-a-dentist': 'ada.org',
        'academy of general dentistry': 'agd.org',
        'american academy of cosmetic dentistry': 'aacd.com',
        'american academy of implant dentistry': 'aaid.com',
        'better business bureau': 'bbb.org',
    }
    
    # Check if directory name matches any corrections
    dir_name_lower = directory_name.lower()
    for keyword, correct_domain in domain_corrections.items():
        if keyword in dir_name_lower:
            if directory_domain != correct_domain:
                print(f"DEBUG: Correcting domain from {directory_domain} to {correct_domain}", flush=True)
                directory_domain = correct_domain
            break
    
    # Build search query - quoted business name + location
    search_name = doctor_name if doctor_name else business_name
    
    # Construct location term
    # For international (non-US), country often works best (e.g. "Australia") like user's manual search.
    # For US, City+State is standard.
    is_us = not country or country.lower().strip() in ['united states', 'usa', 'us', 'united states of america']
    
    if is_us:
        location_term = f"{city} {state}"
    else:
        # Use simple country name for broad discovery (e.g. "Australia")
        location_term = country

    # Single search with quoted business name + location
    query = f'site:{directory_domain} "{search_name}" {location_term}'
    
    # Clean up any Double spaces
    query = " ".join(query.split())
    
    print(f"DEBUG: Serper search: {query}", flush=True)
    
    # Determine country code for Google Search (gl parameter)
    gl_param = "us" # Default
    if country:
        c_lower = country.lower().strip()
        # Common mappings
        if c_lower in ['australia', 'au']: gl_param = 'au'
        elif c_lower in ['canada', 'ca']: gl_param = 'ca'
        elif c_lower in ['united kingdom', 'uk', 'gb', 'great britain']: gl_param = 'uk'
        elif c_lower in ['new zealand', 'nz']: gl_param = 'nz'
        elif c_lower in ['ireland', 'ie']: gl_param = 'ie'
        elif c_lower in ['india', 'in']: gl_param = 'in'

    # FALLBACK SEARCH STRATEGY:
    # 1. Try with country name (e.g., "Australia") - most specific
    # 2. If no results, try without location - catches directories using state abbrevs like "NSW"
    results = search_serper(query, gl=gl_param)
    
    # If no results and we used a location term, try without it (fallback)
    if not results and location_term:
        fallback_query = f'site:{directory_domain} "{search_name}"'
        print(f"DEBUG: No results with location, trying fallback: {fallback_query}", flush=True)
        results = search_serper(fallback_query, gl=gl_param)
    
    if not results:
        print(f"DEBUG: No results for {directory_name}", flush=True)
        return {
            "status": "not_found",
            "url": "",
            "title": "",
            "match_reason": "no_results"
        }
    
    # Google returned results for our quoted query - now prioritize:
    # Priority 1: Direct profile page (business name in URL slug or title)
    # Priority 2: Doctor-specific page
    # Priority 3: List/category page that contains the business
    
    print(f"DEBUG: Got {len(results)} results, prioritizing...", flush=True)
    
    # Categorize results
    profile_matches = []
    doctor_matches = []
    list_matches = []
    
    for result in results:
        title = result.get('title', '')
        snippet = result.get('snippet', '')
        url = result.get('url', '')
        url_lower = url.lower()
        
        # Check if this looks like a direct profile page
        is_profile = False
        is_doctor = False
        is_list = False
        
        # Profile detection: business name in URL slug
        name_parts = [p for p in normalize_name(business_name).split() if len(p) > 2 and p not in ['the', 'and', 'of']]
        if len(name_parts) >= 2:
            slug_pattern = '-'.join(name_parts[:2])
            if slug_pattern in url_lower:
                is_profile = True
        
        # Also check if title contains business name
        if name_in_text(business_name, title):
            is_profile = True
        
        # Doctor detection
        if doctor_name and (name_in_text(doctor_name, title) or name_in_text(doctor_name, snippet)):
            is_doctor = True
        
        # List/category page detection
        list_patterns = ['/top-', '/best-', '/list', '/find-', '-near-', '-in-']
        if any(p in url_lower for p in list_patterns):
            is_list = True
            # Even list pages can be valid if business is mentioned
            if name_in_text(business_name, title) or name_in_text(business_name, snippet):
                is_profile = True  # Upgrade to profile if name is in title/snippet
        
        # Categorize
        result_with_meta = {"url": url, "title": title, "snippet": snippet}
        if is_profile:
            profile_matches.append(result_with_meta)
        elif is_doctor:
            doctor_matches.append(result_with_meta)
        elif is_list:
            # Only add as list match if business name was found in title/snippet
            if name_in_text(business_name, title) or name_in_text(business_name, snippet):
                list_matches.append(result_with_meta)
    
    # Collect all candidates for return
    all_candidates = profile_matches + doctor_matches + list_matches

    # Return best match by priority
    if profile_matches:
        best = profile_matches[0]
        print(f"DEBUG: FOUND (profile page): {best['url']}", flush=True)
        return {
            "status": "found", 
            "url": best['url'], 
            "title": best['title'], 
            "match_reason": "profile_match",
            "candidates": all_candidates
        }
    
    if doctor_matches:
        best = doctor_matches[0]
        print(f"DEBUG: FOUND (doctor page): {best['url']}", flush=True)
        return {
            "status": "found", 
            "url": best['url'], 
            "title": best['title'], 
            "match_reason": "doctor_match",
            "candidates": all_candidates
        }
    
    if list_matches:
        best = list_matches[0]
        print(f"DEBUG: FOUND (list/category page): {best['url']}", flush=True)
        return {
            "status": "found", 
            "url": best['url'], 
            "title": best['title'], 
            "match_reason": "list_match",
            "candidates": all_candidates
        }
    
    # UNCERTAIN RESULTS: Google returned results but none matched confidently
    print(f"DEBUG: Results found but filtered out (uncertain): {[r['url'] for r in results[:3]]}", flush=True)
    return {
        "status": "not_found", 
        "url": "", 
        "title": "", 
        "match_reason": "uncertain_results",
        "candidates": results # Return raw results as candidates if nothing better
    }
    # Use BeautifulSoup to validate if business actually exists on those pages
    if results:
        print(f"DEBUG: {len(results)} uncertain results - validating with BeautifulSoup...", flush=True)
        
        for result in results[:3]:  # Check top 3 uncertain results
            validation = validate_and_extract_profile(result['url'], business_name, directory_domain)
            
            if validation.get('valid'):
                profile_url = validation.get('profile_url', result['url'])
                print(f"DEBUG: FOUND (validated): {profile_url}", flush=True)
                return {"status": "found", "url": profile_url, "title": result['title'], "match_reason": "validated"}
        
        # None of the results validated - business not actually on those pages
        print(f"DEBUG: Validation failed for all {len(results[:3])} results", flush=True)
    
    # FALLBACK: BeautifulSoup direct search on directory website
    # This helps when Google returns NO results at all
    print(f"DEBUG: Trying direct directory search...", flush=True)
    direct_result = search_directory_directly(directory_domain, business_name, city, country)
    
    if direct_result:
        print(f"DEBUG: FOUND (direct search): {direct_result['url']}", flush=True)
        return {"status": "found", "url": direct_result['url'], "title": direct_result.get('title', ''), "match_reason": "direct_search"}
    
    # If all methods fail, return not_found
    print(f"DEBUG: NOT FOUND - no validated results for {business_name}", flush=True)
    return {"status": "not_found", "url": "", "title": "", "match_reason": "no_validated_results"}


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True)
    parser.add_argument("--domain", required=True)
    parser.add_argument("--business", required=True)
    parser.add_argument("--doctor", default="")
    parser.add_argument("--city", required=True)
    args = parser.parse_args()
    
    result = discover_profile_url(
        args.directory, 
        args.domain, 
        args.business, 
        args.doctor, 
        args.city
    )
    print(json.dumps(result, indent=2))
