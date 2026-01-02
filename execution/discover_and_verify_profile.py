"""
discover_and_verify_profile.py

Combined Step 2/3: For a given directory, search for the business profile,
extract NAP data, and calculate similarity scores.

Uses:
- Gemini with Google Search grounding to find candidate URLs
- Jina Reader to scrape pages
- Schema.org JSON-LD parsing for NAP extraction
- Fuzzy matching for similarity scores
"""

import os
import re
import json
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

# Try to import rapidfuzz for fuzzy matching, fallback to fuzzywuzzy
try:
    from rapidfuzz import fuzz
except ImportError:
    try:
        from fuzzywuzzy import fuzz
    except ImportError:
        # Minimal fallback
        class fuzz:
            @staticmethod
            def ratio(s1, s2):
                if not s1 or not s2:
                    return 0
                s1, s2 = s1.lower(), s2.lower()
                if s1 == s2:
                    return 100
                # Simple substring check
                if s1 in s2 or s2 in s1:
                    return 80
                return 0

load_dotenv()


def normalize_phone(phone):
    """Strip all non-digit characters from phone number."""
    if not phone:
        return ""
    return re.sub(r'\D', '', phone)


def normalize_address(address):
    """Basic address normalization."""
    if not address:
        return ""
    addr = address.lower().strip()
    # Common abbreviations
    replacements = {
        'street': 'st',
        'avenue': 'ave',
        'boulevard': 'blvd',
        'drive': 'dr',
        'road': 'rd',
        'suite': 'ste',
        'apartment': 'apt',
        'floor': 'fl',
        ',': '',
        '.': '',
    }
    for old, new in replacements.items():
        addr = addr.replace(old, new)
    # Remove extra whitespace
    addr = ' '.join(addr.split())
    return addr


def calculate_similarity(source_nap, extracted_nap):
    """
    Calculate similarity score between source and extracted NAP.
    Returns: { total_score, name_score, address_score, phone_score }
    """
    scores = {}
    
    # Name matching (use token sort ratio for flexibility)
    source_name = (source_nap.get('doctor_name') or source_nap.get('business_name') or '').lower()
    extracted_name = (extracted_nap.get('name') or '').lower()
    
    # Try token_sort_ratio if available, else use ratio
    try:
        scores['name_score'] = fuzz.token_sort_ratio(source_name, extracted_name)
    except:
        scores['name_score'] = fuzz.ratio(source_name, extracted_name)
    
    # Address matching
    source_addr = normalize_address(
        f"{source_nap.get('street_address', '')} {source_nap.get('city', '')} {source_nap.get('state', '')} {source_nap.get('zip_code', '')}"
    )
    extracted_addr = normalize_address(extracted_nap.get('address', ''))
    
    try:
        scores['address_score'] = fuzz.token_sort_ratio(source_addr, extracted_addr)
    except:
        scores['address_score'] = fuzz.ratio(source_addr, extracted_addr)
    
    # Phone matching (exact match after normalization)
    source_phone = normalize_phone(source_nap.get('phone', ''))
    extracted_phone = normalize_phone(extracted_nap.get('phone', ''))
    
    if source_phone and extracted_phone:
        # Check if one contains the other (handles country codes)
        if source_phone in extracted_phone or extracted_phone in source_phone:
            scores['phone_score'] = 100
        elif source_phone == extracted_phone:
            scores['phone_score'] = 100
        else:
            scores['phone_score'] = 0
    else:
        scores['phone_score'] = 0  # Can't compare if missing
    
    # Weighted average (name is most important)
    weights = {'name': 0.5, 'address': 0.3, 'phone': 0.2}
    scores['total_score'] = int(
        scores['name_score'] * weights['name'] +
        scores['address_score'] * weights['address'] +
        scores['phone_score'] * weights['phone']
    )
    
    return scores


def extract_nap_from_jsonld(html_content):
    """
    Extract NAP from schema.org JSON-LD data.
    Returns: { name, address, phone } or None
    """
    try:
        # Find all JSON-LD blocks
        jsonld_pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, html_content, re.DOTALL | re.IGNORECASE)
        
        for match in matches:
            try:
                data = json.loads(match.strip())
                
                # Handle @graph structure
                if isinstance(data, dict) and '@graph' in data:
                    data = data['@graph']
                
                # Make it a list for uniform processing
                if isinstance(data, dict):
                    data = [data]
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    
                    item_type = item.get('@type', '')
                    
                    # Look for relevant types
                    relevant_types = ['Physician', 'Dentist', 'MedicalBusiness', 'LocalBusiness', 
                                      'Dermatologist', 'PlasticSurgery', 'HealthAndBeautyBusiness',
                                      'MedicalClinic', 'Hospital', 'Person', 'Organization']
                    
                    if not any(t in str(item_type) for t in relevant_types):
                        continue
                    
                    result = {}
                    
                    # Extract name
                    result['name'] = item.get('name', '')
                    
                    # Extract address
                    address = item.get('address', {})
                    if isinstance(address, dict):
                        parts = [
                            address.get('streetAddress', ''),
                            address.get('addressLocality', ''),
                            address.get('addressRegion', ''),
                            address.get('postalCode', '')
                        ]
                        result['address'] = ', '.join([p for p in parts if p])
                    elif isinstance(address, str):
                        result['address'] = address
                    else:
                        result['address'] = ''
                    
                    # Extract phone
                    result['phone'] = item.get('telephone', '') or item.get('phone', '')
                    
                    if result['name'] or result['address'] or result['phone']:
                        return result
                        
            except json.JSONDecodeError:
                continue
                
    except Exception as e:
        print(f"DEBUG: JSON-LD extraction error: {e}", flush=True)
    
    return None


def extract_nap_fallback(content, doctor_name):
    """
    Fallback NAP extraction using regex patterns.
    Returns: { name, address, phone } or None
    """
    result = {'name': '', 'address': '', 'phone': ''}
    
    # Phone patterns
    phone_patterns = [
        r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # (123) 456-7890 or 123-456-7890
        r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # +1 format
    ]
    for pattern in phone_patterns:
        match = re.search(pattern, content)
        if match:
            result['phone'] = match.group()
            break
    
    # Try to find doctor name in content
    if doctor_name:
        doctor_lower = doctor_name.lower()
        content_lower = content.lower()
        if doctor_lower in content_lower:
            result['name'] = doctor_name
    
    # Address is hard to extract without structure, skip for fallback
    
    return result if result['phone'] or result['name'] else None


def search_with_google_cse(query):
    """
    Use Google Custom Search API to search and return URLs.
    Returns: List of { url, title, snippet }
    """
    api_key = os.environ.get('GEMINI_API_KEY')  # Using same key - it's a Google Cloud API key
    cx_id = os.environ.get('GOOGLE_CSE_ID', '31b19af92a2e848a9')  # Default to user's CX
    
    if not api_key:
        print("DEBUG: Google API key not found", flush=True)
        return []
    
    url = "https://www.googleapis.com/customsearch/v1"
    
    params = {
        "key": api_key,
        "cx": cx_id,
        "q": query,
        "num": 5  # Get top 5 results
    }
    
    try:
        print(f"DEBUG: Google CSE search: {query}", flush=True)
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code != 200:
            print(f"DEBUG: Google CSE failed: {response.status_code} {response.text[:200]}", flush=True)
            return []
        
        data = response.json()
        
        # Parse results
        results = []
        for item in data.get('items', []):
            results.append({
                "url": item.get('link', ''),
                "title": item.get('title', ''),
                "snippet": item.get('snippet', '')
            })
        
        print(f"DEBUG: Google CSE returned {len(results)} results", flush=True)
        return results
        
    except Exception as e:
        print(f"DEBUG: Google CSE error: {e}", flush=True)
        return []


def scrape_page(url):
    """
    Scrape a page using Jina Reader with fallback to requests.
    Returns: { html: raw HTML, text: markdown text }
    """
    result = {'html': '', 'text': ''}
    
    # Try Jina first (returns markdown, good for text)
    try:
        jina_url = f"https://r.jina.ai/{url}"
        jina_resp = requests.get(jina_url, timeout=15, headers={"Accept": "text/markdown"})
        if jina_resp.status_code == 200:
            result['text'] = jina_resp.text
    except Exception as e:
        print(f"DEBUG: Jina scrape failed for {url}: {e}", flush=True)
    
    # Also get raw HTML for JSON-LD extraction
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            result['html'] = resp.text
    except Exception as e:
        print(f"DEBUG: HTML fetch failed for {url}: {e}", flush=True)
    
    return result


def discover_and_verify_profile(directory_name, directory_domain, source_nap, threshold=70):
    """
    Main function: Search for a business profile on a directory and verify it.
    
    Args:
        directory_name: e.g., "Healthgrades"
        directory_domain: e.g., "healthgrades.com"
        source_nap: {
            doctor_name: "Dr. John Smith",
            business_name: "Smith Clinic",
            street_address: "123 Main St",
            city: "New York",
            state: "NY",
            zip_code: "10001",
            phone: "(555) 123-4567"
        }
        threshold: Minimum similarity score to accept (default 70)
    
    Returns:
        {
            status: "found" | "not_found",
            url: "https://...",
            extracted_nap: { name, address, phone },
            similarity_score: 85,
            match_details: { name_score, address_score, phone_score }
        }
    """
    doctor_name = source_nap.get('doctor_name', '')
    business_name = source_nap.get('business_name', '')
    city = source_nap.get('city', '')
    
    state = source_nap.get('state', '')
    
    search_name = doctor_name if doctor_name else business_name
    
    # Construct search query with state for better precision
    query = f"site:{directory_domain} {search_name} {city} {state}"
    print(f"DEBUG: Searching: {query}", flush=True)
    
    # Search using Google CSE
    search_results = search_with_google_cse(query)
    
    if not search_results or isinstance(search_results, dict):
        print(f"DEBUG: No search results for {directory_name}", flush=True)
        return {
            "status": "not_found",
            "url": "",
            "extracted_nap": {},
            "similarity_score": 0,
            "match_details": {},
            "log": ["No search results found"]
        }
    
    # URL patterns to skip (category/search pages, not profiles)
    SKIP_URL_PATTERNS = [
        '/search', '/category', '/browse', '/listings', '/directory',
        '/find-a-', '/find-doctor', '/specialty/', '/articles/', '/topics/',
        'best-', 'top-', '-near-me', 'dentists-in-', 'doctors-in-', 
        '/all-', '/list-of-', '/index.'
    ]
    
    # Evaluate each candidate
    best_match = None
    best_score = 0
    log = []
    
    for candidate in search_results[:5]:
        url = candidate.get('url', '')
        if not url:
            continue
        
        # Skip category/search pages
        url_lower = url.lower()
        if any(pat in url_lower for pat in SKIP_URL_PATTERNS):
            log.append(f"Skipped (category page): {url}")
            print(f"DEBUG: Skipping category page: {url}", flush=True)
            continue
        
        log.append(f"Checking: {url}")
        print(f"DEBUG: Evaluating {url}", flush=True)
        
        # Scrape the page
        page_data = scrape_page(url)
        
        if not page_data['html'] and not page_data['text']:
            log.append(f"  - Could not scrape")
            continue
        
        # STRICT VALIDATION: Verify business name is on page using proven logic from index.py
        page_content_lower = (page_data['text'] or page_data['html'] or '').lower()
        
        # Normalize business name
        clean_name = search_name.lower().replace('dr.', '').replace('dr ', '').strip()
        name_parts = [p for p in clean_name.split() if len(p) > 2]
        
        # Check 1: Full name present
        name_found = clean_name in page_content_lower
        
        # Check 2: All significant parts present (e.g. "Andrew" and "Jacono")
        if not name_found and name_parts:
            name_found = all(part in page_content_lower for part in name_parts)
        
        # Check 3: Slug-based fallback (for JS-rendered pages)
        if not name_found:
            try:
                slug = url.lower().split('?')[0]
                # Check if name is in slug
                if clean_name.replace(' ', '-') in slug or clean_name.replace(' ', '') in slug:
                    name_found = True
                    log.append(f"  - Name found in URL slug")
                elif all(part in slug for part in name_parts):
                    name_found = True
                    log.append(f"  - All name parts found in URL slug")
            except:
                pass
        
        if not name_found:
            log.append(f"  - Business name '{search_name}' not found in content or URL")
            print(f"DEBUG: Business name not verified on page: {url}", flush=True)
            continue
        
        # Extract NAP (try JSON-LD first, then fallback)
        extracted_nap = extract_nap_from_jsonld(page_data['html'])
        
        if not extracted_nap:
            log.append(f"  - No JSON-LD, trying fallback")
            extracted_nap = extract_nap_fallback(page_data['text'] or page_data['html'], search_name)
        
        if not extracted_nap:
            log.append(f"  - Could not extract NAP")
            continue
        
        log.append(f"  - Extracted: {extracted_nap}")
        
        # Calculate similarity
        scores = calculate_similarity(source_nap, extracted_nap)
        log.append(f"  - Scores: {scores}")
        
        if scores['total_score'] > best_score:
            best_score = scores['total_score']
            best_match = {
                "url": url,
                "extracted_nap": extracted_nap,
                "similarity_score": scores['total_score'],
                "match_details": scores
            }
    
    # Return best match if above threshold
    if best_match and best_match['similarity_score'] >= threshold:
        return {
            "status": "found",
            **best_match,
            "log": log
        }
    else:
        return {
            "status": "not_found",
            "url": best_match['url'] if best_match else "",
            "extracted_nap": best_match['extracted_nap'] if best_match else {},
            "similarity_score": best_score,
            "match_details": best_match['match_details'] if best_match else {},
            "log": log,
            "reason": f"Best score {best_score} below threshold {threshold}"
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", required=True, help="Directory name")
    parser.add_argument("--domain", required=True, help="Directory domain")
    parser.add_argument("--doctor", default="", help="Doctor name")
    parser.add_argument("--business", default="", help="Business name")
    parser.add_argument("--city", required=True, help="City")
    parser.add_argument("--state", default="", help="State")
    parser.add_argument("--phone", default="", help="Phone")
    parser.add_argument("--address", default="", help="Street address")
    args = parser.parse_args()
    
    source_nap = {
        "doctor_name": args.doctor,
        "business_name": args.business,
        "city": args.city,
        "state": args.state,
        "phone": args.phone,
        "street_address": args.address
    }
    
    result = discover_and_verify_profile(args.directory, args.domain, source_nap)
    print(json.dumps(result, indent=2))
