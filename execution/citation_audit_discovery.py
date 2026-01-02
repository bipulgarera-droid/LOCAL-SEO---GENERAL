import os
import requests
import json
import re
import urllib.parse
from urllib.parse import urlparse

# API Keys
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") # Used for Google CSE
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")

def verify_url_exists(url, timeout=5):
    """
    Checks if a URL is reachable (returns 200-399 status code).
    Uses a browser-like User-Agent to avoid blocking.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
        }
        # Try HEAD first
        try:
            response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
            if response.status_code < 400:
                return True
        except requests.RequestException:
            pass # Fallback to GET

        # Try GET if HEAD fails
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        return response.status_code < 400
    except Exception:
        return False

def get_domain(url):
    """Extracts the base domain from a URL (e.g. https://www.ada.org/foo -> ada.org)"""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain.lower()
    except:
        return ""

def domain_matches_name(domain, name):
    """
    Checks if the domain matches the directory name semantically.
    1. Acronym matching (American Dental Association -> ADA in ada.org)
    2. Keyword matching (Central Ohio Dental Society -> match 'ohio', 'dental' in columbusdentalsociety.org)
    """
    if not domain or not name:
        return False
        
    # Use full domain for matching to distinguish subdomains (e.g. health.usnews.com)
    # Strip TLDs to avoid 'com', 'org' being matched as keywords
    target_domain = domain.lower()
    for tld in ['.com', '.org', '.net', '.edu', '.gov', '.io', '.co', '.us', '.uk']:
        if target_domain.endswith(tld):
            target_domain = target_domain[:-len(tld)]
            break # strip only the last part
            
    # Normalize name
    clean_name = re.sub(r'[^\w\s]', '', name.lower())
    words = clean_name.split()
    
    # Significant words (skip stop words)
    stop_words = {'the', 'and', 'or', 'of', 'for', 'in', 'a', 'an', 'at', 'to', 'by', 'inc', 'llc', 'ltd', 'com', 'org', 'net'}
    significant_words = [w for w in words if w not in stop_words]
    
    # 1. Check for Acronym Match (e.g. ADA)
    # Be strict: acronym must be at least 3 chars OR if 2 chars, very distinct? 
    if len(significant_words) > 1:
        acronym = "".join([w[0] for w in significant_words])
        if len(acronym) >= 2:
            # Check against full tokenized domain
            tokens = re.split(r'[.-]', target_domain)
            if acronym in tokens or target_domain.startswith(acronym):
                return True
    
    # 2. Check for Significant Keyword Match
    matches = 0
    for word in significant_words:
        if len(word) > 2 and word in target_domain: 
            matches += 1
            
    # Success threshold:
    # If name has 1-2 significant words, need at least 1 match
    # If name has 3+ significant words, need at least 2 matches (to avoid generic 'dental' matching random usage)
    if len(significant_words) <= 2:
        return matches >= 1
    else:
        return matches >= 2 # Require 2 matches for longer names (prevents 'WorthingtonDentalGroup' matching 'American Dental Association')
    
    return False

def search_correct_domain(directory_name):
    """
    Uses Google Custom Search Engine (CSE) to find the official homepage.
    Iterates through top 3 results and accepts the first one that matches the name/domain logic.
    """
    if not GEMINI_API_KEY or not GOOGLE_CSE_ID:
        print("DEBUG: Google CSE keys missing")
        return None
        
    try:
        print(f"DEBUG: Searching correct domain for: {directory_name}...", flush=True)
        # Search query: directory name only (cleanest)
        query = directory_name
        url = f"https://www.googleapis.com/customsearch/v1?key={GEMINI_API_KEY}&cx={GOOGLE_CSE_ID}&q={urllib.parse.quote(query)}&num=3"
        
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if 'items' in data:
            for item in data['items']:
                link = item['link']
                domain_str = get_domain(link)
                
                # Check if this result is valid
                if domain_matches_name(domain_str, directory_name):
                    print(f"DEBUG: Found correct domain: {link} (Matches '{directory_name}')", flush=True)
                    return f"https://{domain_str}"
                else:
                    print(f"DEBUG: Skipping result {link} - Domain mismatch for '{directory_name}'", flush=True)
                    
            print(f"DEBUG: No matching domain found in top 3 CSE results for '{directory_name}'", flush=True)
            return None
            
    except Exception as e:
        print(f"DEBUG: Google CSE search failed: {e}", flush=True)
        return None
    
    return None

def clean_and_validate_directories(directories, country="United States"):
    """
    Validates discovered directories.
    1. Checks if URL exists.
    2. Checks if Domain matches Name.
    3. If invalid/mismatch, tries to find correct URL via Google CSE.
    4. Updates URL if corrected, or keeps original if valid, or removes if fail.
    """
    cleaned = []
    seen = set()
    
    # Directories to exclude
    EXCLUDED_DIRECTORIES = {
        'facebook', 'facebook business', 'facebook business pages',
        'google business profile', 'google business', 'google my business', 'gbp',
        'caredash', 'care dash'
    }
    
    # BAD DIRECTORIES to filter out
    BAD_DOMAINS = {
        'clutch.co', 'clutchco.com.au', 'sortlist.com', 'goodfirms.co', 'upcity.com',
        'yext.com', 'brightlocal.com', 'moz.com', 'semrush.com', 'ahrefs.com',
        'whitespark.ca', 'localviking.com',
        'reddit.com', 'quora.com',
        'localstack.cloud', 'mojo.vision', 
        'provenexpert.com', 'trustpilot.com', 'trustindex.io'
    }
    
    # Dynamic TLD Exclusion based on Country
    c_lower = country.lower()
    BAD_TLDS = []
    
    # If target is NOT X, exclude X's TLDs
    if 'australia' not in c_lower: 
        BAD_TLDS.extend(['.au', '.com.au'])
    if 'united kingdom' not in c_lower and 'uk' not in c_lower:
        BAD_TLDS.extend(['.uk', '.co.uk'])
    if 'canada' not in c_lower:
        BAD_TLDS.extend(['.ca'])
    if 'germany' not in c_lower:
        BAD_TLDS.extend(['.de'])
    if 'france' not in c_lower:
        BAD_TLDS.extend(['.fr'])
    if 'india' not in c_lower:
        BAD_TLDS.extend(['.in', '.co.in'])
    
    # Whitelist of known good directory domains
    KNOWN_DIRECTORIES = {
        'healthgrades.com', 'zocdoc.com', 'vitals.com', 'ratemds.com', 'webmd.com',
        'yelp.com', 'yellowpages.com', 'bbb.org', 'manta.com', 'superpages.com',
        'findatopdoc.com', 'castleconnolly.com', 'sharecare.com', 'wellness.com',
        'usnews.com', 'superdoctors.com', 'hotfrog.com', 'brownbook.net', 'cylex.us.com',
        'foursquare.com', 'mapquest.com', 'nextdoor.com', 'angi.com', 'thumbtack.com',
        'chamberofcommerce.com', 'medifind.com', 'dexknows.com', 'n49.com',
        'threebestrated.com', 'opencare.com', 'topratedlocal.com'
    }
    
    # US-ONLY directories - useless for international projects
    # These have no/very limited coverage outside the United States
    US_ONLY_DIRECTORIES = {
        'mapquest.com', 'angi.com', 'thumbtack.com', 'nextdoor.com', 
        'homeadvisor.com', 'angieslist.com', 'superpages.com', 'dexknows.com',
        'manta.com', 'yellowpages.com', 'whitepages.com', 'citysearch.com',
        'local.com', 'insiderpages.com', 'kudzu.com', 'merchantcircle.com'
    }
    
    # Dynamic Name Exclusion based on Country
    # This prevents "USA Business Directory" from appearing in "Australia" results
    BAD_COUNTRY_TERMS = []
    if 'united states' not in c_lower and 'usa' not in c_lower:
        BAD_COUNTRY_TERMS.extend(['usa', 'united states', 'america', 'american', 'us'])
    if 'united kingdom' not in c_lower and 'uk' not in c_lower:
        BAD_COUNTRY_TERMS.extend(['uk', 'united kingdom', 'britain', 'british'])
    if 'australia' not in c_lower:
        BAD_COUNTRY_TERMS.extend(['australia', 'australian', 'sydney', 'melbourne']) # Add major cities if needed, but risky
    if 'canada' not in c_lower:
        BAD_COUNTRY_TERMS.extend(['canada', 'canadian'])
    
    # Competitor patterns to exclude
    COMPETITOR_PATTERNS = []
    
    for d in directories:
        name = d.get('name', '').strip()
        url = d.get('url', '').strip()
        
        if not name or not url:
            continue
        
        # Skip excluded directories
        name_lower = name.lower()
        
        # Check strict country terms (prevent cross-country leaks)
        tokens = set(re.split(r'[^a-z0-9]', name_lower))
        if any(term in tokens for term in BAD_COUNTRY_TERMS):
             print(f"DEBUG: Skipping wrong country directory: {name}", flush=True)
             continue
             
        if any(excl in name_lower for excl in EXCLUDED_DIRECTORIES):
            print(f"DEBUG: Skipping excluded directory: {name}", flush=True)
            continue
        
        # Skip competitor healthcare systems (unless on whitelist)
        domain = get_domain(url).lower()
        is_whitelisted = any(wd in domain for wd in KNOWN_DIRECTORIES)
        is_competitor = any(cp in domain for cp in COMPETITOR_PATTERNS)
        
        if is_competitor and not is_whitelisted:
            print(f"DEBUG: Skipping competitor site: {name} ({domain})", flush=True)
            continue
        
        # Skip BAD_DOMAINS (B2B, SEO tools, international, etc.)
        if any(bad in domain for bad in BAD_DOMAINS):
            print(f"DEBUG: Skipping bad domain: {name} ({domain})", flush=True)
            continue
        
        # Skip US-ONLY directories for international projects
        # Use EXACT match (domain == us_dir) not 'in' to avoid blocking yellowpages.com.au for yellowpages.com
        if 'united states' not in c_lower and 'usa' not in c_lower:
            if domain in US_ONLY_DIRECTORIES:
                print(f"DEBUG: Skipping US-only directory for {country}: {name} ({domain})", flush=True)
                continue
        
        # Skip international TLDs
        if any(url.lower().endswith(tld) or tld + '/' in url.lower() for tld in BAD_TLDS):
            print(f"DEBUG: Skipping international domain: {name} ({url})", flush=True)
            continue
            
        # Dedupe within this batch - by name AND domain
        domain_for_dedup = get_domain(url)
        if name_lower in seen or domain_for_dedup in seen:
            print(f"DEBUG: Skipping duplicate in batch: {name} ({domain_for_dedup})", flush=True)
            continue
        seen.add(name_lower)
        seen.add(domain_for_dedup)
        
        # 1. Base Domain Check
        domain = get_domain(url)
        
        # 2. Validation Checks
        is_reachable = verify_url_exists(url)
        is_semantic_match = domain_matches_name(domain, name)
        
        final_url = url
        
        if is_reachable and is_semantic_match:
            print(f"DEBUG: ✓ URL validated & matched: {url}", flush=True)
            d['url'] = final_url # Cleaned protocol/domain
            # Ensure protocol
            if not d['url'].startswith('http'): d['url'] = f"https://{d['url']}"
            cleaned.append(d)
        else:
            # Failure case: Try correction
            reason = "unreachable" if not is_reachable else "mismatch"
            print(f"DEBUG: ✗ URL {reason}: {url} vs {name}", flush=True)
            
            corrected_url = search_correct_domain(name)
            
            if corrected_url:
                print(f"DEBUG: ✓ Corrected to: {corrected_url}", flush=True)
                d['url'] = corrected_url
                cleaned.append(d)
            else:
                if is_reachable and "directory" in name.lower():
                     print(f"DEBUG: Discarding {name} - Could not verify correct URL.", flush=True)
                else:
                    print(f"DEBUG: Discarding {name}.", flush=True)

    return cleaned

def discover_directories(business_name, city, state, service_type, country="United States"):
    """
    Step 1: Discover Directories using Perplexity + Verification
    """
    try:
        url = "https://api.perplexity.ai/chat/completions"
        
        # Determine country code for domain filtering
        country_lower = country.lower()
        if 'united states' in country_lower or 'usa' in country_lower or 'us' in country_lower:
            country_domains = ".com, .org, .us, .gov"
            country_note = "US-based directories"
        elif 'canada' in country_lower:
            country_domains = ".ca, .com"
            country_note = "Canadian directories"
        elif 'united kingdom' in country_lower or 'uk' in country_lower:
            country_domains = ".co.uk, .uk, .com"
            country_note = "UK-based directories"
        elif 'australia' in country_lower:
            country_domains = ".com.au, .au, .com"
            country_note = "Australian directories"
        else:
            country_domains = ".com, .org"
            country_note = f"directories for {country}"
        
        # Localization Mapping for Service Terms
        # Format: { (ServiceKeyword, CountryKeyword): LocalizedTerm }
        # Note: Checks if ServiceKeyword is IN the service_type string (case-insensitive)
        SERVICE_LOCALIZATION = {
            # Movers
            ('mover', 'australia'): 'Removalist',
            ('moving', 'australia'): 'Removalists', # "Furniture Removals" is also good
            ('mover', 'united kingdom'): 'Removals',
            ('moving', 'united kingdom'): 'Removals',
            ('mover', 'uk'): 'Removals',
            ('moving', 'uk'): 'Removals',
            ('mover', 'new zealand'): 'Removalist',
            ('moving', 'new zealand'): 'Removalists',
            ('mover', 'ireland'): 'Removals',
            ('moving', 'ireland'): 'Removals',
            ('mover', 'india'): 'Packers and Movers',
            ('moving', 'india'): 'Packers and Movers',
            
            # Legal
            ('lawyer', 'australia'): 'Solicitor',
            ('attorney', 'australia'): 'Solicitor',
            ('law firm', 'australia'): 'Solicitors',
            ('lawyer', 'uk'): 'Solicitor',
            ('attorney', 'uk'): 'Solicitor',
            ('law firm', 'uk'): 'Solicitors',
            ('lawyer', 'new zealand'): 'Barrister and Solicitor',
            ('attorney', 'new zealand'): 'Barrister and Solicitor',
            ('lawyer', 'ireland'): 'Solicitor',
            ('attorney', 'ireland'): 'Solicitor',
            ('lawyer', 'india'): 'Advocate',
            ('attorney', 'india'): 'Advocate',
            
            # Real Estate
            ('real estate', 'uk'): 'Estate Agents',
            ('realtor', 'uk'): 'Estate Agents',
            ('real estate', 'ireland'): 'Estate Agents',
            ('realtor', 'ireland'): 'Auctioneers',
            ('real estate', 'australia'): 'Real Estate Agents',
            ('realtor', 'australia'): 'Real Estate Agents',
            ('real estate', 'india'): 'Property Dealers',
            
            # Trades
            ('hvac', 'australia'): 'Air Conditioning',
            ('hvac', 'uk'): 'Air Conditioning',
            ('hvac', 'united kingdom'): 'Air Conditioning',
            ('hvac', 'new zealand'): 'Heat Pumps', # Very common in NZ
            ('auto repair', 'australia'): 'Mechanic',
            ('auto repair', 'uk'): 'Garage',
            ('mechanic', 'uk'): 'Garage',
            
            # Health
            ('drug store', 'australia'): 'Chemist',
            ('drug store', 'uk'): 'Chemist',
            ('pharmacy', 'australia'): 'Chemist',
            ('pharmacy', 'uk'): 'Chemist',
            ('drug store', 'new zealand'): 'Chemist',
            ('pharmacy', 'new zealand'): 'Pharmacy', # Chemist is also used
            ('gym', 'uk'): 'Fitness Centre',
            ('gym', 'australia'): 'Fitness Centre',
            
            # Food & Drink & Retail
            ('bar', 'uk'): 'Pub',
            ('bar', 'australia'): 'Pub',
            ('bar', 'ireland'): 'Pub',
            ('liquor store', 'australia'): 'Bottle Shop',
            ('liquor store', 'uk'): 'Off Licence',
            ('liquor store', 'ireland'): 'Off Licence',
            ('liquor store', 'new zealand'): 'Bottle Store',
        }
        
        localized_service = service_type
        # Simple lookup
        s_lower = service_type.lower()
        c_lower = country.lower()
        
        for (svc_key, country_key), local_term in SERVICE_LOCALIZATION.items():
            if svc_key in s_lower and country_key in c_lower:
                localized_service = local_term
                print(f"DEBUG: Localized service '{service_type}' to '{localized_service}' for {country}", flush=True)
                break
        
        prompt = f"""
        You are conducting a comprehensive Citation Audit for local SEO.
        
        TARGET BUSINESS:
        - Name: "{business_name}"
        - Location: {city}, {state}, {country}
        - Category/Industry: {service_type} (Local Term: {localized_service})
        
        YOUR TASK: Find ALL citation directories where this business IS listed or SHOULD BE listed.
        
        USE THIS DISCOVERY METHOD (search the web for each):
        
        **PRIORITY 1 - INDUSTRY SPECIFIC DIRECTORIES ({localized_service})** [15+ directories]
        Search: "Best directories for {localized_service} businesses"
        Search: "Where are {localized_service} listed online in {country}?"
        Search: "{country} {localized_service} association directory"
        Find: National {localized_service} associations (e.g. AFRA for Removalists)
        Find: Industry-specific review and listing sites
        
        **PRIORITY 2 - LOCAL & REGIONAL DIRECTORIES ({city}, {state})** [10+ directories]
        Search: "Business directories in {city} {state}"
        Search: "{state} {localized_service} association directory"
        Search: "Local business listings {city} {state}"
        Find: {city} Chamber of Commerce, {state} Chamber of Commerce
        Find: Regional business alliance directories
        
        **PRIORITY 3 - GENERAL BUSINESS DIRECTORIES** [15+ directories]
        Search: "Top local business directories {country}"
        Find: Yelp, YellowPages, BBB, SuperPages, Manta, Hotfrog, Angi, Thumbtack
        Find: Map platforms: Apple Maps, Bing Places, Foursquare, MapQuest
        Find: Review sites: Nextdoor Business, Trustpilot (if relevant)
        
        FOCUS ON:
        - Directories in {country} ONLY.
        - Directories that allow FREE business profile creation with NAP (Name, Address, Phone)
        - Directories where similar {service_type} businesses in {city}/{state} are actually listed
        
        STRICT EXCLUSIONS (NEVER include):
        - Facebook / Meta / Instagram / LinkedIn (Social Media files)
        - Google Business Profile / Google Maps (handled separately)
        - CareDash
        - B2B/Agency directories (Clutch, Sortlist, GoodFirms, UpCity) - Unless the business IS an agency
        - Wrong country domains (e.g., .au, .uk for US businesses)
        - SEO tools (Yext, BrightLocal, Moz, Whitespark)
        - Paid-only directories
        - Directories for other countries (e.g., if target is Australia, DO NOT include USA directories)
        
        OUTPUT REQUIREMENTS:
        - Provide at least 40 high-quality directories
        - Tag each with category: "specialty", "local", or "general"
        - URL must be homepage domain only (e.g., https://yelp.com, https://healthgrades.com)
       
        Return JSON with key "directories" containing list of objects:
        {{"name": "Directory Name", "url": "https://domain.com", "category": "specialty|local|general"}}
        
        """
        
        payload = {
            "model": "sonar",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a strategic SEO auditor. You find gaps and opportunities."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2, # Low temp for precision
            "max_tokens": 8000
        }
        
        headers = {
            "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code != 200:
            print(f"DEBUG: Perplexity API Error {response.status_code}: {response.text}", flush=True)
            return []
            
        try:
            data = response.json()
        except Exception as e:
            print(f"DEBUG: Failed to parse Perplexity JSON: {response.text[:500]}...", flush=True)
            raise e
        
        content = data['choices'][0]['message']['content']
        # Strip markdown code blocks
        content = content.replace("```json", "").replace("```", "").strip()
        
        directories_data = json.loads(content)
        
        # Extract the list from the wrapper object
        if isinstance(directories_data, dict):
            directories = directories_data.get('directories', [])
        else:
            directories = directories_data # Already a list
        
        # Run cleanup and verification
        print(f"DEBUG: validating {len(directories)} raw directories for {country}...", flush=True)
        verified_directories = clean_and_validate_directories(directories, country)
        
        return verified_directories
        
    except Exception as e:
        print(f"DEBUG: Discovery failed: {e}", flush=True)
        return []
