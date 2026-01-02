"""
citation_audit_verify_nap.py

Automated URL verification for citation audit.
Verifies if the stored profile URL for a directory actually mentions the business NAP.
"""

import os
import re
import requests
import json
from urllib.parse import urlparse

# Fuzzy Matching: Try RapidFuzz, Fallback to FuzzyWuzzy, Fallback to Minimal
try:
    from rapidfuzz import fuzz
except ImportError:
    try:
        from fuzzywuzzy import fuzz
    except ImportError:
        # Minimal fallback class to avoid 500 errors if libraries missing
        class fuzz:
            @staticmethod
            def partial_ratio(s1, s2):
                if not s1 or not s2: return 0
                s1, s2 = s1.lower(), s2.lower()
                if s1 in s2 or s2 in s1: return 85
                return 0
            
            @staticmethod
            def ratio(s1, s2):
                return fuzz.partial_ratio(s1, s2)

def normalize_text(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text).lower().strip())

def normalize_phone(phone):
    if not phone: return ""
    return re.sub(r'\D', '', str(phone))

def scrape_content(url, max_retries=2):
    """
    Fetches URL content with Camoufox (stealth browser, bypasses Cloudflare).
    Primary: Camoufox (85% success against Cloudflare Bot Management)
    Secondary: Cloudscraper (for simpler protections)
    Fallback: Jina Reader (for clean markdown when available)
    """
    import time
    
    # Try Camoufox first (best Cloudflare bypass - 85% success rate)
    try:
        from camoufox.sync_api import Camoufox
        print(f"DEBUG: Scraping with Camoufox: {url}", flush=True)
        
        with Camoufox(headless=True) as browser:
            page = browser.new_page()
            page.goto(url, timeout=60000, wait_until='networkidle')
            
            # Wait for Cloudflare challenge to complete (critical for bypass)
            time.sleep(6)
            
            content = page.content()
            
            if len(content) > 500:
                # Check if we got actual content (not a block page)
                # Only consider it blocked if content is SHORT and has block indicators
                content_lower = content.lower()
                block_indicators = ['verify you are human', 'access denied', 'enable javascript and cookies']
                is_short_blocked = len(content) < 5000 and any(indicator in content_lower for indicator in block_indicators)
                
                if not is_short_blocked:
                    print(f"DEBUG: Camoufox SUCCESS - got {len(content)} chars", flush=True)
                    return content
                else:
                    print(f"DEBUG: Camoufox got blocked page ({len(content)} chars), trying Cloudscraper...", flush=True)
            else:
                print(f"DEBUG: Camoufox got minimal content ({len(content)} chars), trying Cloudscraper...", flush=True)
    except Exception as e:
        print(f"DEBUG: Camoufox failed: {e}", flush=True)
    
    # Try Cloudscraper (for simpler protections)
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'mobile': False
            }
        )
        print(f"DEBUG: Scraping with Cloudscraper: {url}", flush=True)
        resp = scraper.get(url, timeout=20)
        
        if resp.status_code == 200 and len(resp.text) > 200:
            content_lower = resp.text.lower()
            block_indicators = ['verify you are human', 'captcha', 'access denied', 'please wait']
            if not any(indicator in content_lower for indicator in block_indicators):
                print(f"DEBUG: Cloudscraper SUCCESS - got {len(resp.text)} chars", flush=True)
                return resp.text
            else:
                print(f"DEBUG: Cloudscraper got blocked page, trying Jina...", flush=True)
        else:
            print(f"DEBUG: Cloudscraper returned {resp.status_code}, trying Jina...", flush=True)
    except Exception as e:
        print(f"DEBUG: Cloudscraper failed: {e}", flush=True)
    
    # Fallback to Jina (good for clean text extraction)
    for attempt in range(max_retries):
        try:
            jina_url = f"https://r.jina.ai/{url}"
            resp = requests.get(jina_url, timeout=15, headers={"Accept": "text/markdown"})
            if resp.status_code == 200 and len(resp.text) > 100:
                content_lower = resp.text.lower()
                if 'verify you are human' not in content_lower and 'just a moment' not in content_lower:
                    print(f"DEBUG: Jina SUCCESS - got {len(resp.text)} chars", flush=True)
                    return resp.text
        except Exception as e:
            print(f"DEBUG: Jina attempt {attempt+1} failed: {e}", flush=True)
        
        if attempt < max_retries - 1:
            time.sleep(1)
    
    print(f"DEBUG: All scrape methods failed for {url}", flush=True)
    return ""


def verify_nap(url, business_name, phone, address_parts=[], website_url=None):
    """
    Verifies if NAP info is present on the page.
    Also checks if client's website URL appears on the listing.
    Returns: { status: 'found'|'not_found', confidence: 0-100, details: str, nap_website_ok: bool }
    """
    if not url or not url.startswith('http'):
        return {'status': 'not_found', 'confidence': 0, 'details': 'Invalid URL'}

    print(f"DEBUG: Verifying NAP on {url}...", flush=True)
    print(f"DEBUG: NAP inputs - name='{business_name}', phone='{phone}', address_parts={address_parts}, website='{website_url}'", flush=True)
    content = scrape_content(url)
    
    if not content:
        return {'status': 'error', 'confidence': 0, 'details': 'Could not scrape page', 'nap_website_ok': None}

    content_norm = normalize_text(content)
    
    # Check for soft blocks - only if content is VERY short AND has block keywords
    BLOCK_KEYWORDS = ['403 forbidden', 'access denied', 'you are blocked', 'captcha required', 
                      'verify you are human', 'just a moment', 'enable javascript']
    is_blocked = any(k in content_norm for k in BLOCK_KEYWORDS) and len(content_norm) < 2000
    
    if is_blocked:
        domain = urlparse(url).netloc.lower()
        print(f"DEBUG: Scraper blocked on {url} (content too short with block keywords)", flush=True)
        return {
            'status': 'found',  # Keep as found - URL is valid
            'confidence': 0,
            'details': f'Scraper blocked by {domain.split(".")[0].title()}. NAP checks require manual verification.',
            'nap_name_ok': None,
            'nap_address_ok': None,
            'nap_phone_ok': None,
            'nap_website_ok': None
        }

    # 1. Phone Match (Best signal)
    phone_norm = normalize_phone(phone)
    phone_score = 0
    if phone_norm and len(phone_norm) >= 10:
        # Look for the phone number in content (ignoring formatting)
        content_phones = re.sub(r'\D', '', content)
        if phone_norm in content_phones:
            print(f"DEBUG: Phone match found for {phone_norm}", flush=True)
            phone_score = 100
        else:
             print(f"DEBUG: Phone {phone_norm} NOT found in content.", flush=True)

    # 2. Business Name Match
    name_score = fuzz.partial_ratio(normalize_text(business_name), content_norm)
    print(f"DEBUG: Name score for '{business_name}': {name_score}% (threshold: 80%)", flush=True)
    
    # 3. Address Match
    calc_address_score = 0
    if address_parts:
        # Check for street, city, zip presence
        hits = 0
        total = 0
        for part in address_parts:
            if not part: continue
            total += 1
            if normalize_text(part) in content_norm:
                hits += 1
        
        if total > 0:
            calc_address_score = int((hits / total) * 100)
        print(f"DEBUG: Address score: {calc_address_score}% ({hits}/{total} parts found)", flush=True)

    # 4. Website URL Match (Check if client's website domain appears on listing)
    website_ok = None  # None means not checked (no website provided)
    if website_url:
        # Extract domain from website URL
        try:
            website_domain = urlparse(website_url).netloc.lower().replace('www.', '')
            
            # Check 1: Domain appears directly in content
            domain_found = website_domain and website_domain in content_norm
            
            # Check 2: Common phrases that indicate a website link exists
            website_phrases = [
                # Direct phrases
                'visit website', 'view website', 'go to website', 'website:', 
                'visit site', 'view site', 'go to site', 'official website',
                'click to visit', 'click here to visit', 'click to view',
                # Ownership phrases
                'visit their website', 'view their website', 'their website',
                'visit our website', 'our website', 'my website',
                'business website', 'practice website', 'clinic website', 'office website',
                'company website', 'doctor website', 'dentist website',
                # Link text variations
                'www.', 'http://', 'https://',
                'homepage', 'home page', 'main site',
                # Button/CTA text
                'learn more at', 'find us at', 'see us at',
                'get directions', 'contact us online', 'book online',
                'schedule online', 'request appointment', 'online booking',
                # External link indicators
                'external link', 'opens in new', 'new window', 'new tab',
                # Icons/labels
                '[website]', '(website)', 'web:', 'url:',
                'visit now', 'go now', 'click here',
            ]
            phrase_found = any(phrase in content_norm for phrase in website_phrases)
            
            if domain_found:
                print(f"DEBUG: Website {website_domain} found on listing", flush=True)
                website_ok = True
            else:
                # Domain not found - mark as NOT found
                # Don't give benefit of doubt on generic phrases like "visit website"
                print(f"DEBUG: Website {website_domain} NOT found on listing", flush=True)
                website_ok = False
        except:
            website_ok = None

    # Weighted Score
    # Phone is strong evidence (50%), Name (30%), Address (20%)
    final_score = (phone_score * 0.5) + (name_score * 0.3) + (calc_address_score * 0.2)
    
    # Important: If phone score is 100, that's a very strong signal.
    if phone_score == 100:
        final_score = max(final_score, 90) # Boost confidence
    
    # Generate detailed human-readable description
    details = generate_detailed_description(
        business_name=business_name,
        phone=phone,
        address_parts=address_parts,
        content=content[:5000],  # Limit content for LLM
        phone_found=(phone_score == 100),
        name_score=name_score,
        address_score=calc_address_score,
        website_ok=website_ok
    )
    
    print(f"DEBUG: Generated details: {details[:200]}...", flush=True)
    
    return {
        'status': 'found', 
        'confidence': int(final_score),
        'details': details,
        'nap_name_ok': name_score >= 80,
        'nap_address_ok': calc_address_score >= 60,
        'nap_phone_ok': phone_score == 100,
        'nap_website_ok': website_ok
    }


def generate_detailed_description(business_name, phone, address_parts, content, phone_found, name_score, address_score, website_ok=None):
    """
    Generate a simple 4-line NAP+W summary.
    """
    lines = []
    
    # Line 1: Name
    if name_score >= 90:
        lines.append(f"✓ Name: Found exactly")
    elif name_score >= 80:
        lines.append(f"✓ Name: Partial match")
    elif name_score >= 70:
        lines.append(f"⚠ Name: Weak match")
    else:
        lines.append(f"✗ Name: Not found")
    
    # Line 2: Address
    if address_score >= 80:
        lines.append(f"✓ Address: Found")
    elif address_score >= 60:
        lines.append(f"⚠ Address: Partial")
    else:
        lines.append(f"✗ Address: Not found")
    
    # Line 3: Phone
    if phone_found:
        lines.append(f"✓ Phone: Verified")
    else:
        lines.append(f"✗ Phone: Not found")
    
    # Line 4: Website
    if website_ok is True:
        lines.append(f"✓ Website: Link found")
    elif website_ok is False:
        lines.append(f"✗ Website: Not found")
    else:
        lines.append(f"— Website: Not checked")
    
    return " | ".join(lines)

# Entry point for module usage
def perform_nap_verification(audit_row, project_data):
    """
    Wrapper to be called from API endpoint logic.
    """
    url = audit_row.get('directory_profile_url') or audit_row.get('directory_website')
    
    # Construct NAP data
    nap_name = project_data.get('business_name')
    nap_phone = project_data.get('phone')
    nap_address_parts = [
        project_data.get('street_address'),
        project_data.get('city'),
        project_data.get('zip_code')
    ]
    
    return verify_nap(url, nap_name, nap_phone, nap_address_parts)
