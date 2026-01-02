"""
scrape_add_business_guide.py

Scrapes a directory website to find the "Add Business" page and extract:
1. The exact URL to add/claim a listing
2. Required form fields
3. Verification details

Uses Camoufox for scraping and Gemini for analysis.
"""

import os
import re
import time
import json
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

load_dotenv()


def scrape_with_camoufox(url):
    """Scrape a URL using Camoufox (stealth Firefox)."""
    try:
        from camoufox.sync_api import Camoufox
        print(f"DEBUG: Scraping with Camoufox: {url}", flush=True)
        
        with Camoufox(headless=True) as browser:
            page = browser.new_page()
            page.goto(url, timeout=45000, wait_until='domcontentloaded')
            
            # Wait for page to settle
            time.sleep(3)
            
            content = page.content()
            print(f"DEBUG: Got {len(content)} chars from {url}", flush=True)
            return content
            
    except Exception as e:
        print(f"DEBUG: Camoufox error: {e}", flush=True)
        return None


def serper_find_add_business(directory_domain):
    """
    Use Serper to search for the Add Business page on a directory.
    Uses broad keywords without strict quoting for better matching.
    
    Returns: URL of add business page or None
    """
    import requests
    
    api_key = os.environ.get('SERPER_API_KEY')
    if not api_key:
        print("DEBUG: SERPER_API_KEY not found", flush=True)
        return None
    
    # Extract domain from URL
    parsed = urlparse(directory_domain)
    domain = parsed.netloc or parsed.path.split('/')[0]
    domain = domain.replace('www.', '')
    
    # Broad search query - no strict quotes, uses OR for multiple keywords
    # This finds pages mentioning any of these terms
    query = f'site:{domain} add business OR add listing OR submit business OR claim listing OR list your business OR register business OR signup business OR login business OR create listing OR free listing OR add your business OR submit your business OR claim your listing OR get listed OR advertise with us OR for business owners'
    
    try:
        print(f"DEBUG: Serper search for add business: {query}", flush=True)
        
        response = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"q": query, "num": 5},
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"DEBUG: Serper error: {response.status_code}", flush=True)
            return None
        
        data = response.json()
        organic = data.get('organic', [])
        
        if organic:
            # Return the first result - most likely the add business page
            best_url = organic[0].get('link', '')
            print(f"DEBUG: Serper found add business page: {best_url}", flush=True)
            return best_url
        
        print(f"DEBUG: Serper found no add business pages", flush=True)
        return None
        
    except Exception as e:
        print(f"DEBUG: Serper search error: {e}", flush=True)
        return None


def find_add_business_link(html_content, base_url):
    """
    Parse HTML to find "Add Business" or similar links.
    Returns the URL if found, otherwise None.
    """
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Patterns to search for in link text or href - expanded list
    add_patterns = [
        # Direct add patterns
        'add your business',
        'add business',
        'add a business',
        'add listing',
        'add your listing',
        'submit business',
        'submit listing',
        'submit your business',
        'list your business',
        'list business',
        # Claim patterns
        'claim your business',
        'claim business',
        'claim listing',
        'claim your listing',
        # Registration patterns
        'register business',
        'register your business',
        'sign up',
        'signup',
        'create account',
        'create listing',
        'create a listing',
        'join now',
        'join us',
        # Marketing patterns
        'free listing',
        'get listed',
        'advertise with us',
        'for business owners',
        'business login',
        'owner login',
        'business sign',
        # URL patterns
        'add-business',
        'add-listing',
        'submit-business',
        'claim-business',
        'register',
        'signup'
    ]
    
    # Negative patterns to avoid false positives (e.g. newsletter signup, job alerts)
    negative_patterns = [
        'newsletter',
        'subscribe',
        'job alert',
        'job seek',
        'candidate',
        'resume',
        'alert',
        'update',
        'mailing list',
        'email list',
        'notifications',
        'search',
        'find'
    ]
    
    # Search all links
    for a in soup.find_all('a', href=True):
        link_text = a.get_text().lower().strip()
        href = a.get('href', '').lower()
        
        # Skip if matches negative patterns
        if any(neg in link_text or neg in href for neg in negative_patterns):
            continue
        
        for pattern in add_patterns:
            if pattern in link_text or pattern.replace(' ', '-') in href or pattern.replace(' ', '_') in href:
                # Found a match
                full_url = a.get('href')
                if full_url:
                    if full_url.startswith('http'):
                        return full_url
                    else:
                        return urljoin(base_url, full_url)
    
    # Also check buttons
    for btn in soup.find_all(['button', 'input'], attrs={'type': ['submit', 'button']}):
        btn_text = btn.get('value', '') or btn.get_text()
        btn_text = btn_text.lower().strip()
        
        for pattern in add_patterns:
            if pattern in btn_text:
                # Button found but no direct URL - return the current page
                return base_url + " (form on page)"
    
    return None


def extract_form_fields(html_content):
    """
    Extract form field names and labels from HTML.
    Returns a list of field descriptions.
    """
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, 'html.parser')
    fields = []
    
    # Find all form inputs
    for inp in soup.find_all(['input', 'textarea', 'select']):
        field_name = inp.get('name') or inp.get('id') or ''
        field_type = inp.get('type', 'text')
        placeholder = inp.get('placeholder', '')
        required = inp.has_attr('required')
        
        # Skip hidden and submit fields
        if field_type in ['hidden', 'submit', 'button']:
            continue
        
        # Try to find associated label
        label_text = ''
        field_id = inp.get('id')
        if field_id:
            label = soup.find('label', attrs={'for': field_id})
            if label:
                label_text = label.get_text().strip()
        
        # Build field description
        if label_text or field_name or placeholder:
            field_desc = label_text or placeholder or field_name
            if required:
                field_desc += " (required)"
            fields.append(field_desc)
    
    return fields


def analyze_with_gemini(directory_name, homepage_content, add_page_url, add_page_content, form_fields):
    """
    Use Gemini to analyze the scraped content and create a concise guide.
    """
    from google import genai
    
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        print("DEBUG: GEMINI_API_KEY not found", flush=True)
        return None
    
    client = genai.Client(api_key=api_key)
    
    # Prepare context
    form_fields_text = "\n".join(f"- {f}" for f in form_fields) if form_fields else "No form fields detected."
    
    prompt = f"""Analyze this directory website to create a concise "How to Add Business" guide.

DIRECTORY: {directory_name}
ADD BUSINESS URL: {add_page_url if add_page_url else "Not found"}

FORM FIELDS DETECTED:
{form_fields_text}

PAGE CONTENT EXTRACT (Seek "List Your Business" / "Sign Up" actions):
{add_page_content[:2000] if add_page_content else "No content available"}

Based on the URL and detected fields, provide a SHORT guide in this EXACT format:

## 1. Where to Go
- IF a URL is provided, instruct user to click "Go to {directory_name}" button.
- Mention if they need to look for a specific button like "List Your Business" or "Sign Up" on that page (check Content Extract).

## 2. What to Submit
- List only key fields as bullet points (5-7 items max)
- If no fields detected, infer standard fields (Name, Address, Phone, Website) and mention likely "Account Creation" requirement.

## 3. Verification
- Method and time estimate (1-2 lines)
- If unknown, say "Likely requires email confirmation or admin review".

RULES:
- Be CONCISE - each section 2-5 lines max
- Use bullet points, not paragraphs
- Do NOT include generic advice or tips
- CRITICAL: Do NOT wrap URLs in backticks (`), quotes ("), or brackets. Write raw URLs only (e.g. https://example.com/add) to prevent link breaking."""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        return response.text
    except Exception as e:
        print(f"DEBUG: Gemini error: {e}", flush=True)
        return None


def fallback_to_perplexity(directory_name, directory_website):
    """
    Fallback to Perplexity when Camoufox scraping fails.
    """
    try:
        from lib.perplexity_client import perform_research
        
        prompt = f"""Find how to add a business listing on {directory_name} ({directory_website}).

PROVIDE A SHORT, ACTIONABLE GUIDE IN THIS EXACT FORMAT:

## 1. Where to Go
- Provide the EXACT URL to add/claim a business
- If no direct link exists, say which button to click

## 2. What to Submit
List only the key required fields (5-7 items max):
- Business Name
- Phone Number
- Address
- (etc.)

## 3. Verification
- Method and time estimate (1-2 lines)

RULES:
- Be CONCISE - each section 2-5 lines max
- Use bullet points only
- Do NOT include citation numbers"""

        result = perform_research(prompt, model="sonar-pro")
        if result:
            # Strip citation numbers
            import re
            result = re.sub(r'\[\d+\]', '', result)
            return result
        else:
            return f"""## 1. Where to Go
- Visit {directory_website}
- Look for "Add Business", "List Your Business", or "Contact Us"

## 2. What to Submit
- Business Name, Address, Phone, Website, Category

## 3. Verification
- Check the website for verification requirements"""
    except Exception as e:
        print(f"DEBUG: Perplexity fallback error: {e}", flush=True)
        return f"""## 1. Where to Go
- Visit {directory_website}
- Look for "Add Business" or "Claim Listing"

## 2. What to Submit
- Business Name, Address, Phone, Website, Category

## 3. Verification
- Check after submitting for email confirmation"""

def get_add_business_guide(directory_name, directory_website):
    """
    Main function: Scrape directory and generate how-to-add guide.
    
    Strategy:
    1. Serper search for "add business" page (fastest, most reliable)
    2. Camoufox scrape homepage and parse for links
    3. Perplexity fallback if all else fails
    
    Returns: Guide text (markdown formatted)
    """
    print(f"DEBUG: Getting Add Business guide for {directory_name} ({directory_website})", flush=True)
    
    # Ensure website has protocol
    if not directory_website.startswith('http'):
        directory_website = 'https://' + directory_website
        
    homepage_content = None
    
    add_page_url = None
    
    # Step 1: Scrape the homepage (Primary strategy)
    print(f"DEBUG: Scraping homepage first: {directory_website}", flush=True)
    homepage_content = scrape_with_camoufox(directory_website)
    
    if homepage_content:
        # Step 2: Look for Add Business link on homepage
        add_page_url = find_add_business_link(homepage_content, directory_website)
        print(f"DEBUG: Camoufox + HTML parsing found: {add_page_url}", flush=True)
    
    # Step 3: If homepage scraping failed or no link found, use Perplexity
    if not add_page_url:
        print(f"DEBUG: No add page found via scraping, falling back to Perplexity for {directory_name}", flush=True)
        return fallback_to_perplexity(directory_name, directory_website)
    
    print(f"DEBUG: Add Business URL found: {add_page_url}", flush=True)
    
    add_page_content = None
    form_fields = []
    
    # Step 3: If found, scrape the Add Business page
    if add_page_url and not add_page_url.endswith("(form on page)"):
        add_page_content = scrape_with_camoufox(add_page_url)
        if add_page_content:
            form_fields = extract_form_fields(add_page_content)
            print(f"DEBUG: Found {len(form_fields)} form fields", flush=True)
    
    # Step 4: Analyze with Gemini
    guide = analyze_with_gemini(
        directory_name=directory_name,
        homepage_content=homepage_content[:10000] if homepage_content else "",  # Limit content size, handle None
        add_page_url=add_page_url,
        add_page_content=add_page_content[:10000] if add_page_content else None,
        form_fields=form_fields
    )
    
    if guide:
        return guide
    
    # Fallback: Generate basic guide without Gemini
    if add_page_url:
        return f"""## 1. Where to Go
- Go to {add_page_url}

## 2. What to Submit
{chr(10).join('- ' + f for f in form_fields[:7]) if form_fields else '- Check the form on the website'}

## 3. Verification
- Check after submitting for confirmation email
"""
    else:
        return f"""## 1. Where to Go
- No "Add Business" link found on {directory_name}
- Visit {directory_website} and look for "Add Listing" or "Contact Us"

## 2. What to Submit
- Typically: Business Name, Address, Phone, Website, Category

## 3. Verification
- Contact the directory for verification process
"""


if __name__ == "__main__":
    # Test
    result = get_add_business_guide("StartLocal", "https://www.startlocal.com.au")
    print(result)
