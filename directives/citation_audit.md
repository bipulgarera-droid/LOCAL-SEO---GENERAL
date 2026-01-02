# Citation Audit: URL Discovery & Verification

**Goal:** Discover directories where a business is listed and find the specific profile URL for that business.

## Inputs
- `business_name` (str): Name of the business.
- `doctor_name` (str, optional): Name of the practitioner.
- `city` (str): City of the business.
- `domain` (str): Website domain of the business.

## Steps

### 1. Directory Discovery
**Tool:** `execution/discover_directories.py` (To be created)
- Uses Perplexity to find relevant directories (City, State, National).
- Deduplicates based on domain.

### 2. Find Profile URLs
**Tool:** `execution/find_urls.py`
- **Input:** List of directory names/domains.
- **Process:**
    - Uses Perplexity (`sonar-pro`) to search for the specific profile URL.
    - Query: `"{doctor}" "{business}" {city} {directory} profile`
    - **Constraint:** Must return JSON.

### 3. Verify URLs
**Tool:** `execution/verify_url.py`
- **Input:** `url`, `required_text` (Doctor Name or Business Name).
- **Logic:**
    1. **Status Check:**
        - If 404/410: **REJECT** immediately.
        - If 403/429/5xx: Treat as "Blocked" (proceed to fallback).
        - If 200: Proceed to text check.
    2. **Text Verification (Layer 1 & 2):**
        - Try `Jina Reader` (fast).
        - Try `requests` + `BeautifulSoup` (fallback).
        - **Check:** Does page text contain `required_text` (fuzzy match)?
        - If YES: **VERIFY**.
    3. **Slug Verification (Layer 3 - Fallback):**
        - **Condition:** Only run if scraping failed (Blocked) or text missing (JS-rendered), AND status was NOT 404/410.
        - **Check:** Does URL slug contain the sanitized `required_text`?
        - If YES: **VERIFY**.

## Outputs
- JSON object mapping Directory Name -> Verified URL (or "NOT FOUND").
