import os
import sys
import time
import traceback
import json
import requests
import threading
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Add parent directory to path to import gemini_client
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import google.generativeai as genai  # REMOVED: Legacy SDK
# from google import genai as genai_new  # REMOVED: New SDK
# from google.genai import types # REMOVED: New SDK types
from lib import gemini_client # Import our custom client wrapper
import markdown
from lib.webflow_client import webflow_client
from lib.nano_banana_client import nano_banana_client
from lib.dataforseo_client import get_serp_competitors, get_ranked_keywords_for_url
from execution.find_urls import find_urls
import re
from supabase import create_client, Client
from dotenv import load_dotenv
import io
import mimetypes

# Load environment variables from .env
load_dotenv()
# Remove static_folder config entirely to avoid any startup path issues
# We are serving files manually in home() and dashboard()
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Explicitly set template and static folders with absolute paths as requested
template_dir = os.path.join(BASE_DIR, 'public')
static_dir = os.path.join(BASE_DIR, 'public')
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

@app.route('/ping')
def ping():
    return "pong", 200

@app.route('/favicon.ico')
def favicon():
    return "", 204
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0 # Disable cache for development
CORS(app)

# File-based logging for debugging
def log_debug(message):
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        log_path = os.path.join(BASE_DIR, "debug.log")
        with open(log_path, "a") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception as e:
        print(f"Logging failed: {e}", file=sys.stderr)

# Initialize log
# Initialize log
log_debug("Server started/reloaded")

# --- AGGRESSIVE LOGGING START ---
print(f"DEBUG: BASE_DIR is {BASE_DIR}", file=sys.stderr, flush=True)
print(f"DEBUG: template_dir is {template_dir}", file=sys.stderr, flush=True)

try:
    if os.path.exists(template_dir):
        print(f"DEBUG: Listing {template_dir}: {os.listdir(template_dir)}", file=sys.stderr, flush=True)
    else:
        print(f"DEBUG: template_dir does not exist!", file=sys.stderr, flush=True)
except Exception as e:
    print(f"DEBUG: Failed to list template_dir: {e}", file=sys.stderr, flush=True)

@app.before_request
def log_request_info():
    print(f"DEBUG: Request started: {request.method} {request.url}", file=sys.stderr, flush=True)
    # print(f"DEBUG: Headers: {request.headers}", file=sys.stderr, flush=True) # Uncomment if needed

@app.after_request
def log_response_info(response):
    print(f"DEBUG: Request finished: {response.status}", file=sys.stderr, flush=True)
    return response

@app.errorhandler(Exception)
def handle_exception(e):
    print(f"CRITICAL: Unhandled Exception: {str(e)}", file=sys.stderr, flush=True)
    traceback.print_exc()
    return jsonify({"error": "Internal Server Error", "details": str(e)}), 500
# --- AGGRESSIVE LOGGING END ---

@app.route('/api/get-debug-log', methods=['GET'])
def get_debug_log():
    try:
        log_path = os.path.join(BASE_DIR, "debug.log")
        if os.path.exists(log_path):
            with open(log_path, "r") as f:
                # Read last 50 lines
                lines = f.readlines()
                return jsonify({"logs": lines[-50:]}), 200
        return jsonify({"logs": ["Log file not found."]}), 200
    except Exception as e:
        return jsonify({"logs": [f"Error reading log: {str(e)}"]}), 200

import logging
try:
    log_path = os.path.join(BASE_DIR, 'backend.log')
    logging.basicConfig(filename=log_path, level=logging.INFO, 
                        format='%(asctime)s %(levelname)s: %(message)s')
    logger = logging.getLogger()
except Exception as e:
    print(f"Warning: Failed to setup file logging: {e}", file=sys.stderr)
    # Fallback to console logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

# Configure Gemini
# GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# if not GEMINI_API_KEY:
#     # In production, this should ideally log an error or fail gracefully if the key is critical
#     pass 
# genai.configure(api_key=GEMINI_API_KEY) # REMOVED: Legacy SDK Config

# Configure Supabase
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Add delay to allow connection pool to spin up (prevents startup crashes)
time.sleep(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

@app.route('/')
def home():
    try:
        print("DEBUG: Entering home route", file=sys.stderr, flush=True)
        
        # Explicitly check for template existence
        template_path = os.path.join(template_dir, 'agency.html')
        if not os.path.exists(template_path):
            error_msg = f"CRITICAL: Template not found at {template_path}"
            print(error_msg, file=sys.stderr, flush=True)
            return jsonify({"error": "Template not found", "path": template_path}), 500
            
        print(f"DEBUG: Serving template from {template_path}", file=sys.stderr, flush=True)
        return send_from_directory(template_dir, 'agency.html')
        
    except Exception as e:
        print(f"CRITICAL ERROR in home route: {str(e)}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

@app.route('/health')
def health_check():
    print("DEBUG: Health check hit", file=sys.stderr, flush=True)
    return "OK", 200

@app.route('/debug-files')
def debug_files():
    try:
        files = os.listdir(app.static_folder)
        return jsonify({"static_folder": app.static_folder, "files": files})
    except Exception as e:
        return jsonify({"error": str(e), "static_folder": app.static_folder})

@app.route('/generated-images/<path:filename>')
def serve_generated_image(filename):
    return send_from_directory(os.path.join(BASE_DIR, 'public', 'generated_images'), filename)


@app.route('/dashboard')
def dashboard():
    try:
        file_path = os.path.join(BASE_DIR, 'public', 'dashboard.html')
        if not os.path.exists(file_path):
            return f"Error: dashboard.html not found at {file_path}", 404
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        response = app.make_response(content)
        response.headers['Content-Type'] = 'text/html'
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response
    except Exception as e:
        return f"Server Error: {str(e)}", 500



@app.route('/api/test-ai', methods=['POST'])
def test_ai():
    if not os.environ.get("GEMINI_API_KEY"):
        return jsonify({"error": "GEMINI_API_KEY not found"}), 500

    try:
        data = request.get_json()
        topic = data.get('topic', 'SaaS Marketing') if data else 'SaaS Marketing'

        # Using the requested model which is confirmed to be available for this key
        # model = genai.GenerativeModel('gemini-2.5-flash')
        # response = model.generate_content(f"Write a short 1-sentence SEO strategy for '{topic}'.")
        
        generated_text = gemini_client.generate_content(
            prompt=f"Write a short 1-sentence SEO strategy for '{topic}'.",
            model_name="gemini-2.5-flash"
        )
        
        if not generated_text:
             return jsonify({"error": "Gemini generation failed"}), 500
             
        return jsonify({"strategy": generated_text.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- SHARED HELPER: Robust Scraper ---
def fetch_html_robust(url):
    """
    Fetches HTML using a 2-Layer Strategy:
    1. Requests Session with Chrome Headers (Stealth)
    2. Curl Fallback (if 403/429)
    Returns: (content_bytes, status_code, final_url)
    """
    logging.info(f"DEBUG: fetch_html_robust called for {url}")
    
    # Layer 1: Requests Session (Stealth)
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/'
    }
    
    content = None
    status_code = 0
    final_url = url
    
    try:
        response = session.get(url, headers=headers, timeout=15, allow_redirects=True)
        status_code = response.status_code
        final_url = response.url
        content = response.content
        logging.info(f"DEBUG: Layer 1 (Requests) Status: {status_code}")
        
        if status_code in [403, 429, 503]:
            raise Exception(f"Blocked (Status {status_code})")
            
    except Exception as e:
        logging.info(f"DEBUG: Layer 1 failed: {e}. Trying Layer 2 (Curl)...")
        # Layer 2: Curl Fallback
        try:
            # Use curl to bypass some TLS fingerprinting issues
            cmd = [
                'curl', '-L', # Follow redirects
                '-A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                '--max-time', '15',
                url
            ]
            result = subprocess.run(cmd, capture_output=True, text=False) # Get bytes
            if result.returncode == 0 and result.stdout:
                content = result.stdout
                status_code = 200
                logging.info("DEBUG: Layer 2 (Curl) successful")
            else:
                logging.info(f"DEBUG: Layer 2 (Curl) failed: {result.stderr.decode('utf-8', errors='ignore')}")
        except Exception as curl_e:
            logging.info(f"DEBUG: Layer 2 (Curl) Exception: {curl_e}")
            
    return content, status_code, final_url

@app.route('/api/start-audit', methods=['POST'])
def start_audit():
    print("DEBUG: AUDIT FIX APPLIED - STARTING REQUEST")
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500

    try:
        data = request.get_json()
        page_id = data.get('page_id')
        
        if not page_id:
            return jsonify({"error": "page_id is required"}), 400
        
        # 1. Get the page
        page_res = supabase.table('pages').select('*').eq('id', page_id).execute()
        if not page_res.data:
            return jsonify({"error": "Page not found"}), 404
        
        page = page_res.data[0]
        target_url = page['url']
        
        print(f"DEBUG: Starting Tech Audit for {target_url}")
        
        # 2. Update status to PROCESSING
        supabase.table('pages').update({"audit_status": "Processing"}).eq('id', page_id).execute()
        
        # 3. Perform Tech Audit
        audit_data = {
            "status_code": None,
            "load_time_ms": 0,
            "title": None,
            "meta_description": None,
            "h1": None,
            "word_count": 0,
            "internal_links_count": 0,
            "broken_links": []
        }
        
        try:
            start_time = time.time()
            # Use Robust Scraper Helper
            content, status_code, final_url = fetch_html_robust(target_url)
            
            audit_data["load_time_ms"] = int((time.time() - start_time) * 1000)
            audit_data["status_code"] = status_code
            
            if status_code == 200 and content:
                soup = BeautifulSoup(content, 'html.parser')
                
                # Title
                audit_data["title"] = soup.title.string.strip() if soup.title else None
                
                # Meta Description
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                if meta_desc:
                    audit_data["meta_description"] = meta_desc.get('content', '').strip()
                
                # H1
                h1 = soup.find('h1')
                audit_data["h1"] = h1.get_text().strip() if h1 else None

                # Open Graph Tags
                og_title = soup.find('meta', attrs={'property': 'og:title'})
                audit_data["og_title"] = og_title.get('content', '').strip() if og_title else None
                
                og_desc = soup.find('meta', attrs={'property': 'og:description'})
                audit_data["og_description"] = og_desc.get('content', '').strip() if og_desc else None
                
                # Word Count (rough estimate)
                text = soup.get_text(separator=' ')
                words = [w for w in text.split() if len(w) > 2]
                audit_data["word_count"] = len(words)
                
                # Internal Links
                links = soup.find_all('a', href=True)
                audit_data["internal_links_count"] = len(links)

                # Canonical
                canonical = soup.find('link', attrs={'rel': 'canonical'})
                if canonical:
                    audit_data["canonical"] = canonical.get('href', '').strip()
                
                # Click Depth (Estimated based on URL path segments)
                import urllib.parse
                path = urllib.parse.urlparse(target_url).path
                # Root / is depth 0 or 1. Let's say root is 0.
                segments = [x for x in path.split('/') if x]
                audit_data["click_depth"] = len(segments)

                # --- On-Page Analysis ---
                score = 100
                checks = []
                
                # Title Analysis
                title = audit_data.get("title")
                if not title:
                    score -= 20
                    checks.append("Missing Title")
                    audit_data["title_length"] = 0
                else:
                    t_len = len(title)
                    audit_data["title_length"] = t_len
                    if t_len < 10: 
                        score -= 10
                        checks.append("Title too short")
                    elif t_len > 60:
                        score -= 10
                        checks.append("Title too long")

                # Meta Description Analysis
                desc = audit_data.get("meta_description")
                if not desc:
                    score -= 20
                    checks.append("Missing Meta Desc")
                    audit_data["description_length"] = 0
                else:
                    d_len = len(desc)
                    audit_data["description_length"] = d_len
                    if d_len < 50:
                        score -= 5
                        checks.append("Desc too short")
                    elif d_len > 160:
                        score -= 5
                        checks.append("Desc too long")

                # H1 Analysis
                h1 = audit_data.get("h1")
                if not h1:
                    score -= 20
                    checks.append("Missing H1")
                    audit_data["missing_h1"] = True
                else:
                    audit_data["missing_h1"] = False

                # OG Checks
                if not audit_data.get("og_title"):
                    checks.append("Missing OG Title")
                if not audit_data.get("og_description"):
                    checks.append("Missing OG Desc")
                
                # Image Alt Analysis
                images = soup.find_all('img')
                missing_alt = [img for img in images if not img.get('alt')]
                audit_data["missing_alt_count"] = len(missing_alt)
                
                if missing_alt:
                    score -= 10
                    checks.append(f"{len(missing_alt)} Images missing Alt")
                
                # --- Technical Issues Checks ---
                # Check for redirects by comparing URLs
                if final_url != target_url and final_url != target_url + '/':
                    audit_data["is_redirect"] = True
                else:
                    audit_data["is_redirect"] = 300 <= status_code < 400

                status = audit_data["status_code"]
                audit_data["is_4xx_code"] = 400 <= status < 500
                audit_data["is_5xx_code"] = 500 <= status < 600
                audit_data["high_loading_time"] = audit_data["load_time_ms"] > 2000
                
                # Advanced Checks
                audit_data["redirect_chain"] = False # Simplified for robust scraper
                
                canonical = audit_data.get("canonical")
                if canonical and canonical != target_url:
                    audit_data["canonical_mismatch"] = True
                else:
                    audit_data["canonical_mismatch"] = False
                    
                audit_data["is_orphan_page"] = False # Placeholder: Requires full link graph
                
                # Final Checks
                audit_data["is_broken"] = status >= 400 or status == 0
                
                # Schema / Microdata Check
                has_json_ld = soup.find('script', type='application/ld+json') is not None
                has_microdata = soup.find(attrs={'itemscope': True}) is not None
                audit_data["has_schema"] = has_json_ld or has_microdata
                
                # Duplicate Checks (Query DB)
                try:
                    if title:
                        dup_title = supabase.table('pages').select('id', count='exact').eq('title', title).neq('id', page_id).execute()
                        audit_data["duplicate_title"] = dup_title.count > 0
                    else:
                        audit_data["duplicate_title"] = False
                        
                    if desc:
                        dup_desc = supabase.table('pages').select('id', count='exact').eq('meta_description', desc).neq('id', page_id).execute()
                        audit_data["duplicate_desc"] = dup_desc.count > 0
                    else:
                        audit_data["duplicate_desc"] = False
                except Exception as e:
                    print(f"Duplicate Check Error: {e}")
                    audit_data["duplicate_title"] = False
                    audit_data["duplicate_desc"] = False

                audit_data["onpage_score"] = max(0, score)
                audit_data["checks"] = checks

            else:
                print(f"Audit Failed: Status {status_code}")
                audit_data["error"] = f"HTTP {status_code}"
                audit_data["onpage_score"] = 0
                
        except Exception as e:
            print(f"Audit Error: {e}")
            audit_data["error"] = str(e)
            audit_data["status_code"] = 0 # Indicate failure
    
    # 4. Save Results (Merge with existing)
        current_tech_data = page.get('tech_audit_data') or {}
        current_tech_data.update(audit_data)
        
        update_payload = {
            "audit_status": "Analyzed",
            "tech_audit_data": current_tech_data,
            # Also update core fields if found
            "title": audit_data.get("title") or page.get("title"),
            "meta_description": audit_data.get("meta_description") or page.get("meta_description"),
            "h1": audit_data.get("h1") or page.get("h1")
        }
        
        print(f"DEBUG: Updating DB for page {page_id}")
        print(f"DEBUG: Payload: {json.dumps(update_payload, default=str)[:500]}...") # Print first 500 chars
        
        res = supabase.table('pages').update(update_payload).eq('id', page_id).execute()
        print(f"DEBUG: DB Update Result: {res}")
        
        return jsonify({
            "message": "Tech audit completed",
            "data": audit_data
        })

    except Exception as e:
        print(f"ERROR in start_audit: {str(e)}")
        import traceback
        traceback.print_exc()
        supabase.table('pages').update({"audit_status": "Failed"}).eq('id', page_id).execute()
        return jsonify({"error": str(e)}), 500

        return jsonify({"error": str(e)}), 500

@app.route('/api/analyze-speed', methods=['POST'])
def analyze_speed():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        page_id = data.get('page_id')
        strategy = data.get('strategy', 'mobile') # mobile or desktop
        
        if not page_id: return jsonify({"error": "page_id required"}), 400
        
        # Fetch Page
        page_res = supabase.table('pages').select('url, tech_audit_data').eq('id', page_id).single().execute()
        if not page_res.data: return jsonify({"error": "Page not found"}), 404
        page = page_res.data
        url = page['url']
        
        print(f"Running PageSpeed ({strategy}) for {url}...")
        
        # Call Google PageSpeed Insights API
        psi_key = os.environ.get("PAGESPEED_API_KEY")
        psi_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={url}&strategy={strategy}"
        if psi_key:
            psi_url += f"&key={psi_key}"
            
        psi_res = requests.get(psi_url, timeout=120)
        
        if psi_res.status_code != 200:
            return jsonify({"error": f"PSI API Failed: {psi_res.text}"}), 400
            
        psi_data = psi_res.json()
        
        # Extract Metrics
        lighthouse = psi_data.get('lighthouseResult', {})
        audits = lighthouse.get('audits', {})
        categories = lighthouse.get('categories', {})
        
        score = categories.get('performance', {}).get('score', 0) * 100
        fcp = audits.get('first-contentful-paint', {}).get('displayValue')
        lcp = audits.get('largest-contentful-paint', {}).get('displayValue')
        cls = audits.get('cumulative-layout-shift', {}).get('displayValue')
        tti = audits.get('interactive', {}).get('displayValue')
        
        # Update DB
        current_data = page.get('tech_audit_data') or {}
        speed_data = current_data.get('speed', {})
        speed_data[strategy] = {
            "score": score,
            "fcp": fcp,
            "lcp": lcp,
            "cls": cls,
            "tti": tti,
            "last_run": int(time.time())
        }
        current_data['speed'] = speed_data
        
        supabase.table('pages').update({"tech_audit_data": current_data}).eq('id', page_id).execute()
        
        return jsonify({
            "message": "Speed analysis complete",
            "data": speed_data[strategy]
        })
        
    except Exception as e:
        print(f"Speed Audit Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/update-page-status', methods=['POST'])
def update_page_status():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        page_id = data.get('page_id')
        updates = {}
        
        if 'funnel_stage' in data:
            updates['funnel_stage'] = data['funnel_stage']
            
        if 'page_type' in data:
            updates['page_type'] = data['page_type']
            
            # Auto-fetch title if classifying as Product and title is missing
            if data['page_type'] == 'Product':
                try:
                    # Get current page data
                    page_res = supabase.table('pages').select('url, tech_audit_data').eq('id', page_id).execute()
                    if page_res.data:
                        page = page_res.data[0]
                        tech_data = page.get('tech_audit_data') or {}
                        
                        if not tech_data.get('title') or tech_data.get('title') == 'Untitled Product':
                            print(f"Auto-fetching title for {page['url']}...")
                            try:
                                resp = requests.get(page['url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                                if resp.status_code == 200:
                                    soup = BeautifulSoup(resp.content, 'html.parser')
                                    if soup.title and soup.title.string:
                                        raw_title = soup.title.string.strip()
                                        new_title = clean_title(raw_title)
                                        tech_data['title'] = new_title
                                        updates['tech_audit_data'] = tech_data
                                        print(f"Fetched title: {new_title}")
                            except Exception as scrape_err:
                                print(f"Scrape failed: {scrape_err}")
                except Exception as e:
                    print(f"Auto-fetch error: {e}")

        if 'approval_status' in data:
            updates['approval_status'] = data['approval_status']
            
        if not updates:
            return jsonify({"error": "No updates provided"}), 400
            
        supabase.table('pages').update(updates).eq('id', page_id).execute()
        return jsonify({"message": "Page updated successfully", "updates": updates})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

import requests
from urllib.parse import urlparse

# ... (existing imports)

# Configure DataForSEO
DATAFORSEO_LOGIN = os.environ.get("DATAFORSEO_LOGIN")
DATAFORSEO_PASSWORD = os.environ.get("DATAFORSEO_PASSWORD")

def get_ranking_keywords(target_url):
    if not DATAFORSEO_LOGIN or not DATAFORSEO_PASSWORD:
        print("DataForSEO credentials missing.")
        return []

    try:
        # Clean URL to get domain (DataForSEO prefers domain without protocol)
        parsed = urlparse(target_url)
        domain = parsed.netloc if parsed.netloc else parsed.path
        if domain.startswith("www."):
            domain = domain[4:]
        
        # Normalize the target URL for comparison (remove protocol, www, trailing slash)
        normalized_target = target_url.lower().replace('https://', '').replace('http://', '').replace('www.', '').rstrip('/')
        
        print(f"DEBUG: Looking for keywords for normalized URL: {normalized_target}")

        url = "https://api.dataforseo.com/v3/dataforseo_labs/google/ranked_keywords/live"
        payload = [
            {
                "target": domain,
                "location_code": 2840, # US
                "language_code": "en",
                "filters": [
                    ["ranked_serp_element.serp_item.rank_absolute", ">=", 1],
                    "and",
                    ["ranked_serp_element.serp_item.rank_absolute", "<=", 10]
                ],
                "order_by": ["keyword_data.keyword_info.search_volume,desc"],
                "limit": 100  # Get more results to filter
            }
        ]
        headers = {
            'content-type': 'application/json'
        }

        response = requests.post(url, json=payload, auth=(DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD), headers=headers)
        response.raise_for_status()
        data = response.json()

        page_keywords = []
        domain_keywords = []
        
        if data['tasks'] and data['tasks'][0]['result'] and data['tasks'][0]['result'][0]['items']:
            for item in data['tasks'][0]['result'][0]['items']:
                keyword = item['keyword_data']['keyword']
                volume = item['keyword_data']['keyword_info']['search_volume']
                
                # Get the ranking URL for this keyword
                ranking_url = item.get('ranked_serp_element', {}).get('serp_item', {}).get('url', '')
                # Normalize ranking URL the same way
                normalized_ranking = ranking_url.lower().replace('https://', '').replace('http://', '').replace('www.', '').rstrip('/')
                
                # Check if this keyword ranks for the specific page
                if normalized_ranking == normalized_target:
                    page_keywords.append(f"{keyword} (Vol: {volume})")
                    print(f"DEBUG: ✓ Page match: {keyword} ranks for {normalized_ranking}")
                elif normalized_ranking.startswith(domain):
                    domain_keywords.append(f"{keyword} (Vol: {volume})")
        
        # If we found page-specific keywords, return those (up to 5)
        if page_keywords:
            print(f"DEBUG: ✓ Found {len(page_keywords)} page-specific keywords for {target_url}")
            return page_keywords[:5]
        
        # Otherwise, return only 3 domain keywords as fallback
        if domain_keywords:
            print(f"DEBUG: ⚠ No page-specific keywords found. Using 3 domain-level keywords as fallback")
            return domain_keywords[:3]
        
        print(f"DEBUG: ✗ No keywords found at all for {target_url}")
        return []

    except Exception as e:
        print(f"DataForSEO Error: {e}")
        import traceback
        traceback.print_exc()
        return []

        return []

def verify_url_content(url, required_text):
    """
    Fetches a URL and checks if the required_text (e.g., doctor name) is present.
    Returns True if present, False otherwise.
    """
    if not url or not required_text:
        return False
        
    # Normalize required text (remove "Dr." etc)
    clean_req = required_text.lower().replace('dr.', '').replace('dr ', '').strip()
    name_parts = clean_req.split()
    
    def check_text(content):
        content = content.lower()
        if clean_req in content:
            return True
        # Check if all parts of the name are present (e.g. "Andrew" and "Jacono")
        if all(part in content for part in name_parts if len(part) > 2):
            return True
        return False

    # 1. Try Jina Reader first (fastest for text)
    try:
        jina_url = f"https://r.jina.ai/{url}"
        jina_response = requests.get(jina_url, timeout=5, headers={"Accept": "text/markdown"})
        if jina_response.status_code == 200:
            if check_text(jina_response.text):
                print(f"DEBUG: VERIFIED (Jina) {url} for {required_text}", flush=True)
                return True
            else:
                print(f"DEBUG: FAILED (Jina) {url}: Text not found", flush=True)
        else:
            print(f"DEBUG: FAILED (Jina) {url}: Status {jina_response.status_code}", flush=True)
    except Exception as e:
        print(f"DEBUG: Jina failed for {url}: {e}", flush=True)

    # 2. Fallback to Requests/BS4
    request_failed_but_not_404 = False
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            if check_text(resp.text):
                print(f"DEBUG: VERIFIED (Requests) {url} for {required_text}", flush=True)
                return True
            else:
                print(f"DEBUG: FAILED (Requests) {url}: Text not found", flush=True)
                # Page loaded but text missing -> could be JS rendered. Allow Slug Match.
                request_failed_but_not_404 = True
        elif resp.status_code in [404, 410]:
            print(f"DEBUG: REJECTED (Requests) {url}: Status {resp.status_code} (Broken Link)", flush=True)
            return False # DEAD LINK - DO NOT FALLBACK
        elif resp.status_code in [403, 429, 500, 502, 503]:
            print(f"DEBUG: FAILED (Requests) {url}: Status {resp.status_code} (Likely Blocked)", flush=True)
            request_failed_but_not_404 = True # Blocked, so try Slug Match
        else:
            request_failed_but_not_404 = True
            
    except Exception as e:
        print(f"DEBUG: Requests failed for {url}: {e}", flush=True)
        request_failed_but_not_404 = True

    # 3. Last Resort: Check URL Slug
    # ONLY run this if the page was NOT a 404/410.
    # Useful for: 403 Blocked, JS-rendered pages (200 but text missing), or Timeouts.
    if request_failed_but_not_404:
        try:
            slug = url.lower().split('?')[0] # Remove query params
            # Check for full name match in slug
            if clean_req.replace(' ', '-') in slug or clean_req.replace(' ', '') in slug:
                 print(f"DEBUG: VERIFIED (Slug Match) {url} for {required_text}", flush=True)
                 return True
            
            # Check parts in slug (e.g. "andrew" and "jacono" both present)
            if all(part in slug for part in name_parts if len(part) > 2):
                 print(f"DEBUG: VERIFIED (Slug Parts Match) {url} for {required_text}", flush=True)
                 return True
        except Exception as e:
            print(f"DEBUG: Slug check failed for {url}: {e}", flush=True)
                
    return False

@app.route('/api/citation-audit/verify-urls', methods=['POST'])
def citation_audit_verify_urls():
    """
    Step 1.5: Verify/correct homepage URLs using Gemini grounding with Google Search.
    Takes directory names from Step 1 and finds their correct homepage URLs.
    """
    try:
        data = request.get_json()
        project_id = data.get('project_id')
        
        if not project_id:
            return jsonify({"error": "Project ID required"}), 400
        
        # Fetch all directories for this project that need URL verification
        rows = supabase.table('citation_audits').select('id, directory_name, directory_website').eq('project_id', project_id).execute().data
        
        if not rows:
            return jsonify({"message": "No directories found"}), 200
        
        log_debug(f"Verify URLs: Processing {len(rows)} directories")
        
        # Use Gemini with Google Search grounding to verify URLs
        # Process in batches of 10 to manage token limits
        batch_size = 10
        updated_count = 0
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            
            # Create search query for Gemini
            directory_names = [r['directory_name'] for r in batch]
            
            prompt = f"""You are a web search expert. For each of these business directories, find their OFFICIAL homepage URL.

DIRECTORIES TO FIND:
{chr(10).join([f"- {name}" for name in directory_names])}

TASK: Search Google for each directory name and return its official website URL.

EXAMPLES of correct results:
- "Healthgrades" → "https://www.healthgrades.com"
- "Yelp" → "https://www.yelp.com"
- "ZocDoc" → "https://www.zocdoc.com"
- "American Dental Association Find a Dentist" → "https://findadentist.ada.org" OR "https://www.ada.org"
- "Academy of General Dentistry" → "https://www.agd.org"
- "Better Business Bureau" → "https://www.bbb.org"

CRITICAL RULES:
1. Return ONLY the main homepage domain (e.g., https://healthgrades.com NOT a search page)
2. NEVER return Google search URLs or Google redirects
3. NEVER return image URLs (.jpg, .png, .webp, .gif)
4. NEVER return random/unrelated websites
5. If you cannot find the official URL with confidence, return empty string ""
6. For associations like "American Academy of X", find their official .org domain

Return ONLY valid JSON:
{{
    "results": [
        {{"name": "Directory Name", "homepage_url": "https://correct-url.com"}},
        ...
    ]
}}"""
            
            try:
                # Use gemini_client wrapper which handles grounding correcty via REST API
                result_text = gemini_client.generate_content(
                    prompt,
                    model_name="gemini-2.0-flash",
                    temperature=0.1,
                    use_grounding=True
                )
                
                if not result_text:
                    log_debug("Verify URLs: Gemini returned no content")
                    continue
                
                result_text = result_text.strip()
                
                # Parse JSON
                if result_text.startswith('```json'):
                    result_text = result_text[7:]
                if result_text.startswith('```'):
                    result_text = result_text[3:]
                if result_text.endswith('```'):
                    result_text = result_text[:-3]
                
                results = json.loads(result_text.strip())
                url_map = {r['name'].lower().strip(): r['homepage_url'] for r in results.get('results', []) if r.get('homepage_url')}
                
                # Helper function to validate URLs
                def is_valid_homepage_url(url):
                    if not url:
                        return False
                    url_lower = url.lower()
                    # Reject search result URLs
                    if 'google.com/search' in url_lower or 'google.com/url' in url_lower:
                        return False
                    # Reject image URLs
                    if url_lower.endswith(('.webp', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.ico')):
                        return False
                    # Reject cached/preview URLs
                    if 'cache:' in url_lower or 'webcache' in url_lower:
                        return False
                    # Must start with http
                    if not url_lower.startswith(('http://', 'https://')):
                        return False
                    return True
                
                # Update database with corrected URLs
                for row in batch:
                    name = row['directory_name'].lower().strip()
                    if name in url_map and url_map[name]:
                        new_url = url_map[name]
                        if is_valid_homepage_url(new_url) and new_url != row.get('directory_website', ''):
                            supabase.table('citation_audits').update({
                                'directory_website': new_url
                            }).eq('id', row['id']).execute()
                            updated_count += 1
                            log_debug(f"Updated URL: {row['directory_name']} -> {new_url}")
                
            except Exception as e:
                log_debug(f"Batch verify failed: {e}")
                continue
        
        log_debug(f"Verify URLs complete: {updated_count} URLs updated")
        
        return jsonify({
            "success": True,
            "total": len(rows),
            "updated": updated_count,
            "message": f"Verified {len(rows)} directories, updated {updated_count} URLs"
        })
        
    except Exception as e:
        log_debug(f"Verify URLs error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/citation-audit/find-urls', methods=['POST'])
def citation_audit_find_urls():
    """
    Step 2: Find profile URLs for each directory using Google Custom Search.
    Uses execution script for logic.
    """
    from execution.discover_profile_url import discover_profile_url
    from urllib.parse import urlparse
    
    try:
        data = request.get_json()
        project_id = data.get('project_id')
        
        if not project_id:
            return jsonify({"error": "Project ID required"}), 400
            
        # Fetch Project Data - check medical_projects first, then projects
        proj = None
        business_name = ''
        doctor_name = ''
        city = ''
        state = ''
        country = ''
        
        # Try medical_projects first
        try:
            project_res = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                proj = project_res.data
                business_name = proj.get('business_name', '')
                doctor_name = ''  # medical_projects doesn't have doctor_name
                location_parts = proj.get('location', '').split(',') if proj.get('location') else []
                city = location_parts[0].strip() if location_parts else ''
                state = location_parts[1].strip() if len(location_parts) > 1 else ''
                log_debug(f"Step 2: Found medical_project {business_name}")
        except:
            pass
        
        # Fall back to projects table
        if not proj:
            project_res = supabase.table('projects').select('*').eq('id', project_id).single().execute()
            if not project_res.data:
                return jsonify({"error": "Project not found"}), 404
            proj = project_res.data
            business_name = proj.get('project_name', '')
            doctor_name = proj.get('doctor_name', '')
            city = proj.get('city', '')
            state = proj.get('state', '')
            country = proj.get('country', '') # Explicit country field
        
        # Extract country for medical_projects if needed
        if not country and proj.get('location'):
             parts = proj.get('location').split(',')
             if len(parts) > 2:
                 country = parts[2].strip()
        
        # Fallback country
        if not country:
            country = "United States"
        
        # Fetch Directories - only pending ones, limit to 20 for batch
        rows = supabase.table('citation_audits').select('*').eq('project_id', project_id).eq('status', 'pending').order('created_at').limit(100).execute().data
        
        if not rows:
            return jsonify({"message": "No pending directories to process (all done or none found)"}), 200
            
        log_debug(f"Step 2: Processing {len(rows)} directories for {city}, {country}")
        
        # Process each directory
        processed_count = 0
        found_count = 0
        results = []
        
        for row in rows:
            directory_name = row['directory_name']
            directory_website = row.get('directory_website', '')
            
            # Extract domain from website URL
            if directory_website:
                try:
                    parsed = urlparse(directory_website)
                    directory_domain = parsed.netloc.replace('www.', '')
                except:
                    directory_domain = directory_website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
            else:
                # Fallback: try to guess domain from name
                directory_domain = directory_name.lower().replace(' ', '') + '.com'
            
            # Call the execution script function
            # Note: discover_profile_url is imported from the script
            result = discover_profile_url(
                directory_name=directory_name,
                directory_domain=directory_domain,
                business_name=business_name,
                doctor_name=doctor_name,
                city=city,
                state=state,
                country=country
            )
            
            # Only save URL if status is "found"
            candidates = result.get('candidates', [])
            best_url = candidates[0]['url'] if candidates else ''
            
            if result.get('status') == 'found' and best_url:
                update_data = {
                    "profile_url": best_url,
                    "status": "found"
                }
                found_count += 1
                
                # Update result object for response
                result_item = {
                    "directory": directory_name,
                    "status": "found",
                    "url": best_url,
                    "match_reason": result.get('match_reason', '')
                }
            else:
                update_data = {
                    "status": "not_found"
                }
                result_item = {
                    "directory": directory_name,
                    "status": "not_found",
                    "url": "",
                    "match_reason": result.get('match_reason', '')
                }
            
            supabase.table('citation_audits').update(update_data).eq('id', row['id']).execute()
            processed_count += 1
            
            results.append(result_item)
            
        log_debug(f"Step 2 Complete: Processed {processed_count}, Found {found_count}")
        
        return jsonify({
            "success": True,
            "processed": processed_count,
            "found": found_count,
            "results": results
        })

    except Exception as e:
        log_debug(f"Step 2 Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/refresh-directory', methods=['POST'])
def citation_audit_refresh_directory():
    """
    Refresh a single directory: Re-run URL discovery for ONE specific listing.
    Used for testing refined search logic on specific cases.
    """
    from execution.discover_profile_url import discover_profile_url
    from urllib.parse import urlparse
    
    try:
        data = request.get_json()
        audit_id = data.get('audit_id')
        
        if not audit_id:
            return jsonify({"error": "audit_id required"}), 400
        
        # Get the audit row
        audit_res = supabase.table('citation_audits').select('*').eq('id', audit_id).single().execute()
        if not audit_res.data:
            return jsonify({"error": "Audit not found"}), 404
        
        audit = audit_res.data
        project_id = audit.get('project_id')
        directory_name = audit.get('directory_name')
        directory_website = audit.get('directory_website', '')
        how_to_add_guide = audit.get('how_to_add_guide', '')
        
        # Get project details for search context
        proj = None
        business_name = ''
        doctor_name = ''
        city = ''
        state = ''
        country = ''
        
        try:
            project_res = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                proj = project_res.data
                business_name = proj.get('business_name', '')
                location_parts = proj.get('location', '').split(',') if proj.get('location') else []
                city = location_parts[0].strip() if location_parts else ''
                state = location_parts[1].strip() if len(location_parts) > 1 else ''
                country = location_parts[2].strip() if len(location_parts) > 2 else ''
        except:
            pass
        
        if not proj:
            project_res = supabase.table('projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                proj = project_res.data
                business_name = proj.get('project_name', '')
                doctor_name = proj.get('doctor_name', '')
                city = proj.get('city', '')
                state = proj.get('state', '')
                country = proj.get('country', '')
        
        if not country:
            country = "United States"
        
        # Extract domain
        if directory_website:
            try:
                parsed = urlparse(directory_website)
                directory_domain = parsed.netloc.replace('www.', '')
            except:
                directory_domain = directory_website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
        else:
            directory_domain = directory_name.lower().replace(' ', '') + '.com'
        
        # ALWAYS force discovery on refresh (User request: "do the whole process from again from the top")
        existing_url = audit.get('profile_url', '')
        
        # Determine if we should clear the existing URL first? 
        # Ideally, discover_profile_url will return a new one or the same one.
        # If it returns empty, we should likely update to "not_found"
        
        log_debug(f"REFRESH: Forced Discovery for {directory_name} ({directory_domain}) - {business_name}, {city}, {country}")
        
        # Call discover_profile_url
        result = discover_profile_url(
            directory_name=directory_name,
            directory_domain=directory_domain,
            business_name=business_name,
            doctor_name=doctor_name,
            city=city,
            state=state,
            country=country
        )
        url_discovery_needed = True # Since we just ran discovery, we treat it as new
            
        # Prepare for Verification (Loop through candidates)
        candidates = result.get('candidates', [])
        
        if not candidates:
            # No candidates found
            log_debug(f"REFRESH complete: {directory_name} -> not_found (no candidates)")
            
            # Update DB to not_found
            update_data = {
                "profile_url": "",
                "status": "not_found",
                "nap_name_ok": None,
                "nap_address_ok": None,
                "nap_phone_ok": None,
                "nap_website_ok": None
            }
            supabase.table('citation_audits').update(update_data).eq('id', audit_id).execute()
            
            return jsonify({
                "success": True,
                "directory": directory_name,
                "directory_website": directory_website,
                "how_to_add_guide": how_to_add_guide,
                "status": "not_found",
                "url": "",
                "match_reason": "no_candidates",
                "nap_details": "No search candidates found." # Use this for Details column
            })

        # We have candidates. Let's verify them to pick the winner.
        from execution.citation_audit_verify_nap import verify_nap
        
        # Prepare Verification Data
        expected_phone = proj.get('phone', '')
        expected_website = proj.get('website', '')
        expected_address = proj.get('address', '') or f"{city}, {state}".strip(", ")
        address_parts = [p.strip() for p in expected_address.split(',') if p.strip()]
        
        best_candidate = None
        best_nap_result = None
        
        log_debug(f"REFRESH: Verifying {len(candidates)} candidates...")
        
        for cand in candidates:
            cand_url = cand['url']
            log_debug(f"REFRESH: Verifying candidate {cand_url}")
            
            nap_res = verify_nap(
                url=cand_url,
                business_name=business_name,
                phone=expected_phone,
                address_parts=address_parts,
                website_url=expected_website
            )
            
            # Check Quality
            is_strong_match = False
            if nap_res.get('nap_phone_ok'): # Phone match is gold
                is_strong_match = True
            elif nap_res.get('nap_name_ok') and nap_res.get('confidence', 0) > 75:
                is_strong_match = True
            
            if is_strong_match:
                best_candidate = cand
                best_nap_result = nap_res
                log_debug("REFRESH: Strong match found!")
                break
            
            # Keep the first one as fallback if no strong match found yet
            if best_nap_result is None:
                best_nap_result = nap_res
                best_candidate = cand
        
        # We now have a best_candidate (either strong match or just the first one)
        final_url = best_candidate['url']
        final_nap = best_nap_result
        
        # Save to DB
        nap_update = {
            "profile_url": final_url,
            "status": "verified", # Or 'found' if validation weak? Let's use 'verified' as it means 'processed'
            "nap_name_ok": final_nap.get('nap_name_ok'),
            "nap_address_ok": final_nap.get('nap_address_ok'),
            "nap_phone_ok": final_nap.get('nap_phone_ok'),
            "nap_website_ok": final_nap.get('nap_website_ok'),
            "nap_details": final_nap.get('details', '')
        }
        supabase.table('citation_audits').update(nap_update).eq('id', audit_id).execute()
        
        log_debug(f"REFRESH complete: {directory_name} -> verified ({final_url})")
        
        return jsonify({
            "success": True,
            "directory": directory_name,
            "directory_website": directory_website,
            "how_to_add_guide": how_to_add_guide,
            "status": "verified",
            "url": final_url,
            "nap_name_ok": final_nap.get('nap_name_ok'),
            "nap_address_ok": final_nap.get('nap_address_ok'),
            "nap_phone_ok": final_nap.get('nap_phone_ok'),
            "nap_website_ok": final_nap.get('nap_website_ok'),
            "nap_details": final_nap.get('details', '')
        })
        
    except Exception as e:
        log_debug(f"Refresh Directory Error: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/verify-nap', methods=['POST'])
def citation_audit_verify_nap():
    """
    Step 3: Use Jina Reader to crawl each found URL and verify NAP accuracy.
    Uses execution script for logic.
    """
    from execution.citation_audit_verify_nap import verify_nap
    
    try:
        data = request.get_json()
        audit_id = data.get('audit_id', '')
        project_id = data.get('project_id', '')
        
        if not audit_id and not project_id:
            return jsonify({"error": "audit_id or project_id is required"}), 400
        
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        # Get found directories
        if project_id:
            result = supabase.table('citation_audits').select('*').eq('project_id', project_id).eq('status', 'found').execute()
        else:
            result = supabase.table('citation_audits').select('*').eq('audit_id', audit_id).eq('status', 'found').execute()
        rows = result.data
        
        if not rows:
            return jsonify({"error": "No found listings to verify"}), 404
        
        # Get project details
        project_id = rows[0].get('project_id')
        if project_id:
            # Try medical_projects first
            try:
                project_res = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
                if project_res.data:
                    project = project_res.data
                    business_name = project.get('business_name', '')
                    location_parts = project.get('location', '').split(',') if project.get('location') else []
                    city = location_parts[0].strip() if location_parts else ''
                    state = location_parts[1].strip() if len(location_parts) > 1 else ''
                    street_address = project.get('address', '')
                    zip_code = '' # medical_projects might not split zip
                    phone = project.get('phone', '')
                    website_url = project.get('website', '')
                    full_address = f"{street_address}, {city}, {state}".strip(", ")
                else:
                     raise Exception("Not in medical_projects")
            except:
                # Fallback to projects
                project_res = supabase.table('projects').select('*').eq('id', project_id).single().execute()
                if project_res.data:
                    project = project_res.data
                    business_name = project.get('project_name', '')
                    city = project.get('city', '') or ''
                    state = project.get('state', '') or ''
                    street_address = project.get('street_address', '')
                    zip_code = project.get('zip_code', '')
                    phone = project.get('phone', '')
                    website_url = project.get('website', '')
                    full_address = f"{street_address}, {city}, {state} {zip_code}".strip(", ")
                else:
                    business_name, full_address, phone, website_url = '', '', '', ''
        else:
            business_name, full_address, phone, website_url = '', '', '', ''

        
        log_debug(f"Citation Audit Step 3: Verifying NAP for {len(rows)} listings")
        
        verified_count = 0
        nap_issues_count = 0
        
        for row in rows[:15]:  # Limit to 15
            profile_url = row.get('profile_url')
            if not profile_url:
                continue
            
            try:
                # Use execution script logic
                # verify_nap signature: (url, business_name, phone, address_parts, website_url)
                address_parts = [street_address, city, state, zip_code] if street_address else [city, state]
                nap_data = verify_nap(profile_url, business_name, phone, address_parts, website_url)
                
                name_ok = nap_data.get('nap_name_ok', False)
                addr_ok = nap_data.get('nap_address_ok', False)
                phone_ok = nap_data.get('nap_phone_ok', False)
                website_ok = nap_data.get('nap_website_ok')  # Can be None, True, or False
                details = nap_data.get('details', '')
                
                # Update Supabase
                supabase.table('citation_audits').update({
                    "status": "verified",
                    "nap_name_ok": name_ok,
                    "nap_address_ok": addr_ok,
                    "nap_phone_ok": phone_ok,
                    "nap_website_ok": website_ok,
                    "nap_details": details
                }).eq('id', row['id']).execute()
                
                verified_count += 1
                if not (name_ok and addr_ok and phone_ok):
                    nap_issues_count += 1
                        
            except Exception as e:
                log_debug(f"Error verifying {profile_url}: {e}")
                continue
        
        log_debug(f"Step 3 Complete: Verified {verified_count}, NAP issues {nap_issues_count}")
        
        return jsonify({
            "success": True,
            "audit_id": audit_id,
            "verified_count": verified_count,
            "nap_issues_count": nap_issues_count,
            "message": f"Step 3 complete: {verified_count} verified, {nap_issues_count} with NAP issues"
        })

    except Exception as e:
        log_debug(f"Citation Audit Step 3 error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/redo-single', methods=['POST'])
def citation_audit_redo_single():
    """
    Redo verification for a single citation audit row.
    - If no profile_url: runs Step 2 (find URL) + Step 3 (verify NAP)
    - If has profile_url: runs only Step 3 (re-verify NAP)
    """
    from execution.discover_profile_url import discover_profile_url
    from execution.citation_audit_verify_nap import verify_nap
    from urllib.parse import urlparse
    
    try:
        data = request.get_json()
        row_id = data.get('row_id')
        
        if not row_id:
            return jsonify({"error": "row_id is required"}), 400
        
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        # Fetch the row
        row_res = supabase.table('citation_audits').select('*').eq('id', row_id).single().execute()
        if not row_res.data:
            return jsonify({"error": "Row not found"}), 404
        
        row = row_res.data
        project_id = row.get('project_id')
        directory_name = row.get('directory_name', '')
        directory_website = row.get('directory_website', '')
        profile_url = row.get('profile_url', '')
        
        log_debug(f"Redo Single: Row {row_id}, Directory: {directory_name}, Has URL: {bool(profile_url)}")
        
        # Fetch Project Data
        business_name = ''
        city = ''
        state = ''
        street_address = ''
        zip_code = ''
        phone = ''
        website_url = ''
        
        # Try medical_projects first
        try:
            project_res = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                proj = project_res.data
                business_name = proj.get('business_name', '')
                location_parts = proj.get('location', '').split(',') if proj.get('location') else []
                city = location_parts[0].strip() if location_parts else ''
                state = location_parts[1].strip() if len(location_parts) > 1 else ''
                street_address = proj.get('address', '')
                phone = proj.get('phone', '')
                website_url = proj.get('website', '')
            else:
                raise Exception("Not in medical_projects")
        except:
            # Fallback to projects
            project_res = supabase.table('projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                proj = project_res.data
                business_name = proj.get('project_name', '')
                city = proj.get('city', '') or ''
                state = proj.get('state', '') or ''
                street_address = proj.get('street_address', '')
                zip_code = proj.get('zip_code', '')
                phone = proj.get('phone', '')
                website_url = proj.get('website', '')
        
        full_address = f"{street_address}, {city}, {state} {zip_code}".strip(", ")
        
        # Step 2: Find URL if no profile_url exists OR if status is not_found
        current_status = row.get('status', '')
        if not profile_url or current_status == 'not_found':
            log_debug(f"Redo Single: Running Step 2 (Find URL) for {directory_name} (status={current_status})")
            
            # Extract domain
            if directory_website:
                try:
                    parsed = urlparse(directory_website)
                    directory_domain = parsed.netloc.replace('www.', '')
                except:
                    directory_domain = directory_website.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
            else:
                directory_domain = directory_name.lower().replace(' ', '') + '.com'
            
            result = discover_profile_url(
                directory_name=directory_name,
                directory_domain=directory_domain,
                business_name=business_name,
                city=city,
                state=state,
                alt_name=''
            )
            
            if result.get('status') == 'found':
                candidates = result.get('candidates', [])
                if candidates:
                    # For redo-single (debugging), likely good to just pick first or we could loop verify
                    # Let's verify candidates
                    profile_url = candidates[0]['url']
                    
                    # Store found URL
                    supabase.table('citation_audits').update({
                        "profile_url": profile_url,
                        "status": "found"
                    }).eq('id', row_id).execute()
                    log_debug(f"Redo Single: Found URL {profile_url}")
                else:
                    # Should not happen if status found
                    profile_url = ""
                    supabase.table('citation_audits').update({"status": "not_found"}).eq('id', row_id).execute()
            else:
                supabase.table('citation_audits').update({
                    "status": "not_found"
                }).eq('id', row_id).execute()
                return jsonify({
                    "success": True,
                    "row_id": row_id,
                    "step_2_result": "not_found",
                    "step_3_result": None,
                    "message": "Profile URL not found"
                })
        
        # Step 3: Verify NAP (if we have a profile_url)
        if profile_url:
            log_debug(f"Redo Single: Running Step 3 (Verify NAP) for {profile_url}")
            
            nap_data = verify_nap(profile_url, business_name, phone, [street_address, city, zip_code], website_url)
            
            name_ok = nap_data.get('nap_name_ok', False)
            addr_ok = nap_data.get('nap_address_ok', False)
            phone_ok = nap_data.get('nap_phone_ok', False)
            website_ok = nap_data.get('nap_website_ok')
            details = nap_data.get('details', '')
            
            supabase.table('citation_audits').update({
                "status": "verified",
                "nap_name_ok": name_ok,
                "nap_address_ok": addr_ok,
                "nap_phone_ok": phone_ok,
                "nap_website_ok": website_ok,
                "nap_details": details
            }).eq('id', row_id).execute()
            
            log_debug(f"Redo Single: Verified - Name:{name_ok}, Addr:{addr_ok}, Phone:{phone_ok}, Website:{website_ok}")
            
            return jsonify({
                "success": True,
                "row_id": row_id,
                # Root level values for frontend (expected by agency.html)
                "status": "verified",
                "nap_name_ok": name_ok,
                "nap_address_ok": addr_ok,
                "nap_phone_ok": phone_ok,
                "nap_website_ok": website_ok,
                "nap_details": details,
                "url": profile_url,
                "profile_url": profile_url,
                # Legacy nested values
                "step_2_result": "found" if not row.get('profile_url') else "skipped",
                "step_3_result": {
                    "nap_name_ok": name_ok,
                    "nap_address_ok": addr_ok,
                    "nap_phone_ok": phone_ok,
                    "nap_website_ok": website_ok,
                    "details": details
                },
                "message": "Verification complete"
            })
        
        return jsonify({"error": "Unexpected state"}), 500

    except Exception as e:
        log_debug(f"Redo Single error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/get-submit-info', methods=['POST'])
def citation_audit_get_submit_info():
    """
    Get submit info for a directory - shows homepage link, business info, and cached description.
    Description is generated ONCE per project and reused for all directories.
    """
    from lib.perplexity_client import perform_research
    
    try:
        data = request.get_json()
        row_id = data.get('row_id')
        project_id = data.get('project_id')
        
        if not row_id:
            return jsonify({"error": "row_id is required"}), 400
        
        # Get the directory info
        row_res = supabase.table('citation_audits').select('*').eq('id', row_id).single().execute()
        if not row_res.data:
            return jsonify({"error": "Directory not found"}), 404
        
        row = row_res.data
        directory_name = row.get('directory_name', '')
        directory_website = row.get('directory_website', '')
        
        # Get business info and cached description from project
        business_name = ""
        address = ""
        phone = ""
        website = ""
        service_type = ""
        cached_description = ""
        
        if project_id:
            proj_res = supabase.table('medical_projects').select('*').eq('id', project_id).execute()
            if proj_res.data:
                proj = proj_res.data[0]
                business_name = proj.get('business_name', '')
                address = proj.get('address', '')
                phone = proj.get('phone', '')
                website = proj.get('website', '')
                service_type = proj.get('service_type', '')
                cached_description = proj.get('listing_description', '')
        
        # If no cached description, generate one and save at project level
        if not cached_description and project_id:
            log_debug(f"Generating listing description for project {project_id}")
            
            prompt = f"""Write a professional 2-3 sentence business description for use on directory listings.

Business: {business_name}
Address: {address}
Type: {service_type}
Website: {website}

Requirements:
- Professional and welcoming tone
- Mention the location/area served
- Highlight what the business offers
- Under 50 words
- Do NOT include phone numbers or exact addresses
- Do NOT include citations like [1] or [2]"""

            cached_description = perform_research(prompt, model="sonar")
            
            if cached_description:
                # Save to project for reuse
                supabase.table('medical_projects').update({
                    "listing_description": cached_description
                }).eq('id', project_id).execute()
                log_debug(f"Saved listing description to project {project_id}")
        
        return jsonify({
            "success": True,
            "directory_name": directory_name,
            "directory_website": directory_website,
            "business_name": business_name,
            "address": address,
            "phone": phone,
            "website": website,
            "description": cached_description or f"{business_name} provides quality {service_type} services in the {address.split(',')[-2] if ',' in address else address} area. Contact us today to learn more."
        })
        
    except Exception as e:
        log_debug(f"Get Submit Guide error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/get-how-to-add', methods=['POST'])
def citation_audit_get_how_to_add():
    """
    Get step-by-step instructions for how to add a business to a specific directory.
    Uses Perplexity to research and generate comprehensive guide.
    Results are cached in the database for future use.
    """
    from lib.perplexity_client import perform_research
    import re
    
    try:
        data = request.get_json()
        row_id = data.get('row_id', '')  # For caching
        directory_name = data.get('directory_name', '')
        directory_website = data.get('directory_website', '')
        force_refresh = data.get('force_refresh', False)
        
        if not directory_name:
            return jsonify({"error": "directory_name is required"}), 400
        
        # Check for cached guide if row_id provided AND not forcing refresh
        if row_id and not force_refresh:
            try:
                row_res = supabase.table('citation_audits').select('how_to_add_guide').eq('id', row_id).single().execute()
                if row_res.data and row_res.data.get('how_to_add_guide'):
                    log_debug(f"Returning cached How-to-Add for {directory_name}")
                    return jsonify({
                        "success": True,
                        "directory_name": directory_name,
                        "directory_website": directory_website,
                        "instructions": row_res.data['how_to_add_guide'],
                        "cached": True
                    })
            except Exception as e:
                log_debug(f"Cache check failed: {e}")
                # Continue to fetch from Perplexity
        
        log_debug(f"Getting How-to-Add instructions for: {directory_name}")
        
        # Use our Camoufox scraper to find the actual Add Business page
        # This is more accurate than Perplexity guessing
        from execution.scrape_add_business_guide import get_add_business_guide
        
        result = get_add_business_guide(directory_name, directory_website)
        
        if not result:
            log_debug(f"Scraper returned no result for {directory_name}")
            return jsonify({
                "success": False,
                "error": "Could not analyze directory website. Please try again.",
                "directory_name": directory_name
            })
        
        log_debug(f"Got How-to-Add instructions for {directory_name} ({len(result)} chars)")
        
        # Cache the result in database if row_id provided
        if row_id:
            try:
                supabase.table('citation_audits').update({
                    'how_to_add_guide': result
                }).eq('id', row_id).execute()
                log_debug(f"Cached How-to-Add guide for row {row_id}")
            except Exception as e:
                log_debug(f"Failed to cache guide: {e}")
                # Continue anyway - caching failure shouldn't break the response
        
        return jsonify({
            "success": True,
            "directory_name": directory_name,
            "directory_website": directory_website,
            "instructions": result,
            "cached": False
        })
        
    except Exception as e:
        log_debug(f"Get How-to-Add error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/process-job', methods=['POST'])
def generate_dynamic_outline(topic, research_context, project_loc, gemini_client):
    """Generates a structured JSON outline for the article based on research."""
    print(f"DEBUG: Generating Dynamic Outline for '{topic}'...", flush=True)
    
    prompt = f"""
    You are an expert Content Strategist. Create a detailed Outline for a "Best-in-Class" article.
    
    TOPIC: {topic}
    TARGET AUDIENCE: {project_loc}
    
    RESEARCH BRIEF:
    {research_context[:15000]} 
    
    TASK:
    Create a logical H2 structure for a comprehensive 2500-4500 word article.
    1. Must include "Introduction" (Hook, Problem, Thesis)
    2. Must include "Blue Ocean" / "Competitor Gaps" section (Differentiators)
    3. Must include "Decision Matrix" / "Framework" section (How to choose)
    4. Must include "Detailed Breakdown" (Models/Types/Categories)
    5. Must include "ROI & Hidden Costs" (Financials/Risks)
    6. Must include "Conclusion & Checklist" (Actionable)
    7. Must include "FAQ" (Schema-ready)
    
    OUTPUT FORMAT (JSON ARRAY):
    [
        {{"title": "Introduction", "instructions": "Hook the reader, define the problem, state the thesis. Mention key stats."}},
        {{"title": "The Hidden Costs of X", "instructions": "Deep dive into costs competitors hide. Use data from research."}},
        ...
    ]
    """
    
    try:
        response = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-pro",
            use_grounding=False # Logic only
        )
        
        # Clean JSON
        if not response: return []
        cleaned = response.strip()
        if cleaned.startswith('```json'): cleaned = cleaned[7:]
        if cleaned.startswith('```'): cleaned = cleaned[3:]
        if cleaned.endswith('```'): cleaned = cleaned[:-3]
        
        import json
        return json.loads(cleaned.strip())
    except Exception as e:
        print(f"Error generating outline: {e}")
        # Fallback Outline
        return [
            {"title": "Introduction", "instructions": "Introduction to the topic."},
            {"title": "Key Concepts", "instructions": "Explain the core concepts."},
            {"title": "Detailed Analysis", "instructions": "Deep dive into the details."},
            {"title": "Comparison", "instructions": "Compare options."},
            {"title": "Conclusion", "instructions": "Wrap up."}
        ]

def generate_sections_chunked(topic, outline, research_context, project_loc, gemini_client, links_str):
    """Generates the article section by section based on the outline."""
    full_content = []
    
    print(f"DEBUG: Starting Chunked Generation for '{topic}' ({len(outline)} sections)...", flush=True)
    
    # Context Window Management (Keep it relevant)
    previous_section_summary = "Start of article."
    
    for i, section in enumerate(outline):
        section_title = section.get('title', f"Section {i+1}")
        instructions = section.get('instructions', '')
        
        print(f"  > Generating Section {i+1}/{len(outline)}: {section_title}...", flush=True)
        
        prompt = f"""
        You are an expert Senior Technical Writer. Write ONE section of a comprehensive guide.
        
        TOPIC: {topic}
        CURRENT SECTION: {section_title}        - Audience Location: {project_loc}
        - Tone: Authoritative, Data-Driven, "Best-in-Class"
        - Previous Section Summary: {previous_section_summary}
        
        RESEARCH DATA (Use strictly):
        {research_context[:10000]}
        
        INTERNAL LINKS TO INSERT (Must fit organically):
        {links_str}
        
        WRITING RULES:
        1. Use Markdown (H2 for the section title, H3/H4 for subsections).
        2. NO INTRO/OUTRO FLUFF. Dive straight into the content.
        3. Use Bullet points, Data tables, and Bold text for readability.
        4. If mentioning a competitor/product from research, be specific (Pros/Cons).
        5. LENGTH: 400-600 words for this section.
        """
        
        try:
            section_content = gemini_client.generate_content(
                prompt=prompt,
                model_name="gemini-2.5-pro",
                use_grounding=True 
            )
            
            if section_content:
                # Clean up
                if section_content.startswith('```markdown'): section_content = section_content[11:]
                if section_content.startswith('```'): section_content = section_content[3:]
                if section_content.endswith('```'): section_content = section_content[:-3]
                
                full_content.append(section_content.strip())
                
                # Update summary for next chunk (simple context propagation)
                previous_section_summary = f"Just covered {section_title}. Key points: {section_content[:200]}..."
            else:
                full_content.append(f"## {section_title}\n\n(Content generation failed for this section.)")
                
        except Exception as e:
            print(f"Error generating section '{section_title}': {e}")
            full_content.append(f"## {section_title}\n\n(Error generating content.)")
            
        # Rate limit pause
        import time
        time.sleep(2)
        
    return "\n\n".join(full_content)

def final_polish(full_content, topic, primary_keyword, cta_url, project_loc, gemini_client):
    """Assembles the chunks and adds a cohesive Intro, Outro, and Meta Description."""
    print(f"DEBUG: Polishing final article for '{topic}'...", flush=True)
    
    prompt = f"""
    You are an expert Editor. Assemble and Polish this article.
    
    TOPIC: {topic}
    PRIMARY KEYWORD: {primary_keyword}
    CTA URL: {cta_url}
    LOCATION: {project_loc}
    
    RAW CONTENT CHUNKS:
    {full_content[:25000]} 
    
    TASK:
    1. Write a **Killer Introduction** (H1 Title + Hook + Thesis).
       - H1 must contain "{primary_keyword}".
    2. Review the body content (passed above) and smooth out transitions if needed (but keep the bulk of it).
    3. Write a **High-Conversion Conclusion**.
       - Must end with a Call-to-Action (CTA) linking to: {cta_url}
    4. Write a **Meta Description** (155 chars, SEO optimized).
    
    OUTPUT FORMAT (Markdown):
    **Meta Description**: [Your Description Here]
    
    # [H1 Title]
    
    [Introduction]
    
    [Body Content - Inserted/Polished]
    
    [Conclusion + CTA]
    """
    
    try:
        final_text = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-pro",
            use_grounding=False # Editing task
        )
        return final_text if final_text else full_content
    except Exception as e:
        print(f"Error in final polish: {e}")
        return full_content

def generate_chunked_article(topic, research_context, outline, project_loc, project_lang, primary_keyword, kw_list, links_str, citations_str, gemini_client, cta_url=None):
    """Generates the article section-by-section to ensure length and depth."""
    print(f"DEBUG: Starting Chunked Generation for '{topic}' ({len(outline)} sections)...", flush=True)
    
    full_content = []
    previous_context = ""
    links_inserted_count = 0
    import re
    
    # Meta Description Generation (First Step)
    meta_prompt = f"""Write a compelling SEO Meta Description for an article about "{topic}".
    Primary Keyword: {primary_keyword}
    Target Audience: {project_loc}
    Length: 150-160 characters.
    """
    try:
        meta_desc = gemini_client.generate_content(meta_prompt, model_name="gemini-2.5-flash").strip()
        full_content.append(f"**Meta Description**: {meta_desc}\n\n")
    except: pass

    for i, section in enumerate(outline):
        print(f"DEBUG: Generating Section {i+1}/{len(outline)}: {section['title']}...", flush=True)
        
        # Special Instructions based on position
        special_instructions = ""
        if i == 0:
            special_instructions += "\n        - **TL;DR**: Include a '## Key Takeaways' section immediately after the introduction bullet points."
        
        if i == len(outline) - 1:
            special_instructions += """
        - **FINAL SECTION STRUCTURE (Strict Order)**:
          1. **Conclusion**: Write the conclusion text.
          2. **CTA**: Place the Mandatory CTA here (see link instructions).
          3. **FAQ**: Add a '## Frequently Asked Questions' section (6-8 Q&As).
          4. **References**: Add a '## References' section. List the citations used as a bulleted list of Markdown links.
            """

        # Smart Linking Logic
        link_instruction = ""
        if links_str and links_str != "No internal links available":
            remaining_sections = len(outline) - (i + 1)
            target_links = 7
            needed_links = target_links - links_inserted_count
            
            # Last Section: FORCE CTA
            if i == len(outline) - 1 and cta_url:
                 link_instruction = f"6. **CTA (CRITICAL)**: You MUST include a strong Call to Action linking to {cta_url} immediately after the Conclusion text (before the FAQ). Use anchor text like 'Get Started', 'View Pricing', or 'Learn More'."
            
            # Other Sections: Smart Distribution
            elif needed_links > 0:
                if needed_links >= remaining_sections: # Must insert now to hit target
                    link_instruction = f"6. **Internal Links**: You MUST include exactly 1 internal link in this section from: {links_str}. Use DYNAMIC anchor text."
                elif links_inserted_count >= 8: # Cap at 8
                    link_instruction = "6. **Internal Links**: Do NOT include any internal links in this section."
                else: # Encourage but don't force
                    link_instruction = f"6. **Internal Links**: Try to naturally include 1 internal link from: {links_str}. Use DYNAMIC anchor text."
            else:
                 link_instruction = "6. **Internal Links**: Do NOT include any internal links in this section."
        else:
            link_instruction = "6. **Internal Links**: No internal links available."

        chunk_prompt = f"""
        You are writing Section {i+1} of {len(outline)} for a deep, expert-level article.
        
        ARTICLE TOPIC: {topic}
        SECTION TITLE: {section['title']}
        SECTION GOAL: {section['instructions']}
        
        CONTEXT:
        - Location: {project_loc}
        - Language: {project_lang}
        - Primary Keyword: {primary_keyword}
        
        PREVIOUS CONTENT CONTEXT (Last 500 words):
        ... {previous_context[-2000:]} ...
        
        FULL RESEARCH BRIEF (Source of Truth):
        {research_context}
        
        INSTRUCTIONS:
        1. Write **350-500 words** for this section alone. Do NOT exceed 600 words.
        2. Be detailed and high-impact. NO FLUFF.
        3. Use Markdown formatting (tables, bolding, lists).
        4. **Strictly follow** the Research Brief for data/facts.
        5. **Blue Ocean & Gaps**: If this section covers gaps, highlight them aggressively.
        {link_instruction}
        7. **Citations**: Use citations from research: {citations_str}
        8. **Anti-Repetition**: Check 'PREVIOUS CONTENT CONTEXT'. Do NOT repeat points already made.
        {special_instructions}
        
        OUTPUT:
        Return ONLY the content for this section. Start with the H2 header: ## {section['title']}
        """
        
        try:
            chunk_text = gemini_client.generate_content(
                prompt=chunk_prompt,
                model_name="gemini-2.5-pro",
                use_grounding=True
            )
            
            if chunk_text:
                # Clean up
                cleaned_chunk = chunk_text.strip()
                if cleaned_chunk.startswith('```markdown'): cleaned_chunk = cleaned_chunk[11:]
                if cleaned_chunk.startswith('```'): cleaned_chunk = cleaned_chunk[3:]
                if cleaned_chunk.endswith('```'): cleaned_chunk = cleaned_chunk[:-3]
                
                full_content.append(cleaned_chunk)
                previous_context += "\n" + cleaned_chunk
                
                # Count links inserted
                links_in_chunk = len(re.findall(r'\[.*?\]\(.*?\)', cleaned_chunk))
                links_inserted_count += links_in_chunk
                print(f"DEBUG: Section {i+1} generated {links_in_chunk} links. Total: {links_inserted_count}", flush=True)
                
                # Small delay to be nice to API
                import time
                time.sleep(1)
            else:
                print(f"⚠ Empty response for section {section['title']}")
                
        except Exception as e:
            print(f"Error generating section {section['title']}: {e}")
            full_content.append(f"## {section['title']}\n\n(Content generation failed for this section. Please review.)")

    return "\n\n".join(full_content)

def process_job():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500

    try:
        # Step A: Fetch one pending job
        response = supabase.table('audit_results').select("*").eq('status', 'PENDING').limit(1).execute()
        
        if not response.data:
            return jsonify({"message": "No pending jobs"})
        
        job = response.data[0]
        job_id = job['id']
        target_url = job.get('url')
        if not target_url:
            target_url = 'example.com'
        
        # Step B: Lock (Update status to PROCESSING)
        supabase.table('audit_results').update({"status": "PROCESSING"}).eq('id', job_id).execute()
        
        # Step C: Work (Generate SEO audit)
        
        # 1. Get Keywords (Graceful degradation)
        keywords = get_ranking_keywords(target_url)
        keywords_str = ", ".join(keywords) if keywords else "No specific ranking keywords found."

        # 2. Generate Audit with Gemini
        # model = genai.GenerativeModel('gemini-2.5-flash')
        
        prompt = f"Analyze SEO for {target_url}. It currently ranks for these top keywords: {keywords_str}. Based on this, suggest 3 new content topics."
        
        audit_result = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-flash"
        )
        
        if not audit_result:
            audit_result = "Audit generation failed."
        
        # Step D: Save (Update result and status to COMPLETED)
        supabase.table('audit_results').update({
            "status": "COMPLETED",
            "result": audit_result
        }).eq('id', job_id).execute()
        
        return jsonify({
            "id": job_id,
            "status": "COMPLETED",
            "result": audit_result
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/write-article', methods=['POST'])
def write_article():
    if not os.environ.get("GEMINI_API_KEY"):
        return jsonify({"error": "GEMINI_API_KEY not found"}), 500

    try:
        data = request.get_json()
        topic = data.get('topic')
        keywords = data.get('keywords', [])

        if not topic:
            return jsonify({"error": "Topic is required"}), 400

        # model = genai.GenerativeModel('gemini-2.5-flash')
        
        system_instruction = "You are an expert SEO content writer. Write a comprehensive, engaging 1,500-word blog post about the given topic. Use H2 and H3 headers. Format in Markdown. Include a catchy title."
        
        keywords_str = ', '.join(keywords) if keywords else 'relevant SEO keywords'
        full_prompt = f"{system_instruction}\n\nTopic: {topic}\nTarget Keywords: {keywords_str}"
        
        generated_text = gemini_client.generate_content(
            prompt=full_prompt,
            model_name="gemini-2.5-flash"
        )
        
        if not generated_text:
             return jsonify({"error": "Gemini generation failed"}), 500
             
        # Save to DB if project_id is present
        page_id = None
        project_id = data.get('project_id')
        
        if project_id and supabase:
            try:
                # Create slug
                slug = topic.lower().replace(' ', '-')
                slug = re.sub(r'[^a-z0-9-]', '', slug)
                
                # Insert page
                page_data = {
                    "project_id": project_id,
                    "url": f"topic://{slug}", # Virtual URL
                    "status": "COMPLETED",
                    "page_type": "Topic",
                    "content": generated_text.strip(),
                    "tech_audit_data": {
                        "title": topic,
                        "meta_description": "Generated by AgencyOS" 
                    },
                    "keyword_data": keywords
                }
                res = supabase.table('pages').insert(page_data).execute()
                if res.data:
                    page_id = res.data[0]['id']
            except Exception as e:
                print(f"Error saving topic page: {e}")

        return jsonify({"content": generated_text.strip(), "page_id": page_id})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

from bs4 import BeautifulSoup

# ... (existing imports)





import subprocess

def fetch_with_curl(url, use_chrome_ua=True):
    """Fetch URL using system curl to bypass TLS fingerprinting blocks. Returns (content, latency)."""
    try:
        # Use a delimiter to separate content from the time metric
        delimiter = "|||CURL_TIME|||"
        # Increased timeout to 30s for slow sites
        cmd = ['curl', '-L', '-s', '-w', f'{delimiter}%{{time_total}}', '--max-time', '30']
        
        if use_chrome_ua:
            cmd.extend([
                '-A', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                '-H', 'Accept-Language: en-US,en;q=0.9',
                '-H', 'Referer: https://www.google.com/',
                '-H', 'Upgrade-Insecure-Requests: 1'
            ])
            
        cmd.append(url)
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        
        # If failed with Chrome UA, retry without it (some sites like Akamai block fake UAs but allow curl)
        if use_chrome_ua and (result.returncode != 0 or not result.stdout or "Access Denied" in result.stdout):
            print(f"DEBUG: Chrome UA failed for {url}, retrying with default curl UA...")
            return fetch_with_curl(url, use_chrome_ua=False)
            
        if result.returncode == 0 and result.stdout:
            # Split content and time
            parts = result.stdout.rsplit(delimiter, 1)
            if len(parts) == 2:
                content = parts[0]
                try:
                    latency = float(parts[1])
                except:
                    latency = 0
                return content, latency
            else:
                return result.stdout, 0
        else:
            print(f"DEBUG: curl failed with code {result.returncode}: {result.stderr}")
            return None, 0
    except Exception as e:
        print(f"DEBUG: curl exception: {e}")
        return None, 0

def crawl_sitemap(domain, project_id, max_pages=200):
    """Recursively crawl sitemaps with anti-bot headers"""
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
        'Sec-Fetch-User': '?1'
    }
    
    base_domain = domain.rstrip('/') if domain.startswith('http') else f"https://{domain.rstrip('/')}"
    sitemap_urls = []
    
    # 1. Try robots.txt first
    robots_url = f"{base_domain}/robots.txt"
    print(f"DEBUG: Fetching robots.txt: {robots_url}")
    try:
        robots_res = requests.get(robots_url, headers=headers, timeout=10)
        if robots_res.status_code == 200:
            for line in robots_res.text.splitlines():
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    sitemap_urls.append(sitemap_url)
            print(f"DEBUG: Found {len(sitemap_urls)} sitemaps in robots.txt")
    except Exception as e:
        print(f"DEBUG: Failed to fetch robots.txt: {e}")

    # 2. Fallback to common paths
    if not sitemap_urls:
        sitemap_urls = [
            f"{base_domain}/sitemap.xml",
            f"{base_domain}/sitemap_index.xml",
            f"{base_domain}/sitemap.php"
        ]

    pages = []
    
    # 3. Process each sitemap
    for sitemap_url in sitemap_urls:
        if len(pages) >= max_pages:
            break
        pages.extend(fetch_sitemap_urls(sitemap_url, project_id, headers, max_pages - len(pages)))
    
    return pages

def clean_title(title):
    """Clean up product titles by removing common e-commerce patterns."""
    if not title: return "Untitled Product"
    
    # Remove "Buy " from start (case insensitive)
    import re
    title = re.sub(r'^buy\s+', '', title, flags=re.IGNORECASE)
    
    # Remove " Online" from end (case insensitive)
    title = re.sub(r'\s+online$', '', title, flags=re.IGNORECASE)
    
    # Remove " - [Brand]" or " | [Brand]" suffix
    # Heuristic: split by " - " or " | " and take the first part if it's long enough
    separators = [" - ", " | ", " – "]
    for sep in separators:
        if sep in title:
            parts = title.split(sep)
            if len(parts[0]) > 3: # Avoid cutting too much if title is short
                title = parts[0]
                break
                
    return title.strip()

def fetch_sitemap_urls(sitemap_url, project_id, headers, max_urls):
    """Fetch URLs from a sitemap, recursively handling sitemap indexes"""
    print(f"DEBUG: Fetching sitemap: {sitemap_url}")
    pages = []
    
    try:
        # Use fetch_with_curl for robustness against bot protection
        content, latency = fetch_with_curl(sitemap_url)
        
        # Fallback to requests if curl fails
        if not content:
            print(f"DEBUG: curl failed for {sitemap_url}, falling back to requests...")
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                resp = requests.get(sitemap_url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    content = resp.text
                    latency = resp.elapsed.total_seconds()
                    print(f"DEBUG: requests fallback successful for {sitemap_url}")
            except Exception as req_err:
                print(f"DEBUG: requests fallback failed: {req_err}")
        
        if not content:
            print(f"DEBUG: Failed to fetch {sitemap_url} (curl and requests failed)")
            return pages

        # Try parsing with XML, fallback to HTML parser if needed
        try:
            soup = BeautifulSoup(content, 'xml')
        except:
            soup = BeautifulSoup(content, 'html.parser')
        
        # Check if this is a sitemap index (contains <sitemap> tags)
        sitemap_tags = soup.find_all('sitemap')
        
        if sitemap_tags:
            # Recursively fetch ALL child sitemaps (removed limit of 5)
            for i, sitemap_tag in enumerate(sitemap_tags):
                if len(pages) >= max_urls:
                    break
                    
                loc = sitemap_tag.find('loc')
                if loc:
                    child_url = loc.text.strip()
                    print(f"DEBUG: Recursively fetching child sitemap {i+1}: {child_url}")
                    
                    # Rate Limit: Sleep 2 seconds between sitemaps to avoid 429/Blocking
                    import time
                    time.sleep(2)
                    
                    child_pages = fetch_sitemap_urls(child_url, project_id, headers, max_urls - len(pages))
                    
                    if not child_pages:
                        print(f"DEBUG: Warning - Child sitemap {child_url} returned 0 pages. Possible block?")
                        
                    pages.extend(child_pages)
        else:
            # Regular sitemap with <url> tags
            url_tags = soup.find_all('url')
            print(f"DEBUG: Found {len(url_tags)} URLs in sitemap")
            
            for tag in url_tags:
                if len(pages) >= max_urls:
                    break
                    
                loc = tag.find('loc')
                if loc and loc.text.strip():
                    url = loc.text.strip()
                    
                    # Skip title scraping for speed. 
                    # User can run "Perform Audit" to get details.
                    title = "Pending Scan"

                    pages.append({
                        'project_id': project_id,
                        'url': url,
                        'status': 'DISCOVERED',
                        'tech_audit_data': {'title': title} 
                    })
    
    except Exception as e:
        print(f"DEBUG: Error fetching sitemap {sitemap_url}: {e}")
    
    return pages


# Helper function to upload to Supabase Storage
def upload_to_supabase(file_data, filename, bucket_name='photoshoots'):
    """
    Uploads file data (bytes) to Supabase Storage and returns the public URL.
    """
    import mimetypes
    try:
        # Guess mime type
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type:
            mime_type = 'application/octet-stream'
            
        # Upload
        res = supabase.storage.from_(bucket_name).upload(
            path=filename,
            file=file_data,
            file_options={"content-type": mime_type, "upsert": "true"}
        )
        
        # Get Public URL
        public_url = supabase.storage.from_(bucket_name).get_public_url(filename)
        return public_url
    except Exception as e:
        print(f"Supabase Upload Error: {e}")
        raise e

# Helper to load image from URL or Path
def load_image_data(source):
    """
    Loads image data from a URL (starts with http) or local path.
    Returns PIL Image object.
    """
    import PIL.Image
    import io
    import os
    if source.startswith('http'):
        print(f"Downloading image from URL: {source}")
        resp = requests.get(source)
        resp.raise_for_status()
        return PIL.Image.open(io.BytesIO(resp.content))
    else:
        # Assume local path relative to public
        # Handle cases where source might be just filename or /uploads/filename
        clean_path = source.lstrip('/')
        local_path = os.path.join(os.getcwd(), 'public', clean_path)
        print(f"Loading image from local path: {local_path}")
        if os.path.exists(local_path):
            return PIL.Image.open(local_path)
        else:
            # Try absolute path just in case
            if os.path.exists(source):
                return PIL.Image.open(source)
            raise Exception(f"Image not found at {source} or {local_path}")



@app.route('/api/get-projects', methods=['GET'])
def get_projects():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        # Fetch projects
        projects_res = supabase.table('projects').select('*').order('created_at', desc=True).execute()
        projects = projects_res.data if projects_res.data else []
        
        if not projects:
            return jsonify({"projects": []})
        
        # Fetch profiles for these projects
        try:
            profiles_res = supabase.table('business_profiles').select('*').execute()
            profiles_data = profiles_res.data if profiles_res.data else []
            profiles_map = {p['project_id']: p for p in profiles_data}
        except Exception as e:
            print(f"Error fetching profiles: {e}")
            profiles_map = {}
        
        # Calculate counts per project (OPTIMIZED: Batched fetch + In-memory aggregation)
        # This avoids N+1 query problem which causes slow loading
        from collections import defaultdict
        counts = defaultdict(int)
        classified_counts = defaultdict(int)
        
        try:
            all_pages = []
            has_more = True
            offset = 0
            limit = 5000 # Fetch in large chunks to minimize requests
            
            while has_more:
                # Fetch just the columns we need
                res = supabase.table('pages').select('project_id, page_type').range(offset, offset + limit - 1).execute()
                batch = res.data if res.data else []
                
                all_pages.extend(batch)
                
                if len(batch) < limit:
                    has_more = False
                offset += limit
                
            # Aggregate counts
            for page in all_pages:
                pid = page.get('project_id')
                if pid:
                    counts[pid] += 1
                    pt = page.get('page_type')
                    if pt and pt.lower() != 'unclassified':
                        classified_counts[pid] += 1
                        
        except Exception as e:
            print(f"Error fetching pages for counts: {e}")
            # Fallback to 0 counts if fetch fails, don't crash the whole endpoint

        # Merge and parse strategy plan
        final_projects = []
        for p in projects:
            try:
                profile = profiles_map.get(p['id'], {})
                
                # Parse Strategy Plan
                summary = profile.get('business_summary') or ''
                strategy_plan = ''
                if '===STRATEGY_PLAN===' in summary:
                    try:
                        parts = summary.split('===STRATEGY_PLAN===')
                        summary = parts[0].strip()
                        if len(parts) > 1:
                            strategy_plan = parts[1].strip()
                    except:
                        pass
                
                # Efficient Counting via Supabase
                # 1. Total Count
                try:
                    count_res = supabase.table('pages').select('id', count='exact', head=True).eq('project_id', p['id']).execute()
                    page_count = count_res.count
                except:
                    page_count = 0
                    
                # 2. Classified Count (Not Unclassified)
                # Note: Supabase 'neq' with NULLs can be tricky. We assume 'Unclassified' is the string.
                # If page_type is NULL, it's also unclassified.
                # So we want count where page_type IS NOT NULL AND page_type != 'Unclassified'
                try:
                    # We can't easily do complex OR logic in one query without RPC.
                    # Simplified: Count where page_type is NOT 'Unclassified' (ignoring NULLs for now or assuming default is Unclassified)
                    # Actually, let's just count where page_type is in ['Product', 'Category', 'Service', 'Article', 'Blog']
                    # Or just subtract unclassified from total?
                    # Let's do: count where page_type != 'Unclassified'
                    class_res = supabase.table('pages').select('id', count='exact', head=True).eq('project_id', p['id']).neq('page_type', 'Unclassified').execute()
                    classified_count = class_res.count
                except:
                    classified_count = 0
                
                # Construct the project object to return
                project_obj = {
                    "id": p['id'],
                    "project_name": p['project_name'],
                    "domain": p['domain'],
                    "language": p['language'],
                    "location": p['location'],
                    "focus": p['focus'],
                    "created_at": p['created_at'],
                    "business_summary": summary, # Cleaned summary
                    "strategy_plan": strategy_plan, # Extracted strategy
                    "ideal_customer_profile": profile.get('ideal_customer_profile'),
                    "brand_voice": profile.get('brand_voice'),
                    "primary_products": profile.get('primary_products'),
                    "competitors": profile.get('competitors'),
                    "unique_selling_points": profile.get('unique_selling_points'),
                    "page_count": page_count,
                    "classified_count": classified_count,
                    # NAP fields for Citation Audit
                    "phone": p.get('phone'),
                    "website": p.get('website'),
                    "service_type": p.get('service_type'),
                    "city": p.get('city'),
                    "state": p.get('state'),
                    "street_address": p.get('street_address'),
                    "zip_code": p.get('zip_code'),
                    "doctor_name": p.get('doctor_name')
                }
                final_projects.append(project_obj)
            except Exception as e:
                print(f"Error processing project {p.get('id')}: {e}")
                continue
            
        return jsonify({"projects": final_projects})
    except Exception as e:
        print(f"Critical error in get_projects: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/get-pages', methods=['GET'])
def get_pages():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "project_id is required"}), 400
        
        # Optimize: Select only necessary columns for the list view
        # We need tech_audit_data for the status/title, but we don't need the full body_content if it's huge.
        # However, Supabase select doesn't support "exclude".
        # Let's select explicit columns.
        response = supabase.table('pages').select('id, project_id, url, page_type, created_at, tech_audit_data, funnel_stage, source_page_id, content_description, keywords, product_action, research_data, content').eq('project_id', project_id).order('id').execute()
        
        import sys
        print(f"DEBUG: get_pages for {project_id} found {len(response.data) if response.data else 0} pages.", file=sys.stderr)
        
        # DEBUG: Check data structure
        if response.data:
            print(f"DEBUG: get_pages first row keys: {response.data[0].keys()}", file=sys.stderr)
            print(f"DEBUG: get_pages first row tech_audit_data: {response.data[0].get('tech_audit_data')}", file=sys.stderr)
            
        return jsonify({"pages": response.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/delete-page', methods=['DELETE'])
def delete_page():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    page_id = request.args.get('page_id')
    if not page_id:
        return jsonify({"error": "page_id is required"}), 400
        
    try:
        # Recursive delete function to handle children manually
        def delete_children(pid):
            # Find all children
            children = supabase.table('pages').select('id').eq('source_page_id', pid).execute()
            if children.data:
                for child in children.data:
                    delete_children(child['id'])
            
            # Delete the page itself
            supabase.table('pages').delete().eq('id', pid).execute()

        delete_children(page_id)
        
        return jsonify({"message": "Page and all children deleted successfully"})
    except Exception as e:
        print(f"Error deleting page: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/get-page-status', methods=['GET'])
def get_page_status():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        page_id = request.args.get('page_id')
        if not page_id:
            return jsonify({"error": "page_id is required"}), 400
            
        response = supabase.table('pages').select('id, product_action, audit_status').eq('id', page_id).single().execute()
        
        if not response.data:
            return jsonify({"error": "Page not found"}), 404
            
        # Log the status being returned (to debug premature closing)
        print(f"DEBUG: get_page_status for {page_id}: {response.data.get('product_action')}", file=sys.stderr)
        
        return jsonify(response.data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/create-project', methods=['POST'])
def create_project():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.get_json()
    print(f"DEBUG: create_project called with data: {data}")
    domain = data.get('domain')
    project_name = data.get('project_name', domain)
    language = data.get('language', 'English')
    location = data.get('location', 'US')
    focus = data.get('focus', 'Product')
    
    # NAP fields for Medical projects
    doctor_name = data.get('doctor_name', '')
    service_type = data.get('service_type', '')
    street_address = data.get('street_address', '')
    city = data.get('city', '')
    state = data.get('state', '')
    zip_code = data.get('zip_code', '')
    phone = data.get('phone', '')
    
    if not domain:
        return jsonify({"error": "Business/Clinic name is required"}), 400
        
    try:
        # 1. Create Project with all fields including NAP
        print(f"Creating project for {domain}...")
        project_data = {
            "domain": domain,
            "project_name": project_name,
            "language": language,
            "location": location,
            "focus": focus,
            "doctor_name": doctor_name,
            "service_type": service_type,
            "street_address": street_address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "phone": phone
        }
        
        project_res = supabase.table('projects').insert(project_data).execute()
        
        if not project_res.data:
            raise Exception("Failed to create project")
            
        project_id = project_res.data[0]['id']
        print(f"Project created: {project_id}")
        
        # 2. Create Business Profile (just link to project)
        supabase.table('business_profiles').insert({
            "project_id": project_id
        }).execute()
        
        return jsonify({
            "message": "Project created successfully",
            "project_id": project_id,
            "focus": focus
        })
        
    except Exception as e:
        print(f"ERROR in create_project: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# =============================================================================
# Medical Projects API (New simplified schema)
# =============================================================================

@app.route('/api/medical-projects', methods=['GET'])
def get_medical_projects():
    """Get all medical projects."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        result = supabase.table('medical_projects').select('*').order('created_at', desc=True).execute()
        return jsonify({"projects": result.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/medical-projects', methods=['POST'])
def create_medical_project():
    """Create a new medical project."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.get_json()
    
    project_data = {
        "business_name": data.get('business_name'),
        "website": data.get('website', ''),
        "language": data.get('language', 'English'),
        "location": data.get('location', ''),
        "service_type": data.get('service_type', ''),
        "phone": data.get('phone', ''),
        "address": data.get('address', ''),
    }
    
    if not project_data['business_name']:
        return jsonify({"error": "business_name is required"}), 400
    
    try:
        result = supabase.table('medical_projects').insert(project_data).execute()
        return jsonify({"project": result.data[0], "message": "Project created"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/medical-projects/<project_id>', methods=['GET'])
def get_medical_project(project_id):
    """Get a single medical project with its reviews."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        project = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
        reviews = supabase.table('google_reviews').select('*').eq('project_id', project_id).order('review_date', desc=True).execute()
        
        return jsonify({
            "project": project.data,
            "reviews": reviews.data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/medical-projects/<project_id>', methods=['DELETE'])
def delete_medical_project(project_id):
    """Delete a medical project."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        supabase.table('medical_projects').delete().eq('id', project_id).execute()
        return jsonify({"message": "Project deleted"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =============================================================================
# Google Reviews API
# =============================================================================

@app.route('/api/reviews/<project_id>', methods=['GET'])
def get_reviews(project_id):
    """Get all reviews for a project."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        result = supabase.table('google_reviews').select('*').eq('project_id', project_id).order('review_date', desc=True).execute()
        return jsonify({"reviews": result.data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/reviews/<review_id>/generate-response', methods=['POST'])
def generate_review_response(review_id):
    """Generate a HIPAA-compliant response for a review."""
    if not supabase or not gemini_client:
        return jsonify({"error": "Service not configured"}), 500
    
    try:
        # Get the review
        review_res = supabase.table('google_reviews').select('*').eq('id', review_id).single().execute()
        if not review_res.data:
            return jsonify({"error": "Review not found"}), 404
        
        review = review_res.data
        star_rating = review.get('star_rating', 3)
        review_text = review.get('review_text', '')
        
        # General Business Response Prompt
        prompt = f"""Generate a professional, warm response to this customer review.

=== GUIDELINES ===
- Be professional, warm, and genuine.
- For positive reviews (4-5 stars): Express gratitude, mention specific aspects they liked, and invite them back.
- For negative reviews (1-2 stars): Show empathy, apologize for the experience (without admitting legal liability), and invite them to contact you directly to resolve the issue.
- Keep it concise (2-4 sentences).

=== REVIEW ===
Star Rating: {star_rating}/5
Review Text: "{review_text}"

=== RESPONSE STYLE ===
- Avoid generic boilerplate.
- Use a friendly tone.
- If the review confirms they were a customer, you can acknowledge it (unlike HIPAA rules for medical).

Generate a response now:"""

        response = gemini_client.generate_content(prompt, model_name="gemini-2.5-flash", temperature=0.7)
        
        if response:
            # Save the response
            supabase.table('google_reviews').update({
                "response": response.strip(),
                "response_generated_at": "now()"
            }).eq('id', review_id).execute()
            
            return jsonify({
                "response": response.strip(),
                "message": "Response generated"
            })
        else:
            return jsonify({"error": "Failed to generate response"}), 500
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/reviews/<review_id>/save-response', methods=['POST'])
def save_review_response(review_id):
    """Save/edit a review response."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.get_json()
    response_text = data.get('response', '')
    
    try:
        supabase.table('google_reviews').update({
            "response": response_text,
            "response_generated_at": "now()"
        }).eq('id', review_id).execute()
        
        return jsonify({"message": "Response saved"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/reviews/add', methods=['POST'])
def add_review():
    """Manually add a review."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.get_json()
    
    review_data = {
        "project_id": data.get('project_id'),
        "reviewer_name": data.get('reviewer_name', 'Anonymous'),
        "star_rating": int(data.get('star_rating', 3)),
        "review_date": data.get('review_date'),
        "review_text": data.get('review_text', ''),
    }
    
    if not review_data['project_id']:
        return jsonify({"error": "project_id is required"}), 400
    
    try:
        result = supabase.table('google_reviews').insert(review_data).execute()
        return jsonify({"review": result.data[0], "message": "Review added"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/scrape-reviews/<project_id>', methods=['POST'])
def scrape_reviews(project_id):
    """Scrape Google reviews for a medical project using Apify."""
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.get_json() or {}
    google_maps_url = data.get('google_maps_url')
    max_reviews = data.get('max_reviews', 20)
    
    if not google_maps_url:
        return jsonify({"error": "google_maps_url is required"}), 400
    
    APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
    if not APIFY_API_KEY:
        return jsonify({"error": "APIFY_API_KEY not configured"}), 500
    
    try:
        import time
        
        ACTOR_ID = "Xb8osYTtOjlsgI6k9"
        run_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_API_KEY}"
        
        # Actor input
        actor_input = {
            "startUrls": [{"url": google_maps_url}],
            "maxReviews": max_reviews,
            "language": "en",
            "sort": "newest"
        }
        
        # Start actor run
        run_response = requests.post(run_url, json=actor_input, timeout=30)
        if run_response.status_code != 201:
            return jsonify({"error": f"Failed to start Apify actor: {run_response.text}"}), 500
        
        run_data = run_response.json()
        run_id = run_data["data"]["id"]
        
        # Poll for completion (max 5 min)
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_API_KEY}"
        max_wait = 300
        wait_time = 0
        
        while wait_time < max_wait:
            time.sleep(5)
            wait_time += 5
            
            status_response = requests.get(status_url, timeout=30)
            status_data = status_response.json()
            status = status_data["data"]["status"]
            
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                return jsonify({"error": f"Apify actor {status}"}), 500
        
        if wait_time >= max_wait:
            return jsonify({"error": "Timeout waiting for Apify results"}), 500
        
        # Fetch results
        dataset_id = status_data["data"]["defaultDatasetId"]
        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_KEY}"
        
        dataset_response = requests.get(dataset_url, timeout=30)
        results = dataset_response.json()
        
        # Parse and save reviews (append mode - skip duplicates)
        reviews_saved = 0
        reviews_skipped = 0
        for item in results:
            review_list = item.get("reviews", [item])
            for review in review_list[:max_reviews]:
                reviewer_name = review.get("name", review.get("reviewerName", "Anonymous"))
                review_date = review.get("publishedAtDate", review.get("date"))
                review_text = review.get("text", review.get("reviewText", ""))
                
                if not review_text:
                    continue
                
                # Check if this review already exists (by reviewer name + first 100 chars of text)
                # This is more reliable than date which can have format variations
                review_text_prefix = review_text[:100] if review_text else ""
                existing = supabase.table('google_reviews').select('id, review_text').eq('project_id', project_id).eq('reviewer_name', reviewer_name).execute()
                
                # Check if any existing review starts with the same text
                is_duplicate = False
                for ex in (existing.data or []):
                    ex_text = ex.get('review_text', '')[:100]
                    if ex_text == review_text_prefix:
                        is_duplicate = True
                        break
                
                if is_duplicate:
                    reviews_skipped += 1
                    continue  # Skip duplicate
                
                review_data = {
                    "project_id": project_id,
                    "reviewer_name": reviewer_name,
                    "star_rating": review.get("rating", review.get("stars", 5)),
                    "review_date": review_date,
                    "review_text": review_text
                }
                
                supabase.table('google_reviews').insert(review_data).execute()
                reviews_saved += 1
                    
                if reviews_saved >= max_reviews:
                    break
        
        # Get total review count for this project
        total_reviews = supabase.table('google_reviews').select('id', count='exact').eq('project_id', project_id).execute()
        total_count = total_reviews.count if total_reviews.count else reviews_saved
        
        # Update project review count
        supabase.table('medical_projects').update({
            "review_count": total_count
        }).eq('id', project_id).execute()
        
        return jsonify({
            "message": f"Scraped {reviews_saved} reviews",
            "reviews_saved": reviews_saved
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/classify-page', methods=['POST'])
def classify_page():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
        
    try:
        data = request.get_json()
        log_debug(f"DEBUG: classify_page received data: {data}")
        page_id = data.get('page_id')
        stage = data.get('stage') or data.get('funnel_stage')
        
        if not page_id or not stage:
            log_debug(f"DEBUG: Missing params. page_id={page_id}, stage={stage}")
            return jsonify({"error": "page_id and stage are required"}), 400
            
        # Try updating page_type instead of funnel_stage
        log_debug(f"DEBUG: Updating page_type to {stage} for {page_id}")
        
        update_data = {'page_type': stage}
        
        # ALWAYS set title from slug when moving to Product OR Category
        if stage == 'Product' or stage == 'Category':
            # Fetch current page data
            page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
            if page_res.data:
                page = page_res.data
                tech_data = page.get('tech_audit_data')
                
                # Robust JSON parsing
                if isinstance(tech_data, str):
                    try:
                        import json
                        tech_data = json.loads(tech_data)
                    except:
                        tech_data = {}
                elif not tech_data:
                    tech_data = {}
                
                # ALWAYS extract title from URL slug, no matter what
                new_title = get_title_from_url(page['url'])
                print(f"DEBUG: Setting title to '{new_title}' for {page['url']}")
                
                # Update tech_data
                tech_data['title'] = new_title
                update_data['tech_audit_data'] = tech_data
                print(f"DEBUG: update_data payload: {update_data}")
        
        supabase.table('pages').update(update_data).eq('id', page_id).execute()
        
        return jsonify({"message": f"Page classified as {stage}"})

    except Exception as e:
        log_debug(f"DEBUG: classify_page error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/auto-classify', methods=['POST'])
def auto_classify():
    # Log to a separate file to ensure we see it
    with open('debug_classify.log', 'a') as f:
        f.write(f"DEBUG: ENTERING auto_classify\n")
    
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        project_id = data.get('project_id')
        if not project_id: return jsonify({"error": "project_id required"}), 400
        
        # Fetch all pages for project, ordered by ID to ensure "list order"
        res = supabase.table('pages').select('id, url, page_type, tech_audit_data').eq('project_id', project_id).order('id').execute()
        all_pages = res.data
        
        # Prioritize Unclassified pages
        unclassified_pages = [p for p in all_pages if p.get('page_type') in [None, 'Unclassified', 'Other', '']]
        
        # LIMIT: Only take the first 50 unclassified pages
        pages = unclassified_pages[:50]
        
        with open('debug_classify.log', 'a') as f:
            f.write(f"DEBUG: Total pages: {len(all_pages)}. Unclassified: {len(unclassified_pages)}. Processing batch of: {len(pages)}\n")
        
        updated_count = 0
        
        for p in pages:
            current_type = p.get('page_type')
            
            # Log every URL
            with open('debug_classify.log', 'a') as f:
                f.write(f"DEBUG: Processing {p['url']} | Type: {current_type}\n")

            # Allow overwriting if it's Unclassified, None, empty, OR 'Other'
            # We ONLY skip if it's already 'Product' or 'Category'
            if current_type in ['Product', 'Category']:
                with open('debug_classify.log', 'a') as f:
                    f.write(f"DEBUG: SKIPPING {p['url']} (Already {current_type})\n")
                continue
                
            url = p['url'].lower()
            new_type = None
            
            # 1. Check Technical Data (Most Accurate)
            tech_data = p.get('tech_audit_data') or {}
            og_type = tech_data.get('og_type', '').lower()
            
            if 'product' in og_type:
                new_type = 'Product'
            elif 'service' in og_type:
                new_type = 'Service'
            elif 'article' in og_type or 'blog' in og_type:
                new_type = 'Category'
            
            # 2. URL Heuristics (Fallback)
            if not new_type:
                # Strict Product
                if any(x in url for x in ['/product/', '/products/', '/item/', '/p/', '/shop/']):
                    new_type = 'Product'
                
                # Strict Service
                elif any(x in url for x in ['/service/', '/services/', '/solution/', '/solutions/', '/consulting/', '/offering/']):
                    new_type = 'Service'

                # Categories / Content
                elif any(x in url for x in ['/category/', '/categories/', '/c/', '/collection/', '/collections/', '/blog/', '/blogs/', '/article/', '/news/']):
                    new_type = 'Category'
                
                # Expanded Content (Generic E-commerce/Blog terms)
                # 'culture', 'trend', 'backstage', 'editorial', 'guide' are common content markers
                elif 'culture' in url or 'trend' in url or 'artistry' in url or 'how-to' in url or 'backstage' in url or 'collections' in url or 'editorial' in url or 'guide' in url:
                    new_type = 'Category'
                
                # Common Beauty/Fashion Categories (Generic)
                # lips, face, eyes, skincare, brushes are standard industry categories
                elif any(f"/{x}" in url for x in ['lips', 'face', 'eyes', 'brushes', 'skincare', 'bestsellers', 'new', 'sets', 'gifts']):
                    new_type = 'Category'
                
                # Keywords that imply a collection/list (Generic)
                elif 'shades' in url or 'colours' in url or 'looks' in url or 'inspiration' in url:
                    new_type = 'Category'
                
                # Generic "products" list pattern
                elif 'trending-products' in url or url.endswith('-products'):
                    new_type = 'Category'
            
            if new_type:
                with open('debug_classify.log', 'a') as f:
                    f.write(f"DEBUG: MATCH! {url} -> {new_type}\n")
            else:
                with open('debug_classify.log', 'a') as f:
                    f.write(f"DEBUG: NO MATCH for {url}\n")
            
            if new_type:
                supabase.table('pages').update({'page_type': new_type}).eq('id', p['id']).execute()
                updated_count += 1
                
        return jsonify({"message": f"Auto-classified {updated_count} pages", "count": updated_count})

    except Exception as e:
        print(f"Auto-classify error: {e}")
        return jsonify({"error": str(e)}), 500

def get_title_from_url(url):
    try:
        from urllib.parse import urlparse
        path = urlparse(url).path
        # Get last non-empty segment
        segments = [s for s in path.split('/') if s]
        if not segments: return "Home"
        slug = segments[-1]
        # Convert slug to title (e.g., "my-page-title" -> "My Page Title")
        return slug.replace('-', ' ').replace('_', ' ').title()
    except:
        return "Untitled Page"

def scrape_page_details(url):
    """Scrape detailed technical data for a single page."""
    import requests
    from bs4 import BeautifulSoup
    import time
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    }
    
    data = {
        'status_code': 0,
        'title': '',
        'meta_description': '',
        'h1': '',
        'canonical': '',
        'word_count': 0,
        'og_title': '',
        'og_description': '',
        'has_schema': False,
        'missing_alt_count': 0,
        'missing_h1': False,
        'onpage_score': 0,
        'load_time_ms': 0,
        'checks': [],
        'error': None
    }
    
    try:
        # Rate Limit: Sleep 1 second before scraping to be polite
        time.sleep(1)
        
        start_time = time.time()
        
        # Use curl to bypass TLS fingerprinting
        content, latency = fetch_with_curl(url)
        data['load_time_ms'] = int(latency * 1000)
        
        if content:
            data['status_code'] = 200 # Assume 200 if curl returns content
            soup = BeautifulSoup(content, 'html.parser')
            
            # Title
            # Robust extraction: Find first title not in SVG/Symbol
            page_title = None
            
            # 1. Try head > title first
            head_title = soup.select_one('head > title')
            if head_title and head_title.string:
                page_title = head_title
            
            # 2. Fallback: Search all titles and filter
            if not page_title:
                all_titles = soup.find_all('title')
                for t in all_titles:
                    # Check if parent or grandparent is SVG-related
                    parents = [p.name for p in t.parents]
                    if not any(x in ['svg', 'symbol', 'defs', 'g'] for x in parents):
                        page_title = t
                        break
            
            if page_title:
                data['title'] = page_title.get_text(strip=True)
            else:
                data['title'] = get_title_from_url(url)
                
            data['title_length'] = len(data['title'])
            
            # Meta Description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                data['meta_description'] = meta_desc.get('content', '').strip()
                data['description_length'] = len(data['meta_description'])
                
            # H1
            h1 = soup.find('h1')
            if h1:
                data['h1'] = h1.get_text(strip=True)
            else:
                data['missing_h1'] = True
                data['checks'].append("Missing H1")
                
            # Canonical
            canonical = soup.find('link', attrs={'rel': 'canonical'})
            if canonical:
                data['canonical'] = canonical.get('href', '').strip()
            else:
                # Fallback regex for malformed HTML
                import re
                match = re.search(r'<link[^>]*rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']', content)
                if match:
                    data['canonical'] = match.group(1).strip()
                
            # Word Count (rough estimate)
            text = soup.get_text(separator=' ')
            data['word_count'] = len(text.split())
            
        # Click Depth (Proxy: URL Depth)
            # Count slashes after the domain. 
            # e.g. https://domain.com/ = 0
            # https://domain.com/page = 1
            # https://domain.com/blog/post = 2
            from urllib.parse import urlparse
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            data['click_depth'] = 0 if not path else len(path.split('/'))
            
            # OG Tags
            # Initialize with 'Missing' to allow fallback logic to work
            data['og_title'] = 'Missing'
            data['og_description'] = 'Missing'
            data['og_image'] = None

            og_title_tag = soup.find('meta', property='og:title') or soup.find('meta', attrs={'name': 'og:title'})
            if og_title_tag and og_title_tag.get('content'):
                data['og_title'] = og_title_tag['content'].strip()
            
            og_desc_tag = soup.find('meta', property='og:description') or soup.find('meta', attrs={'name': 'og:description'})
            if og_desc_tag and og_desc_tag.get('content'):
                data['og_description'] = og_desc_tag['content'].strip()
            
            og_image_tag = soup.find('meta', property='og:image') or soup.find('meta', attrs={'name': 'og:image'})
            if og_image_tag and og_image_tag.get('content'):
                data['og_image'] = og_image_tag['content'].strip()

            # FALLBACK: JSON-LD Schema (Common in Shopify/Wordpress if OG tags are missing/JS-rendered)
            if data['og_title'] == 'Missing' or data['og_description'] == 'Missing' or not data['og_image']:
                try:
                    import json
                    schemas = soup.find_all('script', type='application/ld+json')
                    for schema in schemas:
                        if not schema.string: continue
                        try:
                            json_data = json.loads(schema.string)
                            # Handle list of schemas
                            if isinstance(json_data, list):
                                items = json_data
                            else:
                                items = [json_data]
                                
                            for item in items:
                                # Prioritize Product, then Article, then WebPage
                                item_type = item.get('@type', '')
                                if isinstance(item_type, list): item_type = item_type[0] # Handle type as list
                                
                                if item_type in ['Product', 'Article', 'BlogPosting', 'WebPage']:
                                    if data['og_title'] == 'Missing' and item.get('name'):
                                        data['og_title'] = item['name']
                                        print(f"DEBUG: Recovered OG Title from Schema ({item_type})")
                                        
                                    if data['og_description'] == 'Missing' and item.get('description'):
                                        data['og_description'] = item['description']
                                        print(f"DEBUG: Recovered OG Desc from Schema ({item_type})")
                                        
                                    if not data['og_image'] and item.get('image'):
                                        img = item['image']
                                        if isinstance(img, list): img = img[0]
                                        elif isinstance(img, dict): img = img.get('url')
                                        data['og_image'] = img
                        except:
                            continue
                except Exception as e:
                    print(f"DEBUG: Schema parsing failed: {e}")

            # Schema
            schema = soup.find('script', type='application/ld+json')
            if schema: data['has_schema'] = True
            
            # Missing Alt Tags
            images = soup.find_all('img')
            for img in images:
                if not img.get('alt'):
                    data['missing_alt_count'] += 1
            
            # Calculate OnPage Score (Simple Heuristic)
            score = 100
            if data['missing_h1']: score -= 20
            if not data['title']: score -= 20
            if not data['meta_description']: score -= 20
            if data['missing_alt_count'] > 0: score -= min(10, data['missing_alt_count'] * 2)
            if data['word_count'] < 300: score -= 10
            if not data['og_title']: score -= 5
            if not data['og_description']: score -= 5
            
            data['onpage_score'] = max(0, score)
            
            # Technical Checks
            data['is_redirect'] = False # Cannot detect redirects easily with simple curl
            data['is_4xx_code'] = 400 <= data['status_code'] < 500
            data['is_5xx_code'] = 500 <= data['status_code'] < 600
            data['is_broken'] = data['status_code'] >= 400
            data['high_loading_time'] = data['load_time_ms'] > 30000 # Relaxed to 30s for Railway
            
            # Canonical Mismatch
            if data['canonical']:
                # Normalize URLs for comparison (remove trailing slash, etc)
                norm_url = url.rstrip('/')
                norm_canon = data['canonical'].rstrip('/')
                data['canonical_mismatch'] = norm_url != norm_canon
            else:
                data['canonical_mismatch'] = False # Or True if strict? Let's say False if missing.

    except Exception as e:
        data['error'] = str(e)
        data['is_broken'] = True
        print(f"Error scraping {url}: {e}")
        
    return data

def perform_tech_audit(project_id, limit=5):
    """Audit existing pages that are missing technical data."""
    print(f"Starting technical audit for project {project_id} (Limit: {limit})...")
    
    # 1. Get pages that need auditing (prioritize those without tech data)
    # Fetch all pages (or a large batch) and filter in python
    res = supabase.table('pages').select('id, url, tech_audit_data').eq('project_id', project_id).order('id').execute()
    all_pages = res.data
    
    # Filter for pages that have NO tech_audit_data, or "Pending Scan", or failed status (403/429)
    unaudited_pages = []
    for p in all_pages:
        tech = p.get('tech_audit_data') or {}
        status = tech.get('status_code')
        
        # Retry if:
        # 1. No data
        # 2. Title is missing or "Pending Scan"
        # 3. Status is Forbidden (403) or Rate Limited (429) or 0/None
        if not tech or \
           not tech.get('title') or \
           tech.get('title') == 'Pending Scan' or \
           status in [403, 429, 406, 0, None]:
            unaudited_pages.append(p)
            
    # Take the first 'limit' pages
    pages = unaudited_pages[:limit]
    print(f"DEBUG: Found {len(unaudited_pages)} unaudited pages. Processing first {len(pages)}.")
    
    audited_count = 0
    errors = []
    
    # Helper function for parallel execution
    def audit_single_page(p):
        try:
            url = p['url']
            print(f"DEBUG: Auditing {url}...")
            
            tech_data = scrape_page_details(url)
            # print(f"DEBUG: Scraped {url}. Status: {tech_data.get('status_code')}")
            
            # Merge with existing data
            existing_data = p.get('tech_audit_data') or {}
            existing_data.update(tech_data)
            
            # Update DB
            # print(f"DEBUG: Updating DB for {url}...")
            update_payload = {
                'tech_audit_data': existing_data
            }
            
            # Also update top-level columns if found
            if tech_data.get('title') and tech_data.get('title') != 'Pending Scan':
                update_payload['title'] = tech_data['title']
                
            if tech_data.get('meta_description'):
                update_payload['meta_description'] = tech_data['meta_description']
                
            supabase.table('pages').update(update_payload).eq('id', p['id']).execute()
            
            print(f"DEBUG: Successfully audited {url}")
            return True, p
        except Exception as e:
            print(f"ERROR: Failed to audit {p.get('url')}: {e}")
            # Mark error in object for reporting
            if not p.get('tech_audit_data'): p['tech_audit_data'] = {}
            p['tech_audit_data']['error'] = str(e)
            return False, p

    # Execute sequentially (User requested efficiency/stability over speed)
    for p in pages:
        success, result_p = audit_single_page(p)
        if success:
            audited_count += 1
        else:
            errors.append(result_p)
        
    print(f"Audit complete. Updated {audited_count} pages.")
    return audited_count, errors

@app.route('/api/run-project-setup', methods=['POST'])
def run_project_setup():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    data = request.json
    project_id = data.get('project_id')
    do_audit = data.get('do_audit', False)
    do_tech_audit = data.get('do_tech_audit', False)
    do_profile = data.get('do_profile', False)
    max_pages = data.get('max_pages', 200)
    
    if not project_id:
        return jsonify({"error": "Project ID required"}), 400
        
    try:
        # 1. Tech Audit (Standalone)
        if do_tech_audit:
            count, errors = perform_tech_audit(project_id)
            
            msg = f"Audit complete. Updated {count} pages."
            if count == 0 and len(errors) > 0:
                msg += " (Check console for details)"
                
            return jsonify({
                "message": msg,
                "count": count,
                "details": [f"Failed: {p.get('url')}" for p in errors if p.get('tech_audit_data', {}).get('error')]
            })

        if not project_id: return jsonify({"error": "project_id required"}), 400
        
        # Fetch Project Details
        proj_res = supabase.table('projects').select('*').eq('id', project_id).execute()
        if not proj_res.data: return jsonify({"error": "Project not found"}), 404
        project = proj_res.data[0]
        domain = project['domain']
        
        print(f"Starting Setup for {domain} (Audit: {do_audit}, Tech Audit: {do_tech_audit}, Profile: {do_profile}, Max Pages: {max_pages})...")
        
        profile_data = {}
        strategy_plan = ""
        profile_insert = {} # Initialize to empty dict
        
        # 0. Technical Audit (Deep Dive) - NEW
        if do_tech_audit:
             print(f"[SCRAPER] Starting technical audit for project {project_id}...")
             try:
                 count = perform_tech_audit(project_id, limit=max_pages)
                 print(f"[SCRAPER] ✅ Technical audit completed successfully. Audited {count} pages.")
                 return jsonify({"message": f"Technical audit completed for {count} pages.", "pages_audited": count})
             except Exception as audit_error:
                 error_msg = f"Technical audit failed: {str(audit_error)}"
                 print(f"[SCRAPER] ❌ ERROR: {error_msg}")
                 import traceback
                 traceback.print_exc()
                 return jsonify({"error": error_msg}), 500


        # 1. Research Business (The Brain)
        if do_profile:
            print("Starting Gemini research...")
            # try:
            #     tools = [{'google_search': {}}]
            #     model = genai.GenerativeModel('gemini-2.0-flash-exp', tools=tools)
            # except:
            #     print("Warning: Google Search tool failed. Using standard model.")
            #     model = genai.GenerativeModel('gemini-2.0-flash-exp')
            
            prompt = f"""
            You are an expert business analyst. Research the website {domain} and create a comprehensive Business Profile.
            
            Context:
            - Language: {project.get('language')}
            - Location: {project.get('location')}
            - Focus: {project.get('focus')}
            
            I need you to find:
            1. Business Summary: What do they do? (1 paragraph)
            2. Ideal Customer Profile (ICP): Who are they selling to? Be specific.
            3. Brand Voice: How do they sound?
            4. Primary Products: List their main products/services.
            5. Competitors: List 3-5 potential competitors.
            6. Unique Selling Points (USPs): What makes them different?
            
            Return JSON:
            {{
                "business_summary": "...",
                "ideal_customer_profile": "...",
                "brand_voice": "...",
                "primary_products": ["..."],
                "competitors": ["..."],
                "unique_selling_points": ["..."]
            }}
            """
            
            text = gemini_client.generate_content(
                prompt=prompt,
                model_name="gemini-2.5-flash",
                use_grounding=True
            )
            
            if not text:
                raise Exception("Gemini generation failed for Business Profile")
            
            # Parse JSON
            import json
            if text.startswith('```json'): text = text[7:]
            if text.startswith('```'): text = text[3:]
            if text.endswith('```'): text = text[:-3]
            
            profile_data = json.loads(text.strip())
            
            # 2. Generate Content Strategy Plan
            print("Generating Strategy Plan...")
            strategy_prompt = f"""
            Based on this business profile:
            {json.dumps(profile_data)}
            
            **CONTEXT**:
            - Target Audience Location: {project.get('location')}
            - Target Language: {project.get('language')}
            
            Create a high-level Content Strategy Plan following the "Bottom-Up" approach:
            1. Bottom Funnel (BoFu): What product/service pages need optimization?
            2. Middle Funnel (MoFu): What comparison/best-of topics link to BoFu?
            3. Top Funnel (ToFu): What informational topics link to MoFu?
            
            Return a short markdown summary of the strategy.
            """
            strategy_plan = gemini_client.generate_content(
                prompt=strategy_prompt,
                model_name="gemini-2.5-flash",
                use_grounding=True
            )
            if not strategy_plan: strategy_plan = ""
            
            # Save Business Profile
            # WORKAROUND: Append Strategy Plan to Business Summary for persistence
            combined_summary = profile_data.get("business_summary", "")
            if strategy_plan:
                combined_summary += "\n\n===STRATEGY_PLAN===\n\n" + strategy_plan

            profile_insert = {
                "project_id": project_id,
                "business_summary": combined_summary,
                "ideal_customer_profile": profile_data.get("ideal_customer_profile"),
                "brand_voice": profile_data.get("brand_voice"),
                "primary_products": profile_data.get("primary_products"),
                "competitors": profile_data.get("competitors"),
                "unique_selling_points": profile_data.get("unique_selling_points")
            }
            
            # Check if exists, update or insert
            existing = supabase.table('business_profiles').select('id').eq('project_id', project_id).execute()
            if existing.data:
                supabase.table('business_profiles').update(profile_insert).eq('id', existing.data[0]['id']).execute()
            else:
                supabase.table('business_profiles').insert(profile_insert).execute()

        # 3. Crawl Sitemap (The Map)
        pages_to_insert = []
        
        if do_audit:
            print(f"Starting sitemap crawl (Audit enabled, Max Pages: {max_pages})...")
            pages_to_insert = crawl_sitemap(domain, project_id, max_pages=max_pages)
            
            if pages_to_insert:
                print(f"Found {len(pages_to_insert)} pages. syncing with DB...")
                
                # 1. Get existing URLs to avoid duplicates
                existing_res = supabase.table('pages').select('url, id, tech_audit_data').eq('project_id', project_id).execute()
                existing_map = {row['url']: row for row in existing_res.data}
                
                new_pages = []
                
                for p in pages_to_insert:
                    url = p['url']
                    if url in existing_map:
                        # Update existing page if title is missing or we have a better one
                        existing_row = existing_map[url]
                        existing_data = existing_row.get('tech_audit_data') or {}
                        new_data = p.get('tech_audit_data') or {}
                        
                        # If existing has no title, or we want to refresh it
                        if not existing_data.get('title') or new_data.get('title') != 'Untitled Product':
                            # Merge data
                            updated_data = existing_data.copy()
                            updated_data.update(new_data)
                            
                            # Only update if changed
                            if updated_data != existing_data:
                                print(f"Updating title for {url}")
                                supabase.table('pages').update({'tech_audit_data': updated_data}).eq('id', existing_row['id']).execute()
                    else:
                        new_pages.append(p)
                
                # 2. Insert only new pages
                if new_pages:
                    print(f"Inserting {len(new_pages)} new pages...")
                    batch_size = 100
                    for i in range(0, len(new_pages), batch_size):
                        batch = new_pages[i:i+batch_size]
                        supabase.table('pages').insert(batch).execute()
                    
                #3. Update project page_count field (DISABLED - column doesn't exist in schema)
                # total_pages = supabase.table('pages').select('*', count='exact').eq('project_id', project_id).execute()
                # supabase.table('projects').update({
                #     'page_count': total_pages.count
                # }).eq('id', project_id).execute()
                # print(f"Updated project page_count to {total_pages.count}")
                print(f"Inserted {len(new_pages)} new pages successfully.")
        else:
            print("Audit disabled. Skipping crawl.")
                
        return jsonify({
            "message": "Project setup complete",
            "profile": profile_insert,
            "strategy_plan": strategy_plan,
            "pages_found": len(pages_to_insert),
            "audit_run": do_audit
        })

    except Exception as e:
        print(f"Error in run_project_setup: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/generate-funnel', methods=['POST'])
def generate_funnel():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
        
    try:
        data = request.get_json()
        page_id = data.get('page_id')
        project_id = data.get('project_id')
        current_stage = data.get('current_stage', 'BoFu') # Default to BoFu if not sent
        
        if not page_id or not project_id:
            return jsonify({"error": "page_id and project_id are required"}), 400
            
        # 1. Fetch Context
        profile_res = supabase.table('business_profiles').select('*').eq('project_id', project_id).execute()
        profile = profile_res.data[0] if profile_res.data else {}
        
        page_res = supabase.table('pages').select('*').eq('id', page_id).execute()
        page = page_res.data[0] if page_res.data else {}
        
        target_stage = "MoFu" if current_stage == 'BoFu' else "ToFu"
        
        print(f"Generating {target_stage} strategy for {page.get('url')}...")
        
        # 2. Prompt Gemini
        prompt = f"""
        You are a strategic SEO expert for this business:
        Summary: {profile.get('business_summary')}
        ICP: {profile.get('ideal_customer_profile')}
        
        We are building a Content Funnel.
        Current Page ({current_stage}): {page.get('title')} ({page.get('url')})
        
        Task: Generate 5 high-impact "{target_stage}" content ideas that will drive traffic to this Current Page.
        
        Definitions:
        - If Target is MoFu (Middle of Funnel): Generate "Comparison", "Best X for Y", or "Alternative to Z" articles. These help users evaluate options.
        - If Target is ToFu (Top of Funnel): Generate "How-to", "What is", or "Guide" articles. These help users understand the problem.
        
        Output JSON format:
        [
            {{
                "topic_title": "Title of the article",
                "primary_keyword": "Main SEO keyword",
                "rationale": "Why this drives traffic to the parent page"
            }}
        ]
        """
        
        # model = genai.GenerativeModel('gemini-2.0-flash-exp')
        # response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        
        text = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-flash",
            use_grounding=True
        )
        
        if not text:
            raise Exception("Gemini generation failed for Content Strategy")
            
        # Clean markdown
        if text.startswith('```json'): text = text[7:]
        if text.startswith('```'): text = text[3:]
        if text.endswith('```'): text = text[:-3]
        
        # 3. Parse and Save
        ideas = json.loads(text.strip())
        
        # 4. Enrich with DataForSEO (Optional)
        try:
            keywords = [idea.get('primary_keyword') for idea in ideas if idea.get('primary_keyword')]
            keyword_data = fetch_keyword_data(keywords)
        except Exception as e:
            print(f"DataForSEO Error: {e}")
            keyword_data = {}

        briefs_to_insert = []
        for idea in ideas:
            kw = idea.get('primary_keyword')
            data = keyword_data.get(kw, {})
            
            briefs_to_insert.append({
                "project_id": project_id,
                "topic_title": idea.get('topic_title'),
                "primary_keyword": kw,
                "rationale": idea.get('rationale'),
                "parent_page_id": page_id, 
                "status": "Proposed",
                "funnel_stage": target_stage,
                "meta_data": data # Store volume/kd here
            })
            
        if briefs_to_insert:
            supabase.table('content_briefs').insert(briefs_to_insert).execute()
            
            # SYNC TO PAGES TABLE (Fix for Dashboard Visibility)
            pages_to_insert = []
            for brief in briefs_to_insert:
                pages_to_insert.append({
                    "project_id": project_id,
                    "url": f"pending-slug-{uuid.uuid4()}", # Placeholder URL
                    "page_type": "Topic",
                    "funnel_stage": target_stage,
                    "source_page_id": page_id,
                    "tech_audit_data": {"title": brief['topic_title']},
                    "content_description": brief['rationale'],
                    "keywords": brief['primary_keyword']
                })
            
            if pages_to_insert:
                print(f"Syncing {len(pages_to_insert)} topics to pages table...")
                supabase.table('pages').insert(pages_to_insert).execute()
            
        return jsonify({
            "message": f"Generated {len(briefs_to_insert)} {target_stage} ideas",
            "ideas": briefs_to_insert
        })

    except Exception as e:
        print(f"Error in generate_funnel: {e}")
        return jsonify({"error": str(e)}), 500

def fetch_keyword_data(keywords):
    if not keywords: 
        print("No keywords provided to fetch_keyword_data")
        return {}
    
    login = os.environ.get('DATAFORSEO_LOGIN')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    
    print(f"DataForSEO Login: {login}, Password: {'*' * len(password) if password else 'None'}")
    
    if not login or not password:
        print("DataForSEO credentials missing")
        return {}
        
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
    
    # We can use 'keyword_ideas' or 'keywords_for_site' or just 'search_volume'
    # 'search_volume' is best for specific lists.
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/historical_search_volume/live"
    
    payload = [{
        "keywords": keywords,
        "location_code": 2840, # US
        "language_code": "en"
    }]
    
    try:
        print(f"Fetching keyword data for {len(keywords)} keywords: {keywords[:3]}...")
        response = requests.post(url, auth=(login, password), json=payload)
        res_data = response.json()
        
        print(f"DataForSEO Response Status: {response.status_code}")
        print(f"DataForSEO Response: {res_data}")
        
        result = {}
        if res_data.get('tasks') and len(res_data['tasks']) > 0:
            task_result = res_data['tasks'][0].get('result')
            if task_result:
                for item in task_result:
                    kw = item.get('keyword')
                    vol = item.get('search_volume', 0)
                    result[kw] = {"volume": vol}
                    print(f"Keyword '{kw}': Volume = {vol}")
            else:
                print("No result in task")
        else:
            print("No tasks in response")
                
        return result
        
    except Exception as e:
        print(f"DataForSEO Request Failed: {e}")
        import traceback
        traceback.print_exc()
        return {}

def validate_and_enrich_keywords(ai_keywords_str, topic_title, min_volume=100):
    """
    Validates AI-generated keywords against DataForSEO search volume data.
    Replaces low-volume keywords with high-value alternatives.
    
    Args:
        ai_keywords_str: Comma-separated keyword string from AI
        topic_title: Topic title to use for finding alternatives if needed
        min_volume: Minimum monthly search volume threshold (default: 100)
    
    Returns:
        str: Comma-separated validated keywords with volume annotations
    """
    if not ai_keywords_str:
        return ""
    
    # Parse AI keywords
    ai_keywords = [k.strip() for k in ai_keywords_str.split(',') if k.strip()]
    if not ai_keywords:
        return ""
    
    print(f"Validating {len(ai_keywords)} AI keywords: {ai_keywords[:3]}...")
    
    # Fetch search volume data
    keyword_data = fetch_keyword_data(ai_keywords)
    
    # Filter and format keywords with volume
    validated_keywords = []
    for kw in ai_keywords:
        data = keyword_data.get(kw, {})
        volume = data.get('volume', 0)
        
        if volume >= min_volume:
            validated_keywords.append(f"{kw} (Vol: {volume})")
            print(f"✓ Kept '{kw}' - Volume: {volume}")
        else:
            print(f"✗ Rejected '{kw}' - Volume: {volume} (below threshold)")
    
    # If we have fewer than 3 good keywords, try to find alternatives
    if len(validated_keywords) < 3:
        print(f"Only {len(validated_keywords)} validated keywords. Searching for alternatives...")
        
        try:
            login = os.environ.get('DATAFORSEO_LOGIN')
            password = os.environ.get('DATAFORSEO_PASSWORD')
            
            if login and password:
                url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
                payload = [{
                    "keywords": [topic_title],
                    "location_code": 2840,
                    "language_code": "en",
                    "include_seed_keyword": False,
                    "filters": [
                        ["keyword_data.keyword_info.search_volume", ">=", min_volume]
                    ],
                    "order_by": ["keyword_data.keyword_info.search_volume,desc"],
                    "limit": 10
                }]
                
                response = requests.post(url, auth=(login, password), json=payload)
                res_data = response.json()
                
                if res_data.get('tasks') and res_data['tasks'][0].get('result'):
                    for item in res_data['tasks'][0]['result'][0].get('items', []):
                        kw = item['keyword']
                        volume = item['keyword_data']['keyword_info']['search_volume']
                        
                        # Avoid duplicates
                        if not any(kw.lower() in vk.lower() for vk in validated_keywords):
                            validated_keywords.append(f"{kw} (Vol: {volume})")
                            print(f"+ Added alternative '{kw}' - Volume: {volume}")
                            
                            if len(validated_keywords) >= 5:
                                break
        except Exception as e:
            print(f"Error fetching keyword alternatives: {e}")
    
    # Return top 5 validated keywords
    result = ', '.join(validated_keywords[:5])
    print(f"Final validated keywords: {result}")
    return result



def analyze_serp_for_keyword(keyword, location_code=2840):
    """
    Fetches top 10 SERP results for a keyword using DataForSEO.
    Returns competitor data: titles, URLs, ranking positions.
    """
    login = os.environ.get('DATAFORSEO_LOGIN')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    
    if not login or not password:
        print("DataForSEO credentials missing for SERP analysis")
        return []
    
    try:
        url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": "en",
            "device": "desktop",
            "depth": 10
        }]
        
        print(f"Analyzing SERP for '{keyword}'...")
        response = requests.post(url, auth=(login, password), json=payload)
        data = response.json()
        
        competitors = []
        if data.get('tasks') and data['tasks'][0].get('result') and data['tasks'][0]['result'][0].get('items'):
            for item in data['tasks'][0]['result'][0]['items']:
                if item.get('type') == 'organic':
                    competitors.append({
                        'url': item.get('url'),
                        'title': item.get('title'),
                        'position': item.get('rank_absolute'),
                        'domain': item.get('domain')
                    })
                    print(f"  #{item.get('rank_absolute')}: {item.get('domain')} - {item.get('title')}")
        
        print(f"Found {len(competitors)} competitors for '{keyword}'")
        return competitors
        
    except Exception as e:
        print(f"SERP analysis error for '{keyword}': {e}")
        import traceback
        traceback.print_exc()
        return []

        return []


def perform_gemini_research(topic, location="US", language="English"):
    """
    Uses Gemini 2.0 Flash with Google Search Grounding to perform free research.
    Returns structured data: {
        "competitors": [{"url": "...", "title": "...", "domain": "..."}],
        "keywords": [{"keyword": "...", "intent": "...", "volume": "N/A"}],
        "research_brief": "Markdown content...",
        "citations": ["url1", "url2"]
    }
    """
    log_debug(f"Starting Gemini 2.5 Flash Grounded Research for: {topic} (Loc: {location}, Lang: {language})")
    
    try:

        # Use gemini_client for pure REST API calls (No SDK)
        
        prompt = f"""
        Research the SEO topic: "{topic}"
        
        **CONTEXT**:
        - Target Audience Location: {location}
        - Target Language: {language}
        
        Perform a deep analysis using Google Search to find:
        1. Top 3 Competitor URLs ranking for this topic in **{location}**.
        2. **At least 30 SEO Keywords** relevant to this topic (include Search Intent).
           - Focus on keywords trending in **{location}**.
           - Mix of short-tail and long-tail.
           - Include "People Also Ask" style questions relevant to this region.
           
        **PRIORITIZATION RULES**:
        1. **Primary Focus**: Prioritize keywords specifically trending in **{location}**.
        2. **Global Keywords**: You MAY include high-volume US/Global keywords if they are highly relevant, but they must be secondary to local terms.
        3. **Relevance**: Ensure all keywords are actionable for a user in {location}.
        
        Output strictly in JSON format:
        {{
            "competitors": [
                {{"url": "https://...", "title": "Page Title", "domain": "domain.com"}}
            ],
            "keywords": [
                {{"keyword": "keyword phrase", "intent": "Informational/Commercial/Transactional"}}
            ]
        }}
        """
        
        text = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-flash",
            use_grounding=True
        )
        
        if not text:
            raise Exception("Empty response from Gemini REST API")
        
        # Clean markdown code blocks if present
        if text.startswith('```json'): text = text[7:]
        if text.startswith('```'): text = text[3:]
        if text.endswith('```'): text = text[:-3]
            
        return json.loads(text.strip())
        
    except Exception as e:
        log_debug(f"Gemini Research Failed: {e}")
        return None

def generate_image_prompt(topic, summary=""):
    """Generates an image prompt using Gemini."""
    prompt = f"""
    Create a detailed image generation prompt for a blog post titled: "{topic}"
    Summary: {summary[:500]}

    The image should be:
    - Visually matching the theme and tone of the article (e.g., if it's about nature, use natural elements; if tech, use modern tech aesthetics).
    - High quality, photorealistic or 3D render style.
    - No text in the image.
    - Aspect Ratio: 16:9

    Output ONLY the prompt text, no explanations.
    """
    try:
        return gemini_client.generate_content(prompt=prompt, model_name="gemini-2.5-flash")
    except Exception as e:
        print(f"Error generating image prompt: {e}")
        return f"A professional, modern header image for a blog post about {topic}, high quality, 4k, no text"


def research_with_perplexity(query, location="US", language="English"):
    """
    Uses Perplexity Sonar to get verifiable research with citations.
    Returns structured research data with source URLs.
    """
    log_debug(f"research_with_perplexity called (Loc: {location}, Lang: {language})")
    api_key = os.environ.get('PERPLEXITY_API_KEY')
    
    if not api_key:
        log_debug("Perplexity API key missing - skipping research")
        print("Perplexity API key missing - skipping research")
        return {"research": "Perplexity API not configured", "citations": []}
    
    log_debug(f"Perplexity API key found: {api_key[:10]}...")
    
    try:
        url = "https://api.perplexity.ai/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "sonar-pro",  # Using deep research model
            "messages": [{
                "role": "user",
                "content": f"""**Role**: You are a Senior Content Strategist and Market Researcher conducting deep-dive competitive analysis.

**Objective**: Create a comprehensive Research Brief for a Middle-of-Funnel (MoFu) content asset. This must be the MOST authoritative resource on this topic, outranking all competitors with superior data, utility, and insight.

**CONTEXT**:
- Target Audience Location: {location}
- Target Language: {language}

**LOCALIZATION RULES (CRITICAL)**:
1. **Currency**: You MUST use the local currency for **{location}** (e.g., ₹ INR for India). Convert any research prices (like $) to the local currency using approximate current rates.
2. **Units**: Use the measurement system standard for **{location}**.
3. **Spelling**: Use the correct spelling dialect (e.g., "Colour" for UK/India).

{query}

**CRITICAL RULES**:
- GENERATE A COMPLETE BRIEF based on the provided data and your general knowledge
- Use the provided competitor URLs and scraped text as your primary source
- If specific data is missing, use INDUSTRY BENCHMARKS or GENERAL CATEGORY KNOWLEDGE relevant to **{location}**
- Do not refuse to generate sections - provide the best available estimates
- Format as markdown with ## headers

---

## 1. Strategic Overview

**Proposed Title**: [SEO-optimized H1 using "Best X for Y 2025" or "Product A vs B vs C" format]

**Search Intent**: [Analyze based on the provided keyword list: Informational/Commercial/Transactional]

**Format Strategy**: [Why this format fits the MoFu stage]

---

## 2. Key Insights & Benchmarks (The Evidence)

**Market Data & Specifications** (Extract from content or use category knowledge):
- [Key Feature/Spec 1]: [Value/Description]
- [Key Feature/Spec 2]: [Value/Description]
- [Price Range]: [Estimated category range]
- [User Ratings]: [Typical sentiment/rating]
- [Technical Specs]: [Ingredients, dimensions, etc.]

**Expert/Industry Concepts**:
- [Key Concept 1]: [Explanation]
- [Key Concept 2]: [Explanation]

---

## 3. Competitor Landscape & Content Gaps

**Competitor Analysis** (Based on provided URLs):
- **Competitor 1**: [Name/URL]
  - Strengths: [What they cover well]
  - Weaknesses: [What they miss]
- **Competitor 2**: [Name/URL]
  - Strengths: [What they cover well]
  - Weaknesses: [What they miss]

**The "Blue Ocean" Gap**: [The ONE angle or utility missing from the above competitors. E.g., "No one compares X vs Y directly" or "Missing detailed ingredient breakdown"]

---

## 4. Comprehensive Content Outline

**Type**: [Comparison Guide / Buying Guide / Ultimate Guide]

**Title**: [Final SEO-optimized H1]

**Detailed Structure**:

### H2: Introduction
- Hook: [Problem/Stat]
- Scope: [What's covered]

### H2: [Main Section 1 - Category Overview]
- H3: [Subtopic from keyword list]
  - **Key Point**: [Detail]
- H3: [Subtopic from keyword list]
  - **Key Point**: [Detail]

### H2: [Comparison Section]
- H3: Comparison Chart
  - **Columns**: [Attribute 1], [Attribute 2], [Attribute 3]
  - **Data Source**: [Competitor content or benchmarks]
- H3: [Product A] vs [Competitors]
  - **Differentiator**: [Specific advantage]

### H2: [Buying Guide / Selection Criteria]
- H3: Who is this for?
  - **User Type 1**: [Recommendation]
  - **User Type 2**: [Recommendation]

### H2: FAQ
- [Question from keyword list]: [Answer]
- [Question from keyword list]: [Answer]

### H2: Conclusion
- Final Recommendation
- CTA

---

## 5. Unique Ranking Hypothesis

[Explain why this content will outrank competitors based on the gaps identified above. Focus on: Better data, clearer structure, or more comprehensive scope.]

**GENERATE THE COMPLETE BRIEF NOW.**
"""
            }],
            "return_citations": True,
            "search_recency_filter": "month"
        }
        
        log_debug(f"Calling Perplexity API with query: {query[:50]}...")
        print(f"Researching with Perplexity: {query[:100]}...")
        # Increased timeout to 180s for deep research
        response = requests.post(url, headers=headers, json=payload, timeout=180)
        log_debug(f"Perplexity response status: {response.status_code}")
        
        data = response.json()
        
        if 'choices' in data and len(data['choices']) > 0:
            content = data['choices'][0]['message']['content']
            citations = data.get('citations', [])
            
            log_debug(f"✓ Perplexity success! {len(citations)} citations")
            print(f"✓ Research completed. Found {len(citations)} citations")
            for i, cite in enumerate(citations[:3]):
                print(f"  Citation {i+1}: {cite}")
            
            return {
                "research": content,
                "citations": citations
            }
        else:
            log_debug(f"Unexpected Perplexity response structure: {str(data)[:200]}")
            print(f"Unexpected Perplexity response: {data}")
            return {"research": "Research failed", "citations": []}
            
    except Exception as e:
        log_debug(f"Perplexity error: {type(e).__name__} - {str(e)}")
        print(f"Perplexity research error: {e}")
        import traceback
        traceback.print_exc()
        return {"research": f"Error: {str(e)}", "citations": []}


def get_keyword_ideas(seed_keyword, location_code=2840, min_volume=100, limit=20):
    """
    Gets keyword ideas from DataForSEO based on a seed keyword.
    Returns list of keywords scored by (Volume × CPC) / Competition.
    Prioritizes high-intent, low-competition opportunities.
    """
    login = os.environ.get('DATAFORSEO_LOGIN')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    
    if not login or not password:
        print("DataForSEO credentials missing for keyword research")
        return []
    
    try:
        url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
        payload = [{
            "keywords": [seed_keyword],
            "location_code": location_code,
            "language_code": "en",
            "include_seed_keyword": True,
            "limit": 100
        }]
        
        print(f"Finding keyword ideas for '{seed_keyword}'...")
        log_debug(f"DataForSEO request: seed='{seed_keyword}', location={location_code}, min_vol={min_volume}")
        response = requests.post(url, auth=(login, password), json=payload, timeout=30)
        log_debug(f"DataForSEO status: {response.status_code}")
        data = response.json()
        
        keywords = []
        if data.get('tasks') and data['tasks'][0].get('result') and data['tasks'][0]['result'][0].get('items'):
            items = data['tasks'][0]['result'][0]['items']
            log_debug(f"DataForSEO returned {len(items)} items")
            
            for item in items:
                kw = item.get('keyword')
                
                # Robust extraction for info
                info = {}
                if 'keyword_info' in item:
                    info = item['keyword_info']
                elif 'keyword_data' in item and 'keyword_info' in item['keyword_data']:
                    info = item['keyword_data']['keyword_info']
                
                if not kw or not info:
                    log_debug(f"Skipping {kw}: Missing info")
                    continue
                    
                volume = info.get('search_volume', 0)
                if volume is None: volume = 0
                
                # Filter by min_volume in Python (can't use filters param)
                if volume < min_volume:
                    log_debug(f"Skipping {kw}: Low volume {volume} < {min_volume}")
                    continue
                
                cpc = info.get('cpc', 0.01) or 0.01
                competition = info.get('competition', 0.5) or 0.5
                
                # Smart scoring: (Volume × CPC) / Competition
                score = (volume * cpc) / max(competition, 0.1)
                
                keywords.append({
                    'keyword': kw,
                    'volume': volume,
                    'cpc': cpc,
                    'competition': competition,
                    'score': round(score, 2)
                })
        else:
            log_debug(f"DataForSEO returned NO items. Response structure: {str(data)[:300]}")
        
        # Sort by score (best opportunities first)
        keywords.sort(key=lambda x: x['score'], reverse=True)
        
        # Return top N
        top_keywords = keywords[:limit]
        
        log_debug(f"Returning {len(top_keywords)} keywords (from {len(keywords)} total)")
        print(f"Found {len(keywords)} keywords, returning top {len(top_keywords)} by opportunity score:")
        for kw in top_keywords[:5]:
            print(f"  {kw['keyword']}: Vol={kw['volume']}, CPC=${kw['cpc']:.2f}, Comp={kw['competition']:.2f}, Score={kw['score']}")
        
        return top_keywords
        
    except Exception as e:
        print(f"Keyword research error: {e}")
        import traceback
        traceback.print_exc()
        return []









import uuid # Added for filename generation

@app.route('/api/upload', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
        
    if file:
        try:
            # Read file data
            file_data = file.read()
            filename = f"{uuid.uuid4()}_{file.filename}"
            
            # Upload to Supabase
            public_url = upload_to_supabase(file_data, filename)
            
            return jsonify({"url": public_url})
        except Exception as e:
            print(f"Upload error: {e}")
            return jsonify({"error": str(e)}), 500



@app.route('/api/generate-image', methods=['POST'])
def generate_image_endpoint():
    data = request.json
    prompt = data.get('prompt')
    input_image_url = data.get('input_image_url')
    
    if not prompt:
        return jsonify({'error': 'Prompt is required'}), 400

    try:
        # 1. Enhance Prompt using Gemini (Text)
        # We can use the new client for this too, or stick to old one. 
        # Let's use the new client for consistency if possible, but mixing is fine for now to minimize risk.
        # Actually, let's just use the new client for image gen as tested.
        
        enhanced_prompt = prompt 
        # (Optional: Add enhancement logic back if needed, but for now direct is fine or we can re-add it)
        # The previous code used `model = genai.GenerativeModel("gemini-2.0-flash-exp")` from old SDK.
        # Let's keep the enhancement logic using the old SDK if it works, or switch to new.
        # To avoid conflict, let's just use the prompt directly for now to ensure image gen works, 
        # or use the new client for text generation too.
        
        UPLOAD_FOLDER = os.path.join('public', 'uploads')
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

        output_filename = f"gen_{uuid.uuid4()}.png"
        output_path = os.path.join(UPLOAD_FOLDER, output_filename)
        
        print(f"Generating image for prompt: {prompt}")
        
        result_path = gemini_client.generate_image(
            prompt=prompt,
            output_path=output_path,
            model_name="gemini-2.5-flash-image"
        )
        
        if not result_path:
            raise Exception("Gemini Image API failed")
            
        # Continue with existing logic (which expects output_filename)
        # We need to ensure the file exists at output_path, which generate_image does.
        
        UPLOAD_FOLDER = os.path.join('public', 'uploads')
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)

        output_filename = f"gen_{uuid.uuid4()}.png"
        output_path = os.path.join(UPLOAD_FOLDER, output_filename)
        
        image_saved = False
        if response.candidates and response.candidates[0].content.parts:
            for part in response.candidates[0].content.parts:
                if part.inline_data:
                    data = part.inline_data.data
                    if isinstance(data, str):
                        image_data = base64.b64decode(data)
                    else:
                        image_data = data
                        
                    with open(output_path, "wb") as f:
                        f.write(image_data)
                    image_saved = True
                    break
        
        if not image_saved:
            return jsonify({'error': 'No image generated'}), 500

        # Return URL
        output_url = f"/uploads/{output_filename}"
        
        return jsonify({
            'output_image_url': output_url,
            'status': 'Done',
            'enhanced_prompt': prompt 
        })

    except Exception as e:
        print(f"Error generating image: {e}")
        return jsonify({'error': str(e)}), 500







@app.route('/api/write-article-v2', methods=['POST'])
def write_article_v2():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
        
    try:
        data = request.get_json()
        project_id = data.get('project_id')
        topic = data.get('topic')
        keyword = data.get('keyword')
        parent_page_id = data.get('parent_page_id') # The BoFu page to link to
        
        if not project_id or not topic:
            return jsonify({"error": "project_id and topic are required"}), 400
        
        # 1. Fetch Context
        profile_res = supabase.table('business_profiles').select('*').eq('project_id', project_id).execute()
        profile = profile_res.data[0] if profile_res.data else {}
        
        parent_page = {}
        if parent_page_id:
            page_res = supabase.table('pages').select('*').eq('id', parent_page_id).execute()
            parent_page = page_res.data[0] if page_res.data else {}
            
        print(f"Writing article '{topic}' for project {project_id}...")
            
        # 2. Construct Prompt
        prompt = f"""
        You are a professional content writer for this business:
        Summary: {profile.get('business_summary')}
        ICP: {profile.get('ideal_customer_profile')}
        Voice: {profile.get('brand_voice')}
        
        Task: Write a high-quality, SEO-optimized article.
        Title: {topic}
        Primary Keyword: {keyword}
        
        CRITICAL INSTRUCTION - INTERNAL LINKING:
        You MUST include a natural, persuasive link to our product page within the content.
        Product Page URL: {parent_page.get('url')}
        Product Name: {parent_page.get('title', 'our product')}
        
        The link should not be "Click here". It should be contextual, e.g., "For the best solution, check out [Product Name]." or "Many experts recommend [Product Name] for this."
        
        Format: Markdown.
        Structure:
        - H1 Title
        - Introduction (Hook the ICP)
        - Body Paragraphs (H2s and H3s)
        - Conclusion
        """
        
        # 3. Generate
        # model = genai.GenerativeModel('gemini-2.0-flash-exp')
        
        text = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-flash",
            use_grounding=True
        )
        
        if not text:
            raise Exception("Gemini generation failed for Content Strategy")
        
        content = text
        
        # Return content ONLY (No auto-save)
        return jsonify({
            "content": content,
            "meta": {
                "linked_to": parent_page.get('url')
            }
        })

    except Exception as e:
        print(f"Error in write_article_v2: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/save-article', methods=['POST'])
def save_article():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        print(f"Saving article: {data.get('topic')} for project {data.get('project_id')}")
        
        project_id = data.get('project_id')
        topic = data.get('topic')
        content = data.get('content')
        keyword = data.get('keyword')
        parent_page_id = data.get('parent_page_id')
        
        # Check if brief exists
        existing = supabase.table('content_briefs').select('id').eq('project_id', project_id).eq('topic_title', topic).execute()
        
        if existing.data:
            print(f"Updating existing brief: {existing.data[0]['id']}")
            # Update
            brief_id = existing.data[0]['id']
            supabase.table('content_briefs').update({
                'content_markdown': content,
                'status': 'Draft'
            }).eq('id', brief_id).execute()
        else:
            print("Inserting new brief")
            # Insert new
            supabase.table('content_briefs').insert({
                'project_id': project_id,
                'topic_title': topic,
                'primary_keyword': keyword,
                'parent_page_id': parent_page_id,
                'content_markdown': content,
                'status': 'Draft',
                'funnel_stage': 'MoFu'
            }).execute()
            
        return jsonify({"message": "Article saved successfully"})
    except Exception as e:
        print(f"Error saving article: {e}")
        return jsonify({"error": str(e)}), 500

# ... (generate_image and crawl_project remain unchanged) ...

@app.route('/api/get-articles', methods=['GET'])
def get_articles():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    project_id = request.args.get('project_id')
    if not project_id: return jsonify({"error": "project_id required"}), 400
    
    try:
        print(f"Fetching articles for project: {project_id}")
        res = supabase.table('content_briefs').select('*').eq('project_id', project_id).in_('status', ['Draft', 'Published']).execute()
        print(f"Found {len(res.data)} articles")
        return jsonify({"articles": res.data})
    except Exception as e:
        print(f"Error fetching articles: {e}")
        return jsonify({"error": str(e)}), 500

import time
import os

@app.route('/api/generate-image', methods=['POST'])
def generate_image():
    try:
        data = request.get_json()
        prompt = data.get('prompt')
        
        print(f"Generating image with Gemini 2.5 Flash Image for prompt: {prompt[:100]}...")
        
        # Use gemini_client
        UPLOAD_FOLDER = os.path.join('public', 'generated_images')
        os.makedirs(UPLOAD_FOLDER, exist_ok=True)
        filename = f"gen_{int(time.time())}_{uuid.uuid4()}.png"
        output_path = os.path.join(UPLOAD_FOLDER, filename)
        
        result_path = gemini_client.generate_image(
            prompt=prompt,
            output_path=output_path,
            model_name="gemini-2.5-flash-image"
        )
        
        if not result_path:
            raise Exception("Gemini Image API failed")
            
        return jsonify({"image_url": f"/generated_images/{filename}"})

    except Exception as e:
        error_msg = f"Image generation failed: {str(e)}"
        print(error_msg)
        return jsonify({"error": error_msg}), 500

def scrape_page_content(url):
    """
    Scrapes a URL and returns structured content including body text, title, and meta data.
    Uses BeautifulSoup and Gemini for intelligent extraction.
    """
    import requests
    from bs4 import BeautifulSoup

    try:
        # --- LAYER 1: THE STEALTH CONNECTION ---
        print(f"Testing scrape_page_content for: {url}")
    
        # Use Robust Scraper Helper
        content, status_code, final_url = fetch_html_robust(url)
        
        # 3. Quality Check & Gemini Fallback (if content is poor)
        should_use_gemini = False
        if not content or len(content) < 500:
            print(f"DEBUG: Content too short ({len(content) if content else 0} bytes). triggering Gemini...")
            should_use_gemini = True
        elif b'window.location' in content or b'http-equiv="refresh"' in content:
             print("DEBUG: JS/Meta Redirect detected. triggering Gemini...")
             should_use_gemini = True
             
        if should_use_gemini and status_code != 200: # Only if we haven't already tried Gemini above
             try:
                print("DEBUG: Engaging Gemini Grounding Fallback (Quality Check)...")
                prompt = f"Visit this URL: {url}. Extract the full product details including: Title, Meta Description, H1, Main Content, JSON-LD Data. CRITICAL: Also extract 'Why we made it', 'How to apply', 'Ingredients', 'Benefits', and 'What makes it super' sections if they exist. Return the result as clean text."
                gemini_text = gemini_client.generate_content(
                    prompt=prompt,
                    model_name="gemini-2.5-flash", 
                    use_grounding=True
                )
                
                if gemini_text:
                    print("DEBUG: Gemini Grounding successful")
                    status_code = 200
                    
                    # Heuristic parsing to create synthetic HTML
                    lines = gemini_text.split('\n')
                    g_title = "Generated Title"
                    g_desc = ""
                    g_h1 = "Generated H1"
                    
                    for line in lines:
                        if "Title:" in line: g_title = line.replace("Title:", "").strip()
                        elif "Meta Description:" in line: g_desc = line.replace("Meta Description:", "").strip()
                        elif "H1:" in line: g_h1 = line.replace("H1:", "").strip()
                        
                    content = f"""
                    <html>
                        <head>
                            <title>{g_title}</title>
                            <meta name="description" content="{g_desc}">
                        </head>
                        <body>
                            <h1>{g_h1}</h1>
                            <p>{gemini_text}</p>
                        </body>
                    </html>
                    """.encode('utf-8')
             except Exception as ge:
                print(f"DEBUG: Gemini Grounding Exception: {ge}")

        if status_code != 200 or not content:
            return None

        soup = BeautifulSoup(content, 'html.parser')
        
        # 0. Extract Title (Before cleaning)
        page_title = None
        if soup.title:
            page_title = soup.title.get_text(strip=True)
            
        # Fallback to og:title
        if not page_title:
            meta_title = soup.find('meta', attrs={'property': 'og:title'})
            if meta_title:
                page_title = meta_title.get('content')
                
        # Fallback to H1
        if not page_title:
            h1 = soup.find('h1')
            if h1:
                page_title = h1.get_text(strip=True)
        
        # 0. Extract JSON-LD (Structured Data)
        json_ld_content = ""
        try:
            json_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_scripts:
                if script.string:
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, list):
                            for item in data:
                                if item.get('@type') == 'Product':
                                    json_ld_content += f"\nJSON-LD Product Data:\nName: {item.get('name')}\nDescription: {item.get('description')}\n"
                        elif isinstance(data, dict):
                            if data.get('@type') == 'Product':
                                json_ld_content += f"\nJSON-LD Product Data:\nName: {data.get('name')}\nDescription: {data.get('description')}\n"
                    except: pass
        except: pass

        # 0.5 Extract Meta Descriptions
        meta_description = ""
        try:
            meta_desc = soup.find('meta', attrs={'name': 'description'}) or soup.find('meta', attrs={'property': 'og:description'})
            if meta_desc:
                meta_description = meta_desc.get('content', '')
        except: pass

        # 0.6 Explicitly Extract .short-description
        short_desc_text = ""
        try:
            short_div = soup.find(class_='short-description')
            if short_div:
                short_desc_text = short_div.get_text(strip=True)
        except: pass

        # --- LAYER 2: THE RICH PARSER (Dynamic Section Scraper) ---
        rich_content = ""
        try:
            target_sections = ["why we made it", "how it feels", "what makes it super", "key benefits", "benefits", "how to use", "application", "ingredients"]
            found_sections = []
            
            # Scan headers and strong tags
            for header in soup.find_all(['h2', 'h3', 'h4', 'h5', 'h6', 'strong']):
                header_text = header.get_text(strip=True).lower()
                
                # Check if header matches any target keyword
                if any(keyword in header_text for keyword in target_sections):
                    section_name = header.get_text(strip=True)
                    section_body = ""
                    
                    # Grab siblings until next header
                    for sibling in header.next_siblings:
                        if sibling.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                            break
                        if sibling.name in ['div', 'p', 'ul', 'ol', 'span']:
                            text = sibling.get_text(strip=True)
                            if text:
                                section_body += text + "\n"
                                
                    if section_body and len(section_body) > 10: # Filter empty/tiny sections
                        found_sections.append(f"## {section_name}\n{section_body}")
                        
            if found_sections:
                rich_content = "\n\n".join(found_sections)
                print(f"DEBUG: Found {len(found_sections)} Rich Sections")
        except Exception as e:
            print(f"DEBUG: Rich Section Scraper failed: {e}")

        # 1. Minimal Cleaning
        for unwanted in soup(["script", "style", "svg", "noscript", "iframe", "object", "embed", "applet", "link", "meta"]):
            unwanted.decompose()
        
        for tag in soup.find_all(['nav', 'footer', 'aside']):
            tag.decompose()
            
        noise_headings = ['related', 'you may also like', 'stories', 'intentional living', 'blog', 'latest news', 'articles']
        for header in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = header.get_text().lower()
            if any(x in text for x in noise_headings):
                parent = header.parent
                for _ in range(3):
                    if parent:
                        if parent.name in ['section', 'div', 'aside']:
                            parent.decompose()
                            break
                        parent = parent.parent

        for tag in soup.find_all(True):
            tag.attrs = {}
            
        cleaned_html = str(soup.body)[:150000] 
        
        body_content = ""
        try:

            # extract_model = genai.GenerativeModel('gemini-2.0-flash-exp')
            extraction_prompt = f"""
            You are a strict Content Extraction Robot.
            
            INPUT: Raw HTML content + JSON-LD + Meta Description + Detected Short Description + **RICH SECTIONS**.
            OUTPUT: The actual visible text content from the page, formatted in Markdown.
            
            CRITICAL RULES:
            1. **NO HALLUCINATIONS**: You must ONLY extract text that explicitly exists in the provided data.
            2. **Identify Page Type**: (Product, Category, Service, or Blog Post).
            3. **PRIORITIZE VISIBLE CONTENT**:
               - **Product**: Title, Price, **SHORT DESCRIPTION**, **FULL Description**, Specs, "What's Inside", Ingredients.
               - **Category/Collection**: **Category Name (H1)**, **Description** (Introduction text), **List of Products** (Name, Price, Key Benefit).
               - **Service**: Service Name, Details, Process.
               - **Blog**: Title, Author, Date, Full Article Body.
            4. **HANDLING HIDDEN DATA**: Use Meta/JSON-LD ONLY if visible content is missing.
            5. **IGNORE NOISE**: "Related Products", "Add to cart", "Footer links", "Menu", "Navigation".
            6. **Formatting**: Use Markdown. For Products in Category, use a list or table.
            7. **NO CONVERSATION**: Do not say "Okay", "Sure", "Here is the content". Start directly with the Markdown.
            
            Detected Short Description: {short_desc_text}
            Meta Description: {meta_description}
            JSON-LD Data: {json_ld_content}
            
            *** RICH SECTIONS FOUND (PRIORITY) ***
            {rich_content}
            
            HTML Snippet: {cleaned_html}
            """
            
            body_content = gemini_client.generate_content(
                prompt=extraction_prompt,
                model_name="gemini-2.5-flash"
            )
            
            if not body_content:
                raise Exception("Gemini extraction failed")
                
            body_content = body_content.strip()
            body_content = body_content.replace('```markdown', '').replace('```', '').strip()
            
        except Exception as e:
            print(f"LLM Extraction failed: {e}")
            body_content = soup.get_text(separator='\n\n', strip=True)
        
        if not body_content:
             body_content = "Could not extract meaningful content"
        
        return {
            "title": page_title,
            "body_content": body_content,
            "meta_description": meta_description,
            "json_ld": json_ld_content
        }

    except Exception as e:
        print(f"Scraping error: {e}")
        return None

@app.route('/api/crawl-project', methods=['POST'])
def crawl_project_endpoint():
    if not supabase:
        return jsonify({"error": "Supabase not configured"}), 500
        
    try:
        data = request.get_json()
        project_id = data.get('project_id')
        
        if not project_id:
            return jsonify({"error": "project_id is required"}), 400
            
        # Fetch domain from project
        project_res = supabase.table('projects').select('domain').eq('id', project_id).execute()
        if not project_res.data:
            return jsonify({"error": "Project not found"}), 404
            
        domain = project_res.data[0]['domain']
        
        print(f"Re-crawling project {project_id} ({domain})...")
        pages = crawl_sitemap(domain, project_id)
        
        if pages:
            supabase.table('pages').insert(pages).execute()
            
        return jsonify({
            "message": f"Crawl complete. Found {len(pages)} pages.",
            "pages_found": len(pages)
        })
    except Exception as e:
        print(f"Error crawling project: {e}")
        return jsonify({"error": str(e)}), 500


def generate_content_via_rest(prompt, api_key, model="gemini-2.5-pro", use_grounding=True):
    """
    Generate content using Gemini REST API directly to avoid SDK crashes.
    Supports Google Search Grounding.
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }
    
    if use_grounding:
        data["tools"] = [{
            "google_search": {}  # Enable Google Search Grounding
        }]
        
    try:
        response = requests.post(url, headers=headers, json=data, timeout=60)
        response.raise_for_status()
        result = response.json()
        
        # Extract text
        try:
            text = result['candidates'][0]['content']['parts'][0]['text']
            print(f"DEBUG: REST API Success. Text length: {len(text)}", flush=True)
            return text
        except (KeyError, IndexError):
            print(f"DEBUG: Unexpected REST response structure: {result}", flush=True)
            return None
            
    except Exception as e:
        print(f"DEBUG: REST API call failed: {e}")
        if 'response' in locals() and response is not None:
             print(f"DEBUG: Response content: {response.text}")
        raise e

@app.route('/api/batch-update-pages', methods=['POST'])
def batch_update_pages():
    print(f"====== BATCH UPDATE PAGES CALLED ======", flush=True)
    log_debug("Entered batch_update_pages route")
    log_debug(f"Entered batch_update_pages route")
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.json
        log_debug(f"Received batch update data: {data}")
        page_ids = data.get('page_ids', [])
        action = data.get('action')
        
        if not page_ids or not action:
            return jsonify({"error": "page_ids and action required"}), 400
            
        if action == 'trigger_audit':
            # In a real app, this would trigger a background job
            supabase.table('pages').update({"audit_status": "Pending"}).in_('id', page_ids).execute()
            
        elif action == 'trigger_classification':
            supabase.table('pages').update({"classification_status": "Pending"}).in_('id', page_ids).execute()
            
        elif action == 'approve_strategy':
            supabase.table('pages').update({"approval_status": True}).in_('id', page_ids).execute()
            
        elif action == 'scrape_content':
            # Scrape existing content for selected pages
            for page_id in page_ids:
                page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
                if not page_res.data: continue
                page = page_res.data
                
                try:
                    scraped_data = scrape_page_content(page['url'])
                    
                    if scraped_data:
                        # Update tech_audit_data with body_content AND title
                        current_tech_data = page.get('tech_audit_data', {})
                        current_tech_data['body_content'] = scraped_data['body_content']
                        
                        if not current_tech_data.get('title') or current_tech_data.get('title') == 'Untitled':
                             current_tech_data['title'] = scraped_data['title'] or get_title_from_url(page['url'])
                        
                        supabase.table('pages').update({
                            "tech_audit_data": current_tech_data
                        }).eq('id', page_id).execute()
                        print(f"✓ Scraped content for {page['url']}")
                    else:
                        print(f"⚠ Failed to scrape {page['url']}")
                        
                except Exception as e:
                    print(f"Error scraping page {page_id}: {e}")
            
            return jsonify({"message": "Content scraped successfully"})
        elif action == 'generate_content':
            # Product/Category pages use gemini_client for SEO verification
            # Topic pages use gemini_client (no grounding needed - they have research already)
            
            api_key = os.environ.get("GEMINI_API_KEY")
            if not api_key:
                return jsonify({"error": "GEMINI_API_KEY not found"}), 500

            def process_content_generation_background(page_ids, api_key):
                print(f"====== GENERATE_CONTENT BACKGROUND THREAD STARTED ======", flush=True)
                
                for page_id in page_ids:
                    try:
                        # 1. Get Page Data
                        page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
                        if not page_res.data: continue
                        page = page_res.data
                        
                        # 2. Get existing content
                        existing_content = page.get('tech_audit_data', {}).get('body_content', '')
                        if not existing_content:
                            # If no body content, try to scrape it now
                            try:
                                logging.info(f"DEBUG: No existing content for {page['url']}, attempting fresh scrape...")
                                scraped_data = scrape_page_content(page['url'])
                                if scraped_data and scraped_data.get('body_content'):
                                    existing_content = scraped_data['body_content']
                                    logging.info(f"DEBUG: Fresh scrape successful ({len(existing_content)} bytes)")
                                else:
                                    existing_content = "No content available"
                                    logging.info("DEBUG: Fresh scrape returned no content")
                            except Exception as e:
                                logging.error(f"Error scraping content for {page['url']}: {e}")
                                existing_content = "No content available"
                        
                        # 3. Generate improved content
                        page_title = page.get('tech_audit_data', {}).get('title', page.get('url', ''))
                        page_type = page.get('page_type', 'page')

                        # Fetch Project Settings for Localization
                        project_loc = 'US'
                        project_lang = 'English'
                        try:
                            log_debug(f"Fetching project settings for {page['project_id']}...")
                            project_res = supabase.table('projects').select('location, language').eq('id', page['project_id']).single().execute()
                            if project_res.data:
                                project_loc = project_res.data.get('location', 'US')
                                project_lang = project_res.data.get('language', 'English')
                            log_debug(f"Project settings: Loc={project_loc}, Lang={project_lang}")
                        except Exception as proj_err:
                            log_debug(f"Error fetching project settings: {proj_err}")
                        
                        try:
                            log_debug(f"Checking page type for branching: '{page_type}'")
                            
                            # Fetch Parent Page Context (for Internal Linking)
                            parent_context = ""
                            if page.get('source_page_id'):
                                try:
                                    # 1. Fetch Parent (MoFu)
                                    parent_res = supabase.table('pages').select('id, url, tech_audit_data, source_page_id').eq('id', page['source_page_id']).single().execute()
                                    if parent_res.data:
                                        p_data = parent_res.data
                                        p_title = p_data.get('tech_audit_data', {}).get('title', 'Related Page')
                                        p_url = p_data.get('url', '#')
                                        
                                        # 2. Fetch Grandparent (Product) if exists
                                        gp_context = ""
                                        if p_data.get('source_page_id'):
                                            try:
                                                gp_res = supabase.table('pages').select('url, tech_audit_data').eq('id', p_data['source_page_id']).single().execute()
                                                if gp_res.data:
                                                    gp_data = gp_res.data
                                                    gp_title = gp_data.get('tech_audit_data', {}).get('title', 'Main Product')
                                                    gp_url = gp_data.get('url', '#')
                                                    gp_context = f"\n    - ALSO link to the Main Product: [{gp_title}]({gp_url}) (Context: The ultimate solution)."
                                            except Exception:
                                                pass # Ignore grandparent errors

                                        parent_context = f"\n    **INTERNAL LINKING REQUIREMENT**:\n    - You MUST organically mention and link to the parent page: [{p_title}]({p_url}) (Context: Next step in learning).\n{gp_context}"
                                except Exception as parent_err:
                                    log_debug(f"Error fetching parent context: {parent_err}")

                            # BRANCHING LOGIC: Product vs Category vs Topic
                            generated_text = ""
                            if page_type and page_type.lower().strip() == 'product':
                                log_debug("Entered Product generation block")
                                # PRODUCT PROMPT (Sales & Conversion Focused - Conservative + Grounded)
                                prompt = f"""You are an expert E-commerce Copywriter with access to live Google Search.
                                
            **TASK**: Polish and enhance the content for this **PRODUCT PAGE**. 
            **CRITICAL GOAL**: Improve clarity and persuasion WITHOUT changing the original length or structure significantly.

            **CONTEXT**:
            - Target Audience Location: {project_loc}
            - Target Language: {project_lang}

            **LOCALIZATION RULES (CRITICAL)**:
            1. **Currency**: You MUST use the local currency for **{project_loc}** (e.g., ₹ INR for India). Convert prices if needed.
            2. **Units**: Use the measurement system standard for **{project_loc}**.
            3. **Spelling**: Use the correct spelling dialect (e.g., "Colour" for UK/India).
            4. **Cultural Context**: Use examples relevant to **{project_loc}**.

            **PAGE DETAILS**:
            - URL: {page['url']}
            - Title: {page_title}
            - Product Name: {page_title}

            **EXISTING CONTENT** (Source of Truth):
            ```
            {existing_content[:5000] if existing_content else "No content"}
            ```

            **INSTRUCTIONS**:
            1.  **Refine, Don't Reinvent**: Keep the original structure (paragraphs, bullets, sections). Only fix what is broken or unclear.
            2.  **Respect Length**: The output should be roughly the same length as the original (+/- 10%). Do NOT add long "fluff" sections about industry trends unless they were already there.
            3.  **Persuasion**: Make the existing text more punchy and benefit-driven.
            4.  **STRICT ACCURACY**: 
                -   **DO NOT CHANGE** technical specs, ingredients, dimensions, or "What's Inside".
                -   **DO NOT INVENT** features.
            5.  **Competitive Intelligence** (USE GROUNDING):
                -   Search for similar products to understand competitive positioning
                -   Verify any comparative claims ("best", "top-rated") against live data
                -   Identify unique selling points vs competitors

            **OUTPUT FORMAT** (Markdown):
            -   Return the full page content in Markdown.
            -   Include a **Meta Description** at the top.
            -   Keep the original formatting (H1, H2, bullets) but polished.
            """
                                # Use REST API for Products
                                print(f"DEBUG: Generating content for Product: {page_title} using gemini-2.5-pro (REST)", flush=True)
                                generated_text = generate_content_via_rest(
                                    prompt=prompt,
                                    api_key=api_key,
                                    model="gemini-2.5-pro",
                                    use_grounding=True
                                )
                            
                            elif page_type and page_type.lower() == 'category':
                                # CATEGORY PROMPT (Research-Backed SEO Enhancement - Grounded + Respect Length)
                                prompt = f"""You are an expert E-commerce Copywriter & SEO Specialist.

            **TASK**: Enhance this **CATEGORY/COLLECTION PAGE** using real-time search data.
            **CRITICAL GOAL**: infuse the content with high-value SEO keywords and competitive insights while respecting the original length and structure.

            **CONTEXT**:
            - Target Audience Location: {project_loc}
            - Target Language: {project_lang}

            **LOCALIZATION RULES (CRITICAL)**:
            1. **Currency**: You MUST use the local currency for **{project_loc}** (e.g., ₹ INR for India). Convert prices if needed.
            2. **Units**: Use the measurement system standard for **{project_loc}**.
            3. **Spelling**: Use the correct spelling dialect (e.g., "Colour" for UK/India).
            4. **Cultural Context**: Use examples relevant to **{project_loc}**.

            **PAGE DETAILS**:
            - URL: {page['url']}
            - Title: {page_title}
            - Category Name: {page_title}

            **EXISTING CONTENT** (Source of Truth):
            ```
            {existing_content[:5000]}
            ```

            **INSTRUCTIONS**:
            1.  **Research First (USE GROUNDING)**:
                -   Search for top-ranking competitors for "{page_title}" in **{project_loc}**.
                -   Identify the **primary intent** (e.g., "buy cheap", "luxury", "guide") and align the copy.
                -   Find 3-5 **semantic keywords** competitors are using that are missing here.

            2.  **Enhance & Optimize (The "Better" Part)**:
                -   Rewrite the existing text to include these new keywords naturally.
                -   Improve the value proposition based on what competitors offer.
                -   Make it **better SEO-wise**: clearer headings, stronger hook, better keyword density.

            3.  **Respect Constraints**:
                -   **Length**: Keep it roughly the same length (+/- 10%). Do NOT add massive new sections (like FAQs) unless the original had them.
                -   **Structure**: Maintain the existing flow (Intro -> Products -> Outro).

            4.  **Meta Description**:
                -   Write a new, high-CTR Meta Description (150-160 chars).

            **OUTPUT FORMAT** (Markdown):
            -   Return the full page content in Markdown.
            -   Include a **Meta Description** at the top.
            """
                                # Use REST API for Categories
                                generated_text = generate_content_via_rest(
                                    prompt=prompt,
                                    api_key=api_key,
                                    model="gemini-2.5-pro",
                                    use_grounding=True
                                )
                                
                            elif page_type == 'Topic':
                                # CHUNKED GENERATION LOGIC (New "Best-in-Class" Workflow)
                                print(f"DEBUG: Starting Chunked Workflow for {page_title}...", flush=True)
                                
                                # Get research data
                                research_data = page.get('research_data', {})
                                keyword_cluster = research_data.get('keyword_cluster', [])
                                primary_keyword = research_data.get('primary_keyword', page_title)
                                perplexity_research = research_data.get('perplexity_research', '')
                                citations = research_data.get('citations', [])
                                funnel_stage = page.get('funnel_stage', '')
                                source_page_id = page.get('source_page_id')
                                
                                # Internal Links Logic
                                internal_links = []
                                cta_url = None # URL for the final CTA
                                
                                if source_page_id:
                                    try:
                                        parent_res = supabase.table('pages').select('id, url, tech_audit_data, source_page_id').eq('id', source_page_id).single().execute()
                                        if parent_res.data:
                                            parent = parent_res.data
                                            parent_title = parent.get('tech_audit_data', {}).get('title', parent.get('url'))
                                            if funnel_stage == 'MoFu':
                                                internal_links.append(f"- {parent_title} (Main Product): {parent['url']}")
                                                cta_url = parent['url']
                                            elif funnel_stage == 'ToFu':
                                                internal_links.append(f"- {parent_title} (In-Depth Guide): {parent['url']}")
                                                grandparent_id = parent.get('source_page_id')
                                                if grandparent_id:
                                                    gp_res = supabase.table('pages').select('url, tech_audit_data').eq('id', grandparent_id).single().execute()
                                                    if gp_res.data:
                                                        gp_title = gp_res.data.get('tech_audit_data', {}).get('title', gp_res.data.get('url'))
                                                        internal_links.append(f"- {gp_title} (Main Product): {gp_res.data['url']}")
                                                        cta_url = gp_res.data['url'] # Prefer Grandparent (Product) for ToFu CTA
                                                
                                                if not cta_url: cta_url = parent['url'] # Fallback to Parent if no GP
                                    except Exception as e:
                                        print(f"Error fetching internal links: {e}")
                                links_str = '\n'.join(internal_links) if internal_links else "No internal links available"
                                
                                # Format keywords & citations
                                if keyword_cluster:
                                    kw_list = '\n'.join([f"- {kw['keyword']} ({kw['volume']}/mo, Score: {kw.get('score', 0)})" for kw in keyword_cluster[:15]])
                                else:
                                    kw_list = f"- {primary_keyword}"
                                citations_str = '\n'.join([f"[{i+1}] {cite}" for i, cite in enumerate(citations[:10])]) if citations else "No citations available"
                                
                                # Research Section
                                research_section = ""
                                if perplexity_research:
                                    research_section = f"# DEEP RESEARCH BRIEF (Source: Perplexity):\n{perplexity_research}\n\n# CITATIONS:\n{citations_str}"

                                # 1. Generate Dynamic Outline
                                outline = generate_dynamic_outline(page_title, research_section, project_loc, gemini_client)
                                if not outline:
                                    raise Exception("Failed to generate outline")
                                
                                # 2. Generate Sections (Chunked)
                                full_content = generate_sections_chunked(page_title, outline, research_section, project_loc, gemini_client, links_str)
                                
                                # 3. Final Polish (Intro/Outro/Meta)
                                generated_text = final_polish(full_content, page_title, primary_keyword, cta_url, project_loc, gemini_client)

                            if not generated_text:
                                raise Exception("Content generation returned empty string")

                            # Parse Meta Description if present
                            meta_desc = "Generated by AgencyOS"
                            if "**Meta Description**:" in generated_text:
                                try:
                                    parts = generated_text.split("**Meta Description**:")
                                    meta_part = parts[1].split("\n")[0].strip()
                                    meta_desc = meta_part
                                except: pass
                            
                            # Update Page
                            supabase.table('pages').update({
                                "content": generated_text,
                                "status": "Generated",
                                "product_action": "Idle",
                                "tech_audit_data": {
                                    **page.get('tech_audit_data', {}),
                                    "meta_description": meta_desc
                                }
                            }).eq('id', page_id).execute()
                            
                            log_debug(f"Content generated successfully for {page_title}")

                        except Exception as gen_err:
                            log_debug(f"Generation error for {page_title}: {gen_err}")
                            import traceback
                            traceback.print_exc()
                            # Reset status
                            supabase.table('pages').update({"product_action": "Idle"}).eq('id', page_id).execute()
                            
                    except Exception as e:
                        log_debug(f"Outer error for {page_id}: {e}")
                        try:
                            supabase.table('pages').update({"product_action": "Idle"}).eq('id', page_id).execute()
                        except: pass

            # Update status to Processing IMMEDIATELY (Before thread starts)
            # This ensures frontend sees the loading state
            for pid in page_ids:
                try:
                    supabase.table('pages').update({
                        "product_action": "Processing Content..."
                    }).eq('id', pid).execute()
                except: pass

            # Start background thread
            log_debug("Starting background Content Generation thread...")
            thread = threading.Thread(target=process_content_generation_background, args=(page_ids, api_key))
            thread.start()
            
            return jsonify({"message": "Content generation started in background."}), 202


        elif action == 'conduct_research':
            # SIMPLIFIED: Perplexity Research Brief ONLY
            # (Keywords/Competitors are already done in generate_mofu)
            
            def process_research_background(page_ids, api_key):
                print(f"====== CONDUCT_RESEARCH BACKGROUND THREAD STARTED ======", flush=True)
                log_debug(f"CONDUCT_RESEARCH: Starting for {len(page_ids)} pages")
                
                for page_id in page_ids:
                    print(f"DEBUG: Processing page_id: {page_id}", flush=True)
                    try:
                        # Get the Topic page
                        page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
                        if not page_res.data: continue
                        
                        page = page_res.data
                        topic_title = page.get('tech_audit_data', {}).get('title', '')
                        research_data = page.get('research_data') or {}
                        
                        if not topic_title: continue
                        
                        log_debug(f"Researching topic (Perplexity): {topic_title}")
                        
                        # Get existing keywords/competitors
                        keywords = research_data.get('ranked_keywords', [])
                        competitor_urls = research_data.get('competitor_urls', [])
                        
                        # Fetch Project Settings for Localization
                        project_res = supabase.table('projects').select('location, language').eq('id', page['project_id']).single().execute()
                        project_loc = project_res.data.get('location', 'US') if project_res.data else 'US'
                        project_lang = project_res.data.get('language', 'English') if project_res.data else 'English'
                        
                        # Fallback: If no keywords (maybe old page), run Gemini now
                        if not keywords:
                            log_debug(f"No keywords found for {topic_title}. Running Gemini fallback (Loc: {project_loc})...")
                            gemini_result = perform_gemini_research(topic_title, location=project_loc, language=project_lang)
                            if gemini_result:
                                keywords = gemini_result.get('keywords', [])
                                competitor_urls = [c['url'] for c in gemini_result.get('competitors', [])]
                                # Update research data immediately
                                research_data.update({
                                    "competitor_urls": competitor_urls,
                                    "ranked_keywords": keywords,
                                    "formatted_keywords": '\n'.join([f"{kw.get('keyword', '')} | {kw.get('intent', 'informational')} |" for kw in keywords])
                                })
                        
                        # Prepare query for Perplexity
                        keyword_list = ", ".join([k.get('keyword', '') for k in keywords[:15]])
                        competitor_list = ", ".join(competitor_urls)
                        
                        research_query = f"""
                        Research Topic: {topic_title}
                        Top Competitors: {competitor_list}
                        Top Keywords: {keyword_list}
                        
                        Create a detailed Content Research Brief for this topic.
                        Analyze the competitors and keywords to find content gaps.
                        Focus on User Pain Points, Key Subtopics, and Scientific/Technical details.
                        """
                        
                        log_debug(f"Starting Perplexity Research for brief (Loc: {project_loc})...")
                        perplexity_result = research_with_perplexity(research_query, location=project_loc, language=project_lang)
                        
                        # Update research data with brief
                        research_data.update({
                            "stage": "complete",
                            "mode": "hybrid",
                            "perplexity_research": perplexity_result.get('research', ''),
                            "citations": perplexity_result.get('citations', [])
                        })
                        
                        # Update page
                        supabase.table('pages').update({
                            "research_data": research_data,
                            "product_action": "Idle"
                        }).eq('id', page_id).execute()
                        
                        log_debug(f"Research complete for {topic_title}")
                        
                    except Exception as e:
                        log_debug(f"Research error: {e}")
                        import traceback
                        traceback.print_exc()
                        # Reset status on error
                        try:
                            supabase.table('pages').update({"product_action": "Idle"}).eq('id', page_id).execute()
                        except: pass

            # Update status to Processing IMMEDIATELY (Before thread starts)
            # This ensures frontend sees the loading state
            for pid in page_ids:
                try:
                    supabase.table('pages').update({
                        "product_action": "Processing Research..."
                    }).eq('id', pid).execute()
                except: pass

            # Start background thread
            log_debug("Starting background Research thread...")
            thread = threading.Thread(target=process_research_background, args=(page_ids, os.environ.get("GEMINI_API_KEY")))
            thread.start()
            
            return jsonify({"message": "Research started in background. The status will update to 'Processing...' in the table."}), 202


            return jsonify({"message": "Content generated successfully"})

        elif action == 'generate_mofu':
            print(f"====== GENERATE MOFU ACTION ======", flush=True)
            log_debug(f"GENERATE_MOFU: Starting for {len(page_ids)} pages")
            print(f"DEBUG: Received generate_mofu action for page_ids: {page_ids}")
            print(f"DEBUG: Received generate_mofu action for page_ids: {page_ids}")
            # Use gemini_client with Grounding (ENABLED!)
            # This helps verify that the topic angles are actually trending/relevant.
            # client = genai_new.Client(api_key=os.environ.get("GEMINI_API_KEY")) # REMOVED
            # tool = types.Tool(google_search=types.GoogleSearch()) # REMOVED
            
            def process_mofu_generation(page_ids, api_key):
                log_debug(f"Background MoFu thread started for pages: {page_ids}")
                try:
                    # Use gemini_client with Grounding (ENABLED!)
                    # client = genai_new.Client(api_key=api_key) # REMOVED
                    # tool = types.Tool(google_search=types.GoogleSearch()) # REMOVED
                    
                    for pid in page_ids:
                        print(f"DEBUG: Processing page_id: {pid}")
                        # Get Product Page Data
                        res = supabase.table('pages').select('*').eq('id', pid).single().execute()
                        if not res.data: 
                            print(f"DEBUG: Page {pid} not found")
                            continue
                        product = res.data
                        product_tech = product.get('tech_audit_data', {})


                        
                        print(f"Researching MoFu opportunities for {product.get('url')}...")
                        
                        # === NEW DATA-FIRST WORKFLOW ===
                        
                        # Step 0: Ensure Content Context (Fix for "Memoir vs Candles")
                        body_content = product_tech.get('body_content', '')
                        product_title = product_tech.get('title', 'Untitled')
                        
                        # FIX: If title is "Pending Scan" or generic, force scrape to get REAL title
                        is_bad_title = not product_title or 'pending' in product_title.lower() or 'untitled' in product_title.lower() or 'scan' in product_title.lower()
                        
                        if not body_content or len(body_content) < 100 or is_bad_title:
                            log_debug(f"Content/Title missing or bad ('{product_title}') for {product['url']}, scraping now...")
                            scraped = scrape_page_content(product['url'])
                            if scraped:
                                body_content = scraped['body_content']
                                # Use scraped title if current is bad
                                if is_bad_title and scraped.get('title'):
                                    product_title = scraped['title']
                                    log_debug(f"Updated title from '{product_tech.get('title')}' to '{product_title}'")
                                
                                # Update DB so we don't scrape again
                                current_tech = product.get('tech_audit_data', {})
                                current_tech['body_content'] = body_content
                                current_tech['title'] = product_title # Save real title
                                
                                supabase.table('pages').update({
                                    "tech_audit_data": current_tech
                                }).eq('id', pid).execute()
                                product_tech = current_tech # Update local var
                        
                        log_debug(f"Using Product Title: {product_title}")

                        # Fetch Source Product Page
                        product_res = supabase.table('pages').select('*').eq('id', pid).single().execute()
                        if not product_res.data:
                            print(f"DEBUG: Product page not found for ID: {pid}", flush=True)
                            continue
                        product = product_res.data
                        product_title = product.get('tech_audit_data', {}).get('title', '')
                        print(f"DEBUG: Processing Product: {product_title}", flush=True)
                        
                        # Fetch Project Settings
                        project_res = supabase.table('projects').select('location, language').eq('id', product['project_id']).single().execute()
                        project_loc = project_res.data.get('location', 'US') if project_res.data else 'US'
                        project_lang = project_res.data.get('language', 'English') if project_res.data else 'English'
                        print(f"DEBUG: Project Settings: {project_loc}, {project_lang}", flush=True)

                        # Step 1: Get Keywords
                        keywords = []
                        # (Skipping to where I can inject prints easily)
                        # I'll just add prints around the Gemini call in the next block
                        # Step 1: Generate MULTIPLE Broad Seed Keywords for DataForSEO
                        # Strategy: Don't search for specific product - search for CATEGORY + common queries
                        if not product_title:
                            product_title = get_title_from_url(product['url'])

                        print(f"DEBUG: Analyzing context for: {product_title} (Loc: {project_loc}, Lang: {project_lang})")
                        
                        try:
                            # NEW STRATEGY: Generate multiple broad seeds
                            context_prompt = f"""Analyze this product to generate 3-5 BROAD keyword seeds for DataForSEO research.

        Product Title: "{product_title}"
        Page Content: {body_content[:2000]}

        Task:
        1. Identify the product CATEGORY (e.g., "carrier oils", "lipstick", "sunscreen", "candles")
        2. Generate 3-5 BROAD search terms that people use when researching this category in **{project_loc}**.
        3. DO NOT use the specific product name - use GENERIC category terms

        Examples:
        - Product: "Apricot Kernel Oil" → Seeds: ["carrier oil benefits", "oil for skin", "facial oils", "natural oils skincare"]
        - Product: "MAC Ruby Woo Lipstick" → Seeds: ["red lipstick", "matte lipstick", "long lasting lipstick", "lipstick shades"]
        - Product: "Supergoop Sunscreen" → Seeds: ["face sunscreen", "spf for skin", "sunscreen benefits", "daily sunscreen"]

        OUTPUT: Return ONLY a comma-separated list of 3-5 broad keywords. No explanations.
        Example output: carrier oil benefits, oil for skin, facial oils, natural oils"""
                            
                            seed_res_text = gemini_client.generate_content(
                                prompt=context_prompt,
                                model_name="gemini-2.5-flash",
                                use_grounding=True
                            )
                            seeds_str = seed_res_text.strip().replace('"', '').replace("'", "") if seed_res_text else ""
                            broad_seeds = [s.strip() for s in seeds_str.split(',') if s.strip()]
                            
                            # Fallback if AI fails
                            if not broad_seeds:
                                broad_seeds = [product_title]
                            
                            log_debug(f"Generated {len(broad_seeds)} broad seeds: {broad_seeds}")
                            print(f"DEBUG: Broad seed keywords: {broad_seeds}")
                            
                        except Exception as e:
                            print(f"⚠ Seed generation failed: {e}. Using product title.")
                            broad_seeds = [product_title]

                        
                        # NEW: Use Gemini 2.0 Flash with Grounding as PRIMARY source (User Request)
                        print(f"DEBUG: Using Gemini 2.0 Flash for keyword research (Primary)...")
                        log_debug("Calling perform_gemini_research as PRIMARY source")
                        
                        gemini_result = perform_gemini_research(product_title, location=project_loc, language=project_lang)
                        keywords = []
                        
                        if gemini_result and gemini_result.get('keywords'):
                            print(f"✓ Gemini Research successful. Found {len(gemini_result['keywords'])} keywords.")
                            for k in gemini_result['keywords']:
                                keywords.append({
                                    'keyword': k.get('keyword'),
                                    'volume': 100, # Placeholder volume since Gemini doesn't provide it
                                    'score': 100,
                                    'cpc': 0,
                                    'competition': 0,
                                    'intent': k.get('intent', 'Commercial')
                                })
                        else:
                            print(f"⚠ Gemini Research failed. Using fallback.")
                            keywords = [{'keyword': product_title, 'volume': 0, 'score': 0, 'cpc': 0, 'competition': 0}]


                        
                        # Step 2: Prepare Data for Topic Generation (No Deep Research yet)
                        log_debug("Skipping deep research (will be done in 'Conduct Research' stage).")
                        
                        # Format keyword list for prompt
                        keyword_list = '\n'.join([f"- {k['keyword']} ({k['volume']} searches/month)" for k in keywords[:50]])
                        
                        # Minimal research data for now
                        research_data = {
                            "keywords": keywords,
                            "stage": "research_pending"
                        }


                        # Step 4: Generate Topics from REAL DATA
                        import datetime
                        current_year = datetime.datetime.now().year
                        next_year = current_year + 1
                        
                        topic_prompt = f"""You are an SEO Content Strategist. Generate 6 MoFu (Middle-of-Funnel) article topics based on REAL keyword data.

        **Product**: {product_title}
        **Target Audience**: {project_loc} ({project_lang})

        **VERIFIED HIGH-VOLUME KEYWORDS** (Scored by Opportunity):
        {keyword_list}

        **YOUR TASK**:
        Create 6 MoFu topics. For EACH topic, assign ALL semantically relevant keywords from the list above (could be 3-15 keywords per topic - include as many as naturally fit the angle).

        **Requirements**:
        1. Each topic must target a primary keyword (highest opportunity score for that angle)
        2. Include ALL secondary keywords that semantically match the topic angle
        3. Topics should be Middle-of-Funnel (Comparison, Best Of, Guide, vs)

        **Topic Types**:
        - "Best X for Y in {current_year}" (roundup/comparison)
        - "Product vs Competitor" (head-to-head comparison)
        - "Top Alternatives to X" (alternative guides)  
        - Use cases backed by research

        **Output Format** (JSON):
        {{
          "topics": [
            {{
              "title": "[Exact title - include year {current_year} if relevant]",
              "slug": "url-friendly-slug",
              "description": "2-sentence description of content angle",
              "keyword_cluster": [
                {{"keyword": "[keyword1]", "volume": [INTEGER_FROM_INPUT], "is_primary": true}},
                {{"keyword": "[keyword2]", "volume": [INTEGER_FROM_INPUT], "is_primary": false}},
                ...
              ],
              "research_notes": "Why this topic (reference SERP competitor or research insight)"
            }}
          ]
        }}

        CRITICAL: 
        1. Use EXACT integers for volume from the provided list. DO NOT write "Estimated".
        2. Assign keywords based on semantic relevance. Don't artificially limit - if 12 keywords fit a topic, include all 12.
        """


                        
                        try:
                            text = gemini_client.generate_content(
                                prompt=topic_prompt,
                                model_name="gemini-2.5-flash",
                                use_grounding=True
                            )
                            if not text: raise Exception("Empty response from Gemini")
                            text = text.strip()
                            if text.startswith('```json'): text = text[7:]
                            if text.startswith('```'): text = text[3:]
                            if text.endswith('```'): text = text[:-3]
                            text = text.strip()
                            
                            # Parse JSON with error handling
                            try:
                                data = json.loads(text)
                            except json.JSONDecodeError as json_err:
                                log_debug(f"JSON parse error: {json_err}. Response: {text[:300]}")
                                print(f"✗ Gemini returned invalid JSON. Skipping MoFu for {product_title}")
                                continue  # Skip to next product
                            
                            topics = data.get('topics', [])
                            if not topics:
                                log_debug("No topics in AI response")
                                continue
                            
                            new_pages = []
                            for t in topics:
                                # Handle keyword cluster (multiple keywords per topic)
                                keyword_cluster = t.get('keyword_cluster', [])
                                
                                if keyword_cluster:
                                    # NEW FORMAT: "keyword | intent | secondary intent" (no volume)
                                    # Classify intent based on keyword patterns
                                    def classify_intent(kw_text):
                                        kw_lower = kw_text.lower()
                                        # Transactional indicators
                                        if any(word in kw_lower for word in ['buy', 'price', 'shop', 'purchase', 'best', 'top', 'review', 'vs', 'alternative']):
                                            return 'transactional'
                                        # Commercial indicators
                                        elif any(word in kw_lower for word in ['benefits', 'how to', 'uses', 'guide', 'comparison', 'difference']):
                                            return 'commercial'
                                        # Default: informational
                                        else:
                                            return 'informational'
                                    
                                    keywords_str = '\n'.join([
                                        f"{kw['keyword']} | {classify_intent(kw['keyword'])} |"
                                        for kw in keyword_cluster
                                    ])
                                    # Get primary keyword for research reference
                                    primary_kw = next((kw for kw in keyword_cluster if kw.get('is_primary')), keyword_cluster[0] if keyword_cluster else {})
                                else:
                                    keywords_str = ""
                                    primary_kw = {}
                                
                                # Combine general research with topic-specific notes
                                topic_research = research_data.copy()
                                topic_research['notes'] = t.get('research_notes', '')
                                topic_research['keyword_cluster'] = keyword_cluster
                                topic_research['primary_keyword'] = primary_kw.get('keyword', '')
                                
                                new_pages.append({
                                    "project_id": product['project_id'],
                                    "source_page_id": pid,
                                    "url": f"{product['url'].rstrip('/')}/{t['slug']}",
                                    "page_type": "Topic",
                                    "funnel_stage": "MoFu",
                                    "product_action": "Idle",
                                    "tech_audit_data": {
                                        "title": t['title'],
                                        "meta_description": t['description'],
                                        "meta_title": t['title']
                                    },
                                    "content_description": t['description'],
                                    "keywords": keywords_str,  # Data-backed keywords with volume
                                    "slug": t['slug'],
                                    "research_data": topic_research  # Store all research including citations
                                })
                            
                            
                            
                            if new_pages:
                                print(f"DEBUG: Attempting to insert {len(new_pages)} MoFu topics...", file=sys.stderr)
                                try:
                                    insert_res = supabase.table('pages').insert(new_pages).execute()
                                    print("DEBUG: ✓ MoFu topics inserted successfully.", file=sys.stderr)
                                    
                                    # AUTO-KEYWORD RESEARCH (Gemini)
                                    if insert_res.data:
                                        print(f"DEBUG: Starting Auto-Keyword Research for {len(insert_res.data)} topics...", file=sys.stderr)
                                        for inserted_page in insert_res.data:
                                            try:
                                                p_id = inserted_page['id']
                                                # Handle tech_audit_data being a string or dict
                                                t_data = inserted_page.get('tech_audit_data', {})
                                                if isinstance(t_data, str):
                                                    try: t_data = json.loads(t_data)
                                                    except: t_data = {}
                                                    
                                                p_title = t_data.get('title', '')
                                                if not p_title: continue
                                                
                                                log_debug(f"Auto-Researching keywords for: {p_title} (Loc: {project_loc})")
                                                gemini_result = perform_gemini_research(p_title, location=project_loc, language=project_lang)
                                                
                                                if gemini_result:
                                                    keywords = gemini_result.get('keywords', [])
                                                    formatted_keywords = '\n'.join([
                                                        f"{kw.get('keyword', '')} | {kw.get('intent', 'informational')} |"
                                                        for kw in keywords if kw.get('keyword')
                                                    ])
                                                    
                                                    # Create research data (partial)
                                                    research_data = {
                                                        "stage": "keywords_only", 
                                                        "mode": "hybrid",
                                                        "competitor_urls": [c['url'] for c in gemini_result.get('competitors', [])],
                                                        "ranked_keywords": keywords,
                                                        "formatted_keywords": formatted_keywords
                                                    }
                                                    
                                                    supabase.table('pages').update({
                                                        "keywords": formatted_keywords,
                                                        "research_data": research_data
                                                    }).eq('id', p_id).execute()
                                                    log_debug(f"✓ Keywords saved for {p_title}")
                                            except Exception as research_err:
                                                log_debug(f"Auto-Research failed for {p_title}: {research_err}")
                                except Exception as insert_error:
                                    print(f"DEBUG: Error inserting with research_data: {insert_error}", file=sys.stderr)
                                    # Fallback: Try inserting without research_data (if column missing)
                                    if 'research_data' in str(insert_error) or 'column' in str(insert_error):
                                        print("DEBUG: Retrying insert without research_data column...", file=sys.stderr)
                                        for p in new_pages:
                                            p.pop('research_data', None)
                                        supabase.table('pages').insert(new_pages).execute()
                                        print("DEBUG: ✓ MoFu topics inserted (without research data).", file=sys.stderr)
                                    else:
                                        raise insert_error
                            else:
                                print("DEBUG: No new pages to insert (topics list empty).", file=sys.stderr)
                            
                            # Update Source Page Status
                            supabase.table('pages').update({"product_action": "MoFu Generated"}).eq('id', pid).execute()
                        
                        except Exception as e:
                            print(f"DEBUG: Error generating MoFu topics: {e}", file=sys.stderr)
                            import traceback
                            traceback.print_exc()
                            # Reset status on error so frontend doesn't hang
                            supabase.table('pages').update({"product_action": "Failed"}).eq('id', pid).execute()
                            
                except Exception as e:
                    log_debug(f"MoFu Thread Error: {e}")
                    # Ensure we try to reset status for all pages if the whole thread crashes
                    try:
                        supabase.table('pages').update({"product_action": "Failed"}).in_('id', page_ids).execute()
                    except: pass
                            
                except Exception as e:
                    log_debug(f"MoFu Thread Error: {e}")

            # Set status to Processing immediately
            try:
                log_debug(f"Updating status to Processing for {page_ids}")
                supabase.table('pages').update({"product_action": "Processing..."}).in_('id', page_ids).execute()
            except Exception as e:
                log_debug(f"Failed to update status to Processing: {e}")

            # Start background thread
            log_debug("Starting background MoFu thread...")
            thread = threading.Thread(target=process_mofu_generation, args=(page_ids, os.environ.get("GEMINI_API_KEY")))
            thread.start()
            
            return jsonify({"message": "MoFu generation started in background. The status will update to 'Processing...' in the table."})


        elif action == 'conduct_research':
            # SIMPLIFIED: Perplexity Research Brief ONLY
            # (Keywords/Competitors are already done in generate_mofu)
            
            def process_research_background(page_ids, api_key):
                print(f"====== CONDUCT_RESEARCH BACKGROUND THREAD STARTED ======", flush=True)
                log_debug(f"CONDUCT_RESEARCH: Starting for {len(page_ids)} pages")
                
                for page_id in page_ids:
                    print(f"DEBUG: Processing page_id: {page_id}", flush=True)
                    try:
                        # Update status to Processing
                        supabase.table('pages').update({
                            "product_action": "Processing Research..."
                        }).eq('id', page_id).execute()

                        # Get the Topic page
                        page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
                        if not page_res.data: continue
                        
                        page = page_res.data
                        topic_title = page.get('tech_audit_data', {}).get('title', '')
                        research_data = page.get('research_data') or {}
                        
                        if not topic_title: continue
                        
                        log_debug(f"Researching topic (Perplexity): {topic_title}")
                        
                        # Get existing keywords/competitors
                        keywords = research_data.get('ranked_keywords', [])
                        competitor_urls = research_data.get('competitor_urls', [])
                        
                        # Fetch Project Settings for Localization
                        project_res = supabase.table('projects').select('location, language').eq('id', page['project_id']).single().execute()
                        project_loc = project_res.data.get('location', 'US') if project_res.data else 'US'
                        project_lang = project_res.data.get('language', 'English') if project_res.data else 'English'
                        
                        # Fallback: If no keywords (maybe old page), run Gemini now
                        if not keywords:
                            log_debug(f"No keywords found for {topic_title}. Running Gemini fallback (Loc: {project_loc})...")
                            gemini_result = perform_gemini_research(topic_title, location=project_loc, language=project_lang)
                            if gemini_result:
                                keywords = gemini_result.get('keywords', [])
                                competitor_urls = [c['url'] for c in gemini_result.get('competitors', [])]
                                # Update research data immediately
                                research_data.update({
                                    "competitor_urls": competitor_urls,
                                    "ranked_keywords": keywords,
                                    "formatted_keywords": '\n'.join([f"{kw.get('keyword', '')} | {kw.get('intent', 'informational')} |" for kw in keywords])
                                })
                        
                        # Prepare query for Perplexity
                        keyword_list = ", ".join([k.get('keyword', '') for k in keywords[:15]])
                        competitor_list = ", ".join(competitor_urls)
                        
                        research_query = f"""
                        Research Topic: {topic_title}
                        Top Competitors: {competitor_list}
                        Top Keywords: {keyword_list}
                        
                        Create a detailed Content Research Brief for this topic.
                        Analyze the competitors and keywords to find content gaps.
                        Focus on User Pain Points, Key Subtopics, and Scientific/Technical details.
                        """
                        
                        log_debug(f"Starting Perplexity Research for brief (Loc: {project_loc})...")
                        perplexity_result = research_with_perplexity(research_query, location=project_loc, language=project_lang)
                        
                        # Update research data with brief
                        research_data.update({
                            "stage": "complete",
                            "mode": "hybrid",
                            "perplexity_research": perplexity_result.get('research', ''),
                            "citations": perplexity_result.get('citations', [])
                        })
                        
                        # Update page
                        supabase.table('pages').update({
                            "research_data": research_data,
                            "product_action": "Idle"
                        }).eq('id', page_id).execute()
                        
                        log_debug(f"Research complete for {topic_title}")
                        
                    except Exception as e:
                        log_debug(f"Research error: {e}")
                        import traceback
                        traceback.print_exc()
                        # Reset status on error
                        try:
                            supabase.table('pages').update({"product_action": "Idle"}).eq('id', page_id).execute()
                        except: pass

            # Start background thread
            log_debug("Starting background Research thread...")
            thread = threading.Thread(target=process_research_background, args=(page_ids, os.environ.get("GEMINI_API_KEY")))
            thread.start()
            
            return jsonify({"message": "Research started in background. The status will update to 'Processing...' in the table."}), 202


        elif action == 'generate_tofu':
            # AI ToFu Topic Generation
            
            def process_tofu_generation(page_ids, api_key):
                log_debug(f"Background ToFu thread started for pages: {page_ids}")
                try:

                    
                    for pid in page_ids:
                        # Fetch Source MoFu Page
                        mofu_res = supabase.table('pages').select('*').eq('id', pid).single().execute()
                        if not mofu_res.data: continue
                        mofu = mofu_res.data
                        mofu_tech = mofu.get('tech_audit_data') or {}
                        
                        print(f"Researching ToFu opportunities for MoFu topic: {mofu_tech.get('title')}...")
                        
                        # === NEW DATA-FIRST WORKFLOW FOR TOFU ===
                        
                        # Fetch Project Settings for Localization (Moved UP)
                        project_res = supabase.table('projects').select('location, language').eq('id', mofu['project_id']).single().execute()
                        project_loc = project_res.data.get('location', 'US') if project_res.data else 'US'
                        project_lang = project_res.data.get('language', 'English') if project_res.data else 'English'

                        # Step 1: Get broad keyword ideas based on MoFu topic
                        mofu_title = mofu_tech.get('title', '')
                        print(f"Researching ToFu opportunities for: {mofu_title} (Loc: {project_loc})")
                        
                        # Get keyword opportunities from DataForSEO
                        # For ToFu, we want broader terms, so we might strip "Best" or "Review" from the seed
                        seed_keyword = mofu_title.replace('Best ', '').replace('Review', '').replace(' vs ', ' ').strip()
                        # NEW: Use Gemini 2.0 Flash with Grounding as PRIMARY source (User Request)
                        print(f"DEBUG: Using Gemini 2.0 Flash for ToFu keyword research (Primary)...")
                        
                        gemini_result = perform_gemini_research(seed_keyword, location=project_loc, language=project_lang)
                        keywords = []
                        
                        if gemini_result and gemini_result.get('keywords'):
                            print(f"✓ Gemini Research successful. Found {len(gemini_result['keywords'])} keywords.")
                            for k in gemini_result['keywords']:
                                keywords.append({
                                    'keyword': k.get('keyword'),
                                    'volume': 100, # Placeholder
                                    'score': 100,
                                    'cpc': 0,
                                    'competition': 0,
                                    'intent': k.get('intent', 'Informational')
                                })
                        else:
                            print(f"⚠ Gemini Research failed. Using fallback.")
                            keywords = [{'keyword': seed_keyword, 'volume': 0, 'score': 0, 'cpc': 0, 'competition': 0}]
                        
                        print(f"DEBUG: Proceeding to Topic Generation with {len(keywords)} keywords...", flush=True)
                        
                        # Step 2: Analyze SERP for top 5 keywords (Optional - keeping for context if fast enough, or remove for speed)
                        # For now, we'll keep it lightweight or rely on Gemini Grounding in the prompt.
                        # Let's SKIP DataForSEO SERP to save time/cost, and rely on Gemini Grounding.
                        serp_summary = "Relied on Gemini Grounding for current SERP context."
                        
                        # Step 3: Generate Topics (Lightweight - No Perplexity)
                        import datetime
                        current_year = datetime.datetime.now().year
                        
                        # Format keyword list for prompt
                        keyword_list = '\n'.join([f"- {k['keyword']} ({k['volume']}/mo, Score: {k.get('score', 0)})" for k in keywords[:100]])

                        topic_prompt = f"""
                        You are an SEO Strategist. Generate 5 High-Value Top-of-Funnel (ToFu) topic ideas that lead to: {mofu_tech.get('title')}
                        
                        **CONTEXT**:
                        - Target Audience: People at the beginning of their journey (Problem Aware).
                        - Location: {project_loc}
                        - Language: {project_lang}
                        - Goal: Educate them and naturally lead them to the solution (the MoFu topic).
                        
                        **HIGH-OPPORTUNITY KEYWORDS**:
                        {keyword_list}
                        
                        **INSTRUCTIONS**:
                        1.  **Use Grounding**: Search Google to ensure these topics are currently relevant and not already saturated in **{project_loc}**.
                        2.  **Focus**: "What is", "How to", "Guide to", "Benefits of", "Mistakes to Avoid".
                        3.  **Variety**: specific angles, not just generic guides.
                        
                        **LOCALIZATION RULES (CRITICAL)**:
                        1. **Currency**: You MUST use the local currency for **{project_loc}** (e.g., ₹ INR for India). Convert prices if needed.
                        2. **Units**: Use the measurement system standard for **{project_loc}**.
                        3. **Spelling**: Use the correct spelling dialect (e.g., "Colour" for UK/India).
                        4. **Cultural Context**: Use examples relevant to **{project_loc}**.
                        
                        Current Date: {datetime.datetime.now().strftime("%B %Y")}
                        
                        Return a JSON object with a key "topics" containing a list of objects:
                        - "title": Topic Title (Must include a primary keyword)
                        - "slug": URL friendly slug
                        - "description": Brief content description (intent)
                        - "keyword_cluster": List of ALL semantically relevant keywords from the list (aim for 30+ per topic if relevant)
                        - "primary_keyword": The main keyword targeted
                        """
                        
                        try:
                            text = gemini_client.generate_content(
                                prompt=topic_prompt,
                                model_name="gemini-2.5-flash",
                                use_grounding=True
                            )
                            if not text: raise Exception("Empty response from Gemini")
                            text = text.strip()
                            if text.startswith('```json'): text = text[7:]
                            if text.startswith('```'): text = text[3:]
                            if text.endswith('```'): text = text[:-3]
                            
                            data = json.loads(text)
                            topics = data.get('topics', [])
                            
                            new_pages = []
                            for t in topics:
                                # Map selected keywords back to their data
                                cluster_data = []
                                for k_str in t.get('keyword_cluster', []):
                                    match = next((k for k in keywords if k['keyword'].lower() == k_str.lower()), None)
                                    if match: cluster_data.append(match)
                                    else: cluster_data.append({'keyword': k_str, 'volume': 0, 'score': 0, 'intent': 'Informational'})
                                
                                # Standardized Format: "keyword | intent |" (Matches MoFu style)
                                keywords_str = '\n'.join([
                                    f"{k['keyword']} | {k.get('intent', 'Informational')} |"
                                    for k in cluster_data
                                ])
                                
                                # Minimal research data (No Perplexity yet)
                                topic_research = {
                                    "stage": "topic_generated",
                                    "keyword_cluster": cluster_data,
                                    "primary_keyword": t.get('primary_keyword')
                                }

                                new_pages.append({
                                    "project_id": mofu['project_id'],
                                    "source_page_id": pid,
                                    "url": f"{mofu['url'].rsplit('/', 1)[0]}/{t['slug']}", 
                                    "page_type": "Topic",
                                    "funnel_stage": "ToFu",
                                    "product_action": "Idle", # Ready for manual "Conduct Research"
                                    "tech_audit_data": {
                                        "title": t['title'],
                                        "meta_description": t['description'],
                                        "meta_title": t['title']
                                    },
                                    "content_description": t['description'],
                                    "keywords": keywords_str,
                                    "slug": t['slug'],
                                    "research_data": topic_research
                                })
                            
                            if new_pages:
                                print(f"Attempting to insert {len(new_pages)} ToFu topics...")
                                insert_res = supabase.table('pages').insert(new_pages).execute()
                                print("✓ ToFu topics inserted successfully.")
                                
                                # AUTO-KEYWORD RESEARCH (Gemini) - Architecture Parity with MoFu
                                if insert_res.data:
                                    print(f"DEBUG: Starting Auto-Keyword Research for {len(insert_res.data)} ToFu topics...")
                                    for inserted_page in insert_res.data:
                                        try:
                                            p_id = inserted_page['id']
                                            t_data = inserted_page.get('tech_audit_data', {})
                                            if isinstance(t_data, str):
                                                try: t_data = json.loads(t_data)
                                                except: t_data = {}
                                                
                                            p_title = t_data.get('title', '')
                                            if not p_title: continue
                                            
                                            log_debug(f"Auto-Researching keywords for ToFu: {p_title}")
                                            # Use project location/language for research
                                            gemini_result = perform_gemini_research(p_title, location=project_loc, language=project_lang)
                                            
                                            if gemini_result:
                                                keywords = gemini_result.get('keywords', [])
                                                formatted_keywords = '\n'.join([
                                                    f"{kw.get('keyword', '')} | {kw.get('intent', 'informational')} |"
                                                    for kw in keywords if kw.get('keyword')
                                                ])
                                                
                                                # Create research data (partial)
                                                research_data = {
                                                    "stage": "keywords_only", 
                                                    "mode": "hybrid",
                                                    "competitor_urls": [c['url'] for c in gemini_result.get('competitors', [])],
                                                    "ranked_keywords": keywords,
                                                    "formatted_keywords": formatted_keywords
                                                }
                                                
                                                supabase.table('pages').update({
                                                    "keywords": formatted_keywords,
                                                    "research_data": research_data
                                                }).eq('id', p_id).execute()
                                            log_debug(f"✓ Keywords saved for {p_title}")
                                        except Exception as research_err:
                                            log_debug(f"Auto-Research failed for {p_title}: {research_err}")
                            
                            log_debug(f"ToFu generation complete for {pid}. Updating status...")
                            # Update Source Page Status
                            supabase.table('pages').update({"product_action": "ToFu Generated"}).eq('id', pid).execute()
                            log_debug(f"Status updated to 'ToFu Generated' for {pid}")
                            
                        except Exception as e:
                            print(f"Error generating ToFu topics: {e}")
                            import traceback
                            traceback.print_exc()
                            # Reset status on error so frontend doesn't hang
                            supabase.table('pages').update({"product_action": "Failed"}).eq('id', pid).execute()
                
                except Exception as e:
                    log_debug(f"ToFu Thread Error: {e}")
                    # Ensure we try to reset status for all pages if the whole thread crashes
                    try:
                        supabase.table('pages').update({"product_action": "Failed"}).in_('id', page_ids).execute()
                    except: pass

            # Set status to Processing immediately
            try:
                log_debug(f"Updating status to Processing for {page_ids}")
                supabase.table('pages').update({"product_action": "Processing..."}).in_('id', page_ids).execute()
            except Exception as e:
                log_debug(f"Failed to update status to Processing: {e}")

            # Start background thread
            log_debug("Starting background ToFu thread...")
            thread = threading.Thread(target=process_tofu_generation, args=(page_ids, os.environ.get("GEMINI_API_KEY")))
            thread.start()
            
            return jsonify({"message": "ToFu generation started in background. The status will update to 'Processing...' in the table."})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_page_details():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        page_id = request.args.get('page_id')
        if not page_id: return jsonify({"error": "page_id required"}), 400
        
        res = supabase.table('pages').select('*').eq('id', page_id).execute()
        if not res.data: return jsonify({"error": "Page not found"}), 404
        
        return jsonify(res.data[0])
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    except Exception as e:
        print(f"Error in crawl_project: {e}")
        return jsonify({"error": str(e)}), 500





@app.route('/api/generate-image-prompt', methods=['POST'])
def generate_image_prompt_endpoint():
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        topic = data.get('topic')
        project_id = data.get('project_id') # Ensure frontend sends this
        
        # Fetch Project Settings
        project_loc = 'US'
        if project_id:
            project_res = supabase.table('projects').select('location').eq('id', project_id).single().execute()
            if project_res.data:
                project_loc = project_res.data.get('location', 'US')

        prompt = f"""
        You are an expert AI Art Director.
        Create a detailed, high-quality image generation prompt for a blog post titled: "{topic}".
        
        **CONTEXT**:
        - Target Audience Location: {project_loc} (Ensure cultural relevance, e.g., models, setting)
        
        Style: Photorealistic, Cinematic, High-End Editorial.
        The style should be: "Modern, Minimalist, Tech-focused, 3D Render, High Resolution".
        
        Output: Just the prompt text.
        Return ONLY the prompt text. No "Here is the prompt" or quotes.
        """
        
        # model = genai.GenerativeModel('gemini-2.0-flash-exp')
        # response = model.generate_content(prompt)
        
        text = gemini_client.generate_content(
            prompt=prompt,
            model_name="gemini-2.5-flash"
        )
        
        if not text:
            return jsonify({"error": "Gemini generation failed"}), 500
            
        return jsonify({"prompt": text.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/run-migration', methods=['POST'])
def run_migration():
    """Run the photoshoots migration SQL"""
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        # Read the SQL file
        migration_path = os.path.join(BASE_DIR, 'migration_photoshoots.sql')
        with open(migration_path, 'r') as f:
            sql = f.read()
            
        # Execute using Supabase RPC or direct SQL if possible
        # Since Supabase-py client doesn't support direct SQL execution easily without RPC,
        # we'll try to use the 'rpc' method if you have a 'exec_sql' function defined in Postgres
        # OR we can just assume the table exists for now and let the user run it in Supabase dashboard.
        
        # However, to be helpful, let's try to create the table using a raw query if the client supports it.
        # The supabase-py client is a wrapper around postgrest. It doesn't support raw SQL.
        # But we can try to use the 'psycopg2' connection if we had the connection string.
        
        # Since we failed to connect with psycopg2 earlier, we can't run it here either.
        
        return jsonify({"message": "Please run the migration_photoshoots.sql file in your Supabase SQL Editor."}), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ===== PRODUCT PHOTOSHOOT ENDPOINTS =====

@app.route('/api/photoshoots', methods=['GET'])
def get_photoshoots():
    """Get all photoshoot tasks for a project"""
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        project_id = request.args.get('project_id')
        if not project_id:
            return jsonify({"error": "project_id required"}), 400
        
        # 1. Fetch manual photoshoots
        res_tasks = supabase.table('photoshoots').select('*').eq('project_id', project_id).order('created_at', desc=True).execute()
        tasks = res_tasks.data or []
        
        # 2. Fetch blog article images
        # We assume pages belong to the project (linked via project_id if applicable, or we filter by project pages)
        # Since pages table might not have project_id directly (it links via project_pages usually?), let's check.
        # Based on previous code, pages seem to be linked to projects.
        # Let's assume we can filter pages by project_id if that column exists, or we fetch all pages for the project.
        # Wait, the `pages` table schema check:
        # It has `project_id`? Let's check `setup_database_refined.sql` or assume it does based on `loadProject`.
        # `loadProject` fetches pages for a project.
        
        res_pages = supabase.table('pages').select('id, title, image_prompt, main_image_url, updated_at').eq('project_id', project_id).not_.is_('main_image_url', 'null').execute()
        page_images = res_pages.data or []
        
        # 3. Merge and Format
        combined = []
        
        # Add Manual Tasks
        for t in tasks:
            combined.append({
                "id": t['id'],
                "type": "manual",
                "prompt": t['prompt'],
                "status": t['status'],
                "output_image": t['output_image'],
                "aspect_ratio": t.get('aspect_ratio', 'auto'),
                "created_at": t['created_at']
            })
            
        # Add Blog Images
        for p in page_images:
            combined.append({
                "id": p['id'], # Page ID
                "type": "article",
                "prompt": p.get('image_prompt') or f"Blog Image: {p.get('title')}",
                "status": "Done",
                "output_image": p['main_image_url'],
                "aspect_ratio": "16:9",
                "created_at": p.get('updated_at')
            })
            
        # Sort by created_at desc (simple string sort for ISO dates works)
        combined.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        
        return jsonify({"photoshoots": combined})
    except Exception as e:
        print(f"Error fetching photoshoots: {e}")
        return jsonify({"photoshoots": []})

@app.route('/api/photoshoots', methods=['POST'])
def create_photoshoot():
    """Create a new photoshoot task"""
    print("Received create_photoshoot request")
    if not supabase: 
        print("Supabase not configured")
        return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        print(f"Request data: {data}")
        project_id = data.get('project_id')
        prompt = data.get('prompt', '')
        
        if not project_id:
            return jsonify({"error": "project_id required"}), 400
        
        # Insert into database
        new_task = {
            'project_id': project_id,
            'prompt': prompt,
            'status': 'Pending',
            'output_image': None,
            'aspect_ratio': data.get('aspect_ratio', 'auto')
        }
        
        print(f"Inserting task: {new_task}")
        res = supabase.table('photoshoots').insert(new_task).execute()
        print(f"Insert result: {res}")
        return jsonify({"photoshoot": res.data[0] if res.data else new_task})
    except Exception as e:
        print(f"Error creating photoshoot: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/photoshoots/<photoshoot_id>', methods=['PUT'])
def update_photoshoot(photoshoot_id):
    """Update a photoshoot task"""
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        data = request.get_json()
        action = data.get('action')
        
        # Allow updating any field passed in data, excluding 'action' and 'id'
        update_data = {k: v for k, v in data.items() if k not in ['action', 'id', 'project_id']}
        
        # If action is 'run', generate the image
        if action == 'run':


            print(f"Starting generation for task {photoshoot_id}")
            # Get the prompt from the database to be sure
            # Get the prompt and input_image from the database
            current_task = supabase.table('photoshoots').select('prompt, input_image, aspect_ratio').eq('id', photoshoot_id).execute()
            if not current_task.data:
                 return jsonify({"error": "Task not found"}), 404
                 
            task_data = current_task.data[0]
            prompt_text = task_data.get('prompt', '')
            input_image_url = task_data.get('input_image', '')
            db_aspect_ratio = task_data.get('aspect_ratio', '16:9')
            
            if not prompt_text:
                return jsonify({"error": "Prompt is empty"}), 400
                
            # Update status to Processing
            supabase.table('photoshoots').update({'status': 'Processing'}).eq('id', photoshoot_id).execute()
            
            try:
                # content_parts = [prompt_text]
                input_image_b64 = None
                target_aspect = db_aspect_ratio # Use DB value as default
                input_width = None
                input_height = None
                
                # Load input image if it exists
                if input_image_url:
                    try:
                        img = load_image_data(input_image_url)
                        input_width, input_height = img.size
                        print(f"DEBUG: Input image dimensions: {input_width}x{input_height}")
                        
                        # Calculate Aspect Ratio
                        # Logic:
                        # 1. If db_aspect_ratio is 'auto' (or None/empty), we DETECT from input image.
                        # 2. If db_aspect_ratio is explicit (e.g. '16:9', '1:1'), we USE IT directly.
                        
                        if not db_aspect_ratio or db_aspect_ratio == 'auto':
                            ratio = input_width / input_height
                            
                            # Gemini Supported Ratios: 1:1, 3:4, 4:3, 9:16, 16:9
                            # Map to closest
                            if ratio > 1.5: target_aspect = "16:9"
                            elif ratio > 1.1: target_aspect = "4:3"
                            elif ratio < 0.6: target_aspect = "9:16"
                            elif ratio < 0.9: target_aspect = "3:4"
                            else: target_aspect = "1:1"
                            
                            print(f"DEBUG: Auto-Calculated Aspect Ratio: {target_aspect} (from {input_width}x{input_height})")
                        else:
                            # User made an explicit choice (even 16:9)
                            target_aspect = db_aspect_ratio
                            print(f"DEBUG: Using User-Selected Aspect Ratio: {target_aspect}")

                        # CONDITIONAL PROMPT INJECTION
                        # Check if target aspect matches input aspect (approx)
                        input_ratio = input_width / input_height
                        target_ratio_val = 1.0
                        if target_aspect == "16:9": target_ratio_val = 16/9
                        elif target_aspect == "9:16": target_ratio_val = 9/16
                        elif target_aspect == "4:3": target_ratio_val = 4/3
                        elif target_aspect == "3:4": target_ratio_val = 3/4
                        elif target_aspect == "1:1": target_ratio_val = 1.0
                        
                        if abs(input_ratio - target_ratio_val) < 0.1:
                             # Ratios match: Enforce exact dimensions
                             prompt_text += f"\n\nIMPORTANT: The output image MUST be exactly {input_width}x{input_height} pixels. Maintain the exact aspect ratio of the input image."
                        else:
                             # Ratios differ: Enforce target aspect ratio
                             prompt_text += f"\n\nIMPORTANT: The output image MUST be {target_aspect} aspect ratio. Do NOT match the input image dimensions."

                        # Convert PIL Image to Base64
                        import io
                        import base64
                        buffered = io.BytesIO()
                        img.save(buffered, format="JPEG")
                        input_image_b64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
                        # content_parts.append(img)
                    except Exception as e:
                        print(f"Error loading input image: {e}")
                        # Continue without image or fail? Fail seems safer for user expectation
                        return jsonify({"error": f"Failed to load input image: {str(e)}"}), 400
                
                print(f"Generating image with prompt: {prompt_text} and image: {bool(input_image_url)}")
                
                # Save image to Supabase
                filename = f"gen_{photoshoot_id}_{int(time.time())}.png"
                
                # Generate image using gemini_client
                # We need a temporary path for the output
                UPLOAD_FOLDER = os.path.join('public', 'generated_images')
                os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                temp_output_path = os.path.join(UPLOAD_FOLDER, filename)
                
                result_path = gemini_client.generate_image(
                    prompt=prompt_text,
                    output_path=temp_output_path,
                    model_name="gemini-2.5-flash-image",
                    input_image_data=input_image_b64,
                    aspect_ratio=target_aspect
                )
                
                if not result_path:
                    raise Exception("Gemini Image API failed")
                
                # FORCE RESIZE TO EXACT DIMENSIONS - SMART CONDITION
                # Only resize if the target aspect ratio matches the input aspect ratio (approx)
                # This prevents squashing if user selects 1:1 but input is 16:9
                if input_width and input_height:
                    try:
                        input_ratio = input_width / input_height
                        
                        # Parse target_aspect string to float
                        target_ratio_val = 1.0
                        if target_aspect == "16:9": target_ratio_val = 16/9
                        elif target_aspect == "9:16": target_ratio_val = 9/16
                        elif target_aspect == "4:3": target_ratio_val = 4/3
                        elif target_aspect == "3:4": target_ratio_val = 3/4
                        elif target_aspect == "1:1": target_ratio_val = 1.0
                        
                        # Check if ratios match within tolerance
                        if abs(input_ratio - target_ratio_val) < 0.1:
                            from PIL import Image
                            print(f"DEBUG: Ratios match ({input_ratio:.2f} vs {target_ratio_val:.2f}). Resizing output to match input: {input_width}x{input_height}")
                            with Image.open(result_path) as gen_img:
                                resized_img = gen_img.resize((input_width, input_height), Image.Resampling.LANCZOS)
                                resized_img.save(result_path)
                        else:
                            print(f"DEBUG: Ratios mismatch ({input_ratio:.2f} vs {target_ratio_val:.2f}). Skipping resize to preserve aspect ratio.")
                            
                    except Exception as resize_err:
                        print(f"Error resizing generated image: {resize_err}")
                    except Exception as resize_err:
                        print(f"Error resizing generated image: {resize_err}")

                # Read the generated image data
                with open(result_path, 'rb') as f:
                    image_data = f.read()
                
                # Upload to Supabase Storage
                public_url = upload_to_supabase(image_data, filename, bucket_name='photoshoots')
                
                # Update task with output image URL
                supabase.table('photoshoots').update({
                    'status': 'Completed', 
                    'output_image': public_url
                }).eq('id', photoshoot_id).execute()
                
                return jsonify({"message": "Image generated successfully", "url": public_url})
                
            except Exception as e:
                print(f"Generation error: {e}")
                supabase.table('photoshoots').update({'status': 'Failed'}).eq('id', photoshoot_id).execute()
                return jsonify({"error": str(e)}), 500

        elif action == 'upscale':
            print(f"Starting upscale for task {photoshoot_id}")
            
            # Get the output_image from the database
            current_task = supabase.table('photoshoots').select('output_image').eq('id', photoshoot_id).execute()
            if not current_task.data:
                 return jsonify({"error": "Task not found"}), 404
                 
            task_data = current_task.data[0]
            output_image_url = task_data.get('output_image', '')
            
            if not output_image_url:
                return jsonify({"error": "No output image to upscale"}), 400
                
            # Update status to Processing
            supabase.table('photoshoots').update({'status': 'Processing'}).eq('id', photoshoot_id).execute()
            
            try:
                # Load the output image
                print(f"Loading image for upscale from: {output_image_url}")
                img = load_image_data(output_image_url)
                
                # Convert to base64
                import io
                import base64
                buffered = io.BytesIO()
                img.save(buffered, format="JPEG")
                input_image_b64 = base64.b64encode(buffered.getvalue()).decode('utf-8')
                
                upscale_prompt = "Generate a high resolution, 4k, highly detailed, photorealistic version of this image. Maintain the exact composition and details but improve quality and sharpness."
                
                # content_parts = [upscale_prompt, img]
                
                print(f"Generating upscale...")
                # Generate image using gemini_client
                
                filename = f"enhanced_{photoshoot_id}_{int(time.time())}.png"
                UPLOAD_FOLDER = os.path.join('public', 'generated_images')
                os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                temp_output_path = os.path.join(UPLOAD_FOLDER, filename)
                
                result_path = gemini_client.generate_image(
                    prompt=upscale_prompt,
                    output_path=temp_output_path,
                    model_name="gemini-2.5-flash-image",
                    input_image_data=input_image_b64
                )
                
                if not result_path:
                    raise Exception("Gemini Upscale failed")
                
                print("Upscale response received")
                
                # Read the generated image data
                with open(result_path, 'rb') as f:
                    image_data = f.read()
                
                # Upload to Supabase Storage
                public_url = upload_to_supabase(image_data, filename, bucket_name='photoshoots')
                
                # Update task
                supabase.table('photoshoots').update({
                    'status': 'Completed', 
                    'output_image': public_url
                }).eq('id', photoshoot_id).execute()
                
                return jsonify({"message": "Image upscaled successfully", "url": public_url})

            except Exception as e:
                print(f"Upscale error: {e}")
                supabase.table('photoshoots').update({'status': 'Failed'}).eq('id', photoshoot_id).execute()
                return jsonify({"error": str(e)}), 500

                
        # Update the task with final status
        if update_data: # Ensure there's data to update before executing
            res = supabase.table('photoshoots').update(update_data).eq('id', photoshoot_id).execute()
            return jsonify({"photoshoot": res.data[0] if res.data else {}})
        
        return jsonify({"message": "No updates"})
    except Exception as e:
        print(f"Error updating photoshoot: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/photoshoots/<photoshoot_id>', methods=['DELETE'])
def delete_photoshoot(photoshoot_id):
    """Delete a photoshoot task"""
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        supabase.table('photoshoots').delete().eq('id', photoshoot_id).execute()
        return jsonify({"message": "Deleted successfully"})
    except Exception as e:
        print(f"Error deleting photoshoot: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/projects/<project_id>', methods=['DELETE'])
def delete_project(project_id):
    """Delete a project and all associated data"""
    if not supabase: return jsonify({"error": "Supabase not configured"}), 500
    
    try:
        # Delete the project (cascading should handle related data if configured in DB, 
        # otherwise we might need to delete related rows first. Assuming cascade for now or simple delete)
        supabase.table('projects').delete().eq('id', project_id).execute()
        return jsonify({"message": "Project deleted successfully"})
    except Exception as e:
        print(f"Error deleting project: {e}")
        return jsonify({"error": str(e)}), 500

        return jsonify({"error": str(e)}), 500

@app.route('/api/webflow/sites', methods=['POST'])
def webflow_list_sites():
    try:
        data = request.json
        api_key = data.get('api_key')
        if not api_key: return jsonify({"error": "Missing API Key"}), 400
        sites = webflow_client.list_sites(api_key)
        return jsonify({"sites": sites})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/webflow/collections', methods=['POST'])
def webflow_list_collections():
    try:
        data = request.json
        api_key = data.get('api_key')
        site_id = data.get('site_id')
        if not api_key or not site_id: return jsonify({"error": "Missing API Key or Site ID"}), 400
        collections = webflow_client.list_collections(api_key, site_id)
        return jsonify({"collections": collections})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/generate-blog-image', methods=['POST'])
def generate_blog_image_endpoint():
    data = request.json
    page_id = data.get('page_id')
    custom_prompt = data.get('prompt')
    
    if not page_id: return jsonify({"error": "page_id required"}), 400
    
    try:
        # Fetch page
        page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
        if not page_res.data: return jsonify({"error": "Page not found"}), 404
        page = page_res.data
        
        topic = page.get('tech_audit_data', {}).get('title') or page.get('url')
        summary = page.get('content', '')[:500]
        
        # Generate Prompt if not provided
        if not custom_prompt:
            prompt = generate_image_prompt(topic, summary)
        else:
            prompt = custom_prompt
            
        # Generate Image
        image_url = nano_banana_client.generate_image(prompt)
        
        # Update Page
        supabase.table('pages').update({
            'main_image_url': image_url,
            'image_prompt': prompt
        }).eq('id', page_id).execute()
        
        return jsonify({
            "message": "Image generated successfully",
            "image_url": image_url,
            "prompt": prompt
        })
        
    except Exception as e:
        print(f"Error generating blog image: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/publish-webflow', methods=['POST'])
def webflow_publish():
    data = request.json
    page_id = data.get('page_id')
    api_key = data.get('api_key')
    collection_id = data.get('collection_id')
    field_mapping = data.get('field_mapping', {}) # { 'wf_field_slug': 'data_key' }
    
    if not all([page_id, api_key, collection_id]):
        return jsonify({"error": "Missing required fields"}), 400
        
    try:
        # Fetch page
        page_res = supabase.table('pages').select('*').eq('id', page_id).single().execute()
        if not page_res.data: return jsonify({"error": "Page not found"}), 404
        page = page_res.data
        
        # Prepare content
        content_md = page.get('content', '')
        content_html = markdown.markdown(content_md)
        
        # Prepare fields
        site_id = data.get('site_id')  # Frontend needs to pass this
        image_wf_field = None
        image_url = None
        
        fields = {}
        for wf_field, data_key in field_mapping.items():
            value = None
            if data_key == 'title':
                value = page.get('tech_audit_data', {}).get('title') or page.get('url')
            elif data_key == 'slug':
                value = page.get('slug')
            elif data_key == 'content':
                value = content_html
            elif data_key == 'meta_description':
                value = page.get('tech_audit_data', {}).get('meta_description')
            elif data_key == 'main_image':
                # Store for later processing - we need to upload the image first
                image_wf_field = wf_field
                image_url = page.get('main_image_url')
                continue  # Don't add to fields yet
            
            if value:
                fields[wf_field] = value
        
        # Handle image upload if present
        if image_url and site_id and image_wf_field:
            try:
                import tempfile
                import requests as req
                
                # Download image from Supabase URL
                print(f"DEBUG: Downloading image from {image_url}", flush=True)
                img_response = req.get(image_url, timeout=30)
                img_response.raise_for_status()
                
                # Save to temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp:
                    tmp.write(img_response.content)
                    tmp_path = tmp.name
                
                print(f"DEBUG: Image downloaded to {tmp_path}", flush=True)
                
                # Upload to Webflow
                asset = webflow_client.upload_asset(api_key, site_id, tmp_path)
                
                # Use asset ID (or URL) in the field
                # Webflow v2 API might use 'fileId' or 'url' - check the asset response
                if 'id' in asset:
                    fields[image_wf_field] = asset['id']
                    print(f"DEBUG: Using asset ID: {asset['id']}", flush=True)
                elif 'url' in asset:
                    fields[image_wf_field] = asset['url']
                    print(f"DEBUG: Using asset URL: {asset['url']}", flush=True)
                
                # Clean up temp file
                import os
                os.unlink(tmp_path)
                
            except Exception as img_error:
                print(f"WARNING: Failed to upload image to Webflow: {img_error}", flush=True)
                # Continue without image rather than failing entire publish
                
        # Publish
        with open('debug_payload.json', 'w') as f:
            json.dump(fields, f, indent=2)
        print(f"DEBUG: Webflow Payload: {json.dumps(fields, indent=2)}", flush=True)
        res = webflow_client.create_item(api_key, collection_id, fields)

        
        # Update status
        supabase.table('pages').update({'status': 'Published'}).eq('id', page_id).execute()
        
        return jsonify({"message": "Published successfully", "webflow_response": res})
        
    except Exception as e:
        print(f"Error publishing to Webflow: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/download-image', methods=['GET'])
def download_image():
    """
    Download an image with PIL re-encoding to ensure valid JPEG format.
    This fixes corrupt images generated before the PIL fix.
    """
    image_url = request.args.get('url')
    if not image_url:
        return jsonify({"error": "url parameter required"}), 400
    
    try:
        import requests as req
        import tempfile
        from PIL import Image
        import io
        
        print(f"DEBUG: download_image called with URL: {image_url}", flush=True)
        print(f"DEBUG: URL starts with '/': {image_url.startswith('/')}", flush=True)
        
        # Handle relative URLs (e.g., /generated-images/...)
        if image_url.startswith('/'):
            # It's a relative path - read directly from disk
            # Note: URLs use /generated-images/ but directory is generated_images
            clean_path = image_url.lstrip('/').replace('generated-images/', 'generated_images/')
            file_path = os.path.join(BASE_DIR, 'public', clean_path)
            print(f"DEBUG: Reading local file for re-encoding: {file_path}", flush=True)
            print(f"DEBUG: File exists: {os.path.exists(file_path)}", flush=True)
            
            if not os.path.exists(file_path):
                print(f"ERROR: File not found at {file_path}", flush=True)
                return jsonify({"error": f"File not found: {image_url}"}), 404
            
            with open(file_path, 'rb') as f:
                image_data = f.read()
            print(f"DEBUG: Read {len(image_data)} bytes from disk", flush=True)
        else:
            # It's an absolute URL - download it
            print(f"DEBUG: Downloading image for re-encoding: {image_url}", flush=True)
            response = req.get(image_url, timeout=30)
            response.raise_for_status()
            image_data = response.content
            print(f"DEBUG: Downloaded {len(image_data)} bytes", flush=True)
        
        # Load and re-encode with PIL
        img = Image.open(io.BytesIO(image_data))
        
        # Convert to RGB if needed
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Save to bytes buffer
        buffer = io.BytesIO()
        img.save(buffer, 'JPEG', quality=95, optimize=True)
        buffer.seek(0)
        
        # Generate filename from URL or default
        from urllib.parse import urlparse
        parsed = urlparse(image_url)
        filename = os.path.basename(parsed.path) or 'image.jpg'
        if not filename.endswith('.jpg'):
            filename = filename.rsplit('.', 1)[0] + '.jpg'
        
        # Return as download
        from flask import send_file
        return send_file(
            buffer,
            mimetype='image/jpeg',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        print(f"Error downloading image: {e}", flush=True)
        return jsonify({"error": str(e)}), 500


# ============================================================================
# 3-STEP DATABASE-DRIVEN CITATION AUDIT
# ============================================================================

import uuid

@app.route('/api/citation-audit/discover', methods=['POST'])
def citation_audit_discover():
    """
    Step 1: Use Perplexity to discover ALL directories for the service/location.
    Stores discovered directories in Supabase with status='pending'.
    Now links to project_id and fetches NAP from projects table.
    """
    try:
        data = request.get_json()
        log_debug(f"DISCOVER ROUTE HIT. Data: {data}")
        project_id = data.get('project_id')
        
        if not project_id:
            return jsonify({"error": "project_id is required"}), 400
        
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        # Fetch project details (NAP data) - check medical_projects first, then projects
        project = None
        
        # Try medical_projects table first
        try:
            project_res = supabase.table('medical_projects').select('*').eq('id', project_id).single().execute()
            if project_res.data:
                project = project_res.data
                # Map medical_projects fields to expected format
                business_name = project.get('business_name', '')
                location_parts = [p.strip() for p in project.get('location', '').split(',') if p.strip()]
                city = location_parts[0] if location_parts else ''
                state = location_parts[1] if len(location_parts) > 1 else ''
                # Extract Country from Location (3rd part) or Smart Detect
                if len(location_parts) > 2:
                    country = location_parts[2]
                else:
                    # Smart Fallback for Legacy Projects
                    # Default to US, but check State/City for strong signals
                    country = project.get('country', 'United States')
                    
                    # Heuristics
                    state_upper = state.strip().upper()
                    city_lower = city.strip().lower()
                    
                    au_states = {'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT', 'NEW SOUTH WALES'}
                    if state_upper in au_states or city_lower in {'sydney', 'melbourne', 'brisbane', 'perth', 'adelaide'}:
                        country = 'Australia'
                    elif state_upper in {'ON', 'BC', 'QC', 'AB', 'MB', 'SK', 'NS', 'NB'}:
                        country = 'Canada'
                    elif city_lower in {'london', 'manchester', 'birmingham', 'uk'} or state_upper in {'UK'}:
                        country = 'United Kingdom'
                
                street_address = project.get('address', '')
                zip_code = ''
                phone = project.get('phone', '')
                service_type = project.get('service_type', 'medical')
                log_debug(f"Found medical_project: {business_name}, location={project.get('location')}, country={country}")
        except Exception as e:
            log_debug(f"medical_projects lookup failed: {e}")
        
        # Fall back to projects table if not found
        if not project:
            project_res = supabase.table('projects').select('*').eq('id', project_id).single().execute()
            if not project_res.data:
                return jsonify({"error": "Project not found in either projects or medical_projects table"}), 404
            
            project = project_res.data
            business_name = project.get('project_name', '')
            city = project.get('city', '') or project.get('location', '').split(',')[0].strip() if project.get('location') else ''
            state = project.get('state', '') or (project.get('location', '').split(',')[1].strip() if project.get('location') and ',' in project.get('location', '') else '')
            street_address = project.get('street_address', '')
            zip_code = project.get('zip_code', '')
            phone = project.get('phone', '')
            service_type = project.get('service_type', 'medical')
        
        # Final Country Fallback (preserve Smart Detect if set)
        if not country:
             country = project.get('country', '') or 'United States'
        
        if not city or not state:
            log_debug(f"Citation Audit: Missing city/state. city='{city}', state='{state}', location='{project.get('location', '')}'")
            return jsonify({"error": "Project must have city and state set. Please update the project."}), 400
        
        full_address = f"{street_address}, {city}, {state} {zip_code}".strip(", ")
        service_display = service_type.replace("_", " ").title() if service_type else "Medical"
        
        # Check if this project already has audits - use existing audit_id if so
        existing_audit = supabase.table('citation_audits').select('audit_id').eq('project_id', project_id).limit(1).execute()
        if existing_audit.data:
            audit_id = existing_audit.data[0].get('audit_id')
            log_debug(f"Using existing audit_id: {audit_id}")
        else:
            audit_id = str(uuid.uuid4())[:8]
            log_debug(f"Creating new audit_id: {audit_id}")
        
        log_debug(f"Citation Audit Step 1: business={business_name}, city={city}, state={state}, country={country}, service={service_display}")
        log_debug(f"Citation Audit Step 1: Discovering directories for {service_display} in {city}, {state}, {country}")
        
        # Check for required API key
        if not os.environ.get('PERPLEXITY_API_KEY'):
            log_debug("Citation Audit Step 1: PERPLEXITY_API_KEY not configured")
            return jsonify({"error": "PERPLEXITY_API_KEY not configured on server"}), 500
        
        # Perplexity discovery via execution script (Refactored for stability)
        from execution.citation_audit_discovery import discover_directories
        
        # Discover directories using the standardized script
        unique_directories = discover_directories(business_name, city, state, service_type, country)
        
        if not unique_directories:
             return jsonify({"error": "No directories discovered"}), 500

        # FETCH existing directories for this project to avoid duplicates
        # Check by BOTH name AND domain for better dedup
        existing_result = supabase.table('citation_audits').select('directory_name, directory_website').eq('project_id', project_id).execute()
        existing_names = set()
        existing_domains = set()
        if existing_result.data:
            for d in existing_result.data:
                if d.get('directory_name'):
                    existing_names.add(d['directory_name'].lower().strip())
                if d.get('directory_website'):
                    # Extract domain from URL
                    try:
                        from urllib.parse import urlparse
                        domain = urlparse(d['directory_website']).netloc.lower().replace('www.', '')
                        if domain:
                            existing_domains.add(domain)
                    except:
                        pass
        
        log_debug(f"Discover More: Found {len(existing_names)} existing names, {len(existing_domains)} existing domains for project {project_id}")
        
        # Filter out directories that already exist (by name OR domain)
        new_directories = []
        for d in unique_directories:
            name = d.get('name', '').strip()
            url = d.get('url', '')
            
            # Skip if name already exists
            if name.lower() in existing_names:
                log_debug(f"Skipping duplicate by name: {name}")
                continue
            
            # Skip if domain already exists
            if url:
                try:
                    from urllib.parse import urlparse
                    domain = urlparse(url).netloc.lower().replace('www.', '')
                    if domain in existing_domains:
                        log_debug(f"Skipping duplicate by domain: {name} ({domain})")
                        continue
                    # Add to set to prevent duplicates within this batch
                    existing_domains.add(domain)
                except:
                    pass
            
            # Add to set to prevent duplicates within this batch
            existing_names.add(name.lower())
            new_directories.append(d)
        
        log_debug(f"Discover More: {len(new_directories)} new directories to add (filtered from {len(unique_directories)} total)")
        
        if not new_directories:
            return jsonify({
                "success": True,
                "audit_id": audit_id,
                "project_id": project_id,
                "directories_count": 0,
                "total_discovered": len(unique_directories),
                "already_existed": len(unique_directories),
                "directories": [],
                "message": f"All {len(unique_directories)} directories already exist. No new directories to add."
            })

        # Insert only NEW directories into Supabase
        rows = []
        for d in new_directories:
            name = d.get('name', '')
            homepage = d.get('url', '') # Script already validates and cleans this
            
            rows.append({
                "project_id": project_id,
                "audit_id": audit_id,
                "directory_name": name,
                "directory_website": homepage, 
                "category": d.get('category', 'business'),
                "directory_type": d.get('type', 'business'),
                "status": "pending"
            })
        
        result = supabase.table('citation_audits').insert(rows).execute()
        
        log_debug(f"Step 1 Complete: Added {len(rows)} NEW directories (skipped {len(unique_directories) - len(rows)} existing), audit_id={audit_id}")
        
        return jsonify({
            "success": True,
            "audit_id": audit_id,
            "project_id": project_id,
            "directories_count": len(rows),
            "total_discovered": len(unique_directories),
            "already_existed": len(unique_directories) - len(rows),
            "directories": new_directories,
            "message": f"Added {len(rows)} new directories (skipped {len(unique_directories) - len(rows)} existing)"
        })
        
    except Exception as e:
        log_debug(f"Citation Audit Step 1 error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/update/<audit_id>', methods=['PUT'])
def citation_audit_update(audit_id):
    """Update a citation audit entry (directory URL, profile URL)."""
    try:
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        data = request.get_json()
        update_fields = {}
        
        if 'directory_website' in data:
            update_fields['directory_website'] = data['directory_website']
        if 'profile_url' in data:
            update_fields['profile_url'] = data['profile_url']
        
        if not update_fields:
            return jsonify({"error": "No fields to update"}), 400
        
        supabase.table('citation_audits').update(update_fields).eq('id', audit_id).execute()
        
        log_debug(f"Updated citation audit {audit_id}: {update_fields}")
        return jsonify({"success": True, "message": "Updated successfully"})
        
    except Exception as e:
        log_debug(f"Citation Audit Update error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/delete/<audit_id>', methods=['DELETE'])
def citation_audit_delete(audit_id):
    """Delete a citation audit entry."""
    try:
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        supabase.table('citation_audits').delete().eq('id', audit_id).execute()
        
        log_debug(f"Deleted citation audit {audit_id}")
        return jsonify({"success": True, "message": "Deleted successfully"})
        
    except Exception as e:
        log_debug(f"Citation Audit Delete error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/add-directory', methods=['POST'])
def citation_audit_add_directory():
    """Manually add a new directory to a citation audit."""
    try:
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        data = request.get_json()
        project_id = data.get('project_id')
        audit_id = data.get('audit_id')
        directory_name = data.get('directory_name', '').strip()
        directory_website = data.get('directory_website', '').strip()
        profile_url = data.get('profile_url', '').strip()
        category = data.get('category', 'business')
        
        if not project_id:
            return jsonify({"error": "project_id is required"}), 400
        if not directory_name:
            return jsonify({"error": "directory_name is required"}), 400
        if not directory_website:
            return jsonify({"error": "directory_website is required"}), 400
        
        # Check if directory already exists for this project
        existing = supabase.table('citation_audits').select('id').eq('project_id', project_id).ilike('directory_name', directory_name).execute()
        if existing.data:
            return jsonify({"error": f"Directory '{directory_name}' already exists for this project"}), 400
        
        # Determine status based on profile_url
        status = "found" if profile_url else "pending"
        
        # Get audit_id from existing audits if not provided
        if not audit_id:
            existing_audit = supabase.table('citation_audits').select('audit_id').eq('project_id', project_id).limit(1).execute()
            if existing_audit.data:
                audit_id = existing_audit.data[0].get('audit_id', str(uuid.uuid4())[:8])
            else:
                audit_id = str(uuid.uuid4())[:8]
        
        # Insert new directory
        new_row = {
            "project_id": project_id,
            "audit_id": audit_id,
            "directory_name": directory_name,
            "directory_website": directory_website,
            "profile_url": profile_url if profile_url else None,
            "category": category,
            "directory_type": category,
            "status": status
        }
        
        result = supabase.table('citation_audits').insert(new_row).execute()
        
        log_debug(f"Added new directory: {directory_name} for project {project_id}")
        return jsonify({
            "success": True, 
            "message": f"Directory '{directory_name}' added successfully",
            "id": result.data[0]['id'] if result.data else None
        })
        
    except Exception as e:
        log_debug(f"Citation Audit Add Directory error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/citation-audit/status/<id_param>', methods=['GET'])
def citation_audit_status(id_param):
    """
    Get current status of all directories for a project or audit.
    Now primarily uses project_id, with fallback to audit_id for backwards compatibility.
    """
    try:
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        # Try project_id first (preferred - shows ALL directories for project)
        result = supabase.table('citation_audits').select('*').eq('project_id', id_param).order('category').execute()
        rows = result.data
        
        # Fallback to audit_id if project_id returned nothing
        if not rows:
            result = supabase.table('citation_audits').select('*').eq('audit_id', id_param).order('category').execute()
            rows = result.data
        
        if not rows:
            return jsonify({"error": "No audit found with this ID"}), 404
        
        # Calculate summary
        total = len(rows)
        
        # Not Found = Step 2 couldn't find a URL
        not_found = sum(1 for r in rows if r.get('status') == 'not_found')
        
        # Found = has a profile_url (either found or verified status)
        found_with_url = sum(1 for r in rows if r.get('profile_url'))
        
        # Pending NAP = has URL but hasn't been verified yet (status='found')
        pending_nap = sum(1 for r in rows if r.get('status') == 'found' and r.get('profile_url'))
        
        # Verified = Step 3 completed (status='verified')
        verified_total = sum(1 for r in rows if r.get('status') == 'verified')
        
        # Verified OK = all NAP checks pass
        verified_ok = sum(1 for r in rows if r.get('status') == 'verified' and 
                        r.get('nap_name_ok') == True and 
                        r.get('nap_address_ok') == True and 
                        r.get('nap_phone_ok') == True)
        
        # Issues = verified but has at least one NAP problem
        issues = sum(1 for r in rows if r.get('status') == 'verified' and 
                    (r.get('nap_name_ok') == False or 
                     r.get('nap_address_ok') == False or 
                     r.get('nap_phone_ok') == False))
        
        # Pending = waiting for Step 2 (no URL search done yet)
        pending_step2 = sum(1 for r in rows if r.get('status') == 'pending')
        
        return jsonify({
            "audit_id": id_param,
            "summary": {
                "total_directories": total,
                "pending": pending_step2,
                "pending_nap": pending_nap,
                "found": found_with_url,
                "not_found": not_found,
                "verified": verified_ok,
                "issues": issues
            },
            "directories": rows
        })
        
    except Exception as e:
        log_debug(f"Citation Audit Status error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/citation-audit/project/<project_id>', methods=['GET'])
def get_project_audits(project_id):
    """
    Get all citation audits for a project (for loading saved data on page refresh).
    """
    try:
        if not supabase:
            return jsonify({"error": "Supabase not configured"}), 500
        
        result = supabase.table('citation_audits').select('*').eq('project_id', project_id).order('created_at', desc=True).execute()
        
        return jsonify({
            "success": True,
            "audits": result.data or []
        })
        
    except Exception as e:
        log_debug(f"Error fetching project audits: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 3000))
    app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
