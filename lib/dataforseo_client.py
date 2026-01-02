import os
import requests
import json

def log_debug(msg):
    print(f"[DataForSEO] {msg}", flush=True)

def get_serp_competitors(keyword, location_code=2840, limit=5):
    """
    Gets top ranking URLs for a keyword using DataForSEO SERP API.
    Returns list of competitor URLs.
    """
    login = os.environ.get('DATAFORSEO_LOGIN')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    
    if not login or not password:
        log_debug("DataForSEO credentials missing for SERP API")
        return []
    
    try:
        url = "https://api.dataforseo.com/v3/serp/google/organic/live/advanced"
        payload = [{
            "keyword": keyword,
            "location_code": location_code,
            "language_code": "en",
            "depth": 20,  # Get top 20 results
            "device": "desktop"
        }]
        
        log_debug(f"SERP API: Finding competitors for '{keyword}'")
        response = requests.post(url, auth=(login, password), json=payload, timeout=30)
        log_debug(f"SERP API status: {response.status_code}")
        
        data = response.json()
        
        competitors = []
        if data.get('tasks') and data['tasks'][0].get('result'):
            results = data['tasks'][0]['result']
            if results and len(results) > 0 and results[0].get('items'):
                for item in results[0]['items'][:limit]:
                    url_data = item.get('url')
                    title = item.get('title', '')
                    domain = item.get('domain', '')
                    
                    if url_data and domain:
                        competitors.append({
                            'url': url_data,
                            'title': title,
                            'domain': domain,
                            'position': item.get('rank_group', 0)
                        })
        
        log_debug(f"SERP API returned {len(competitors)} competitors")
        return competitors
        
    except Exception as e:
        log_debug(f"SERP API error: {type(e).__name__} - {str(e)}")
        print(f"SERP API error: {e}")
        return []


def get_ranked_keywords_for_url(target_url, location_code=2840, limit=100):
    """
    Gets keywords that a specific URL ranks for using DataForSEO Ranked Keywords API.
    This is the KEY API that generates the keyword list like the screenshot.
    Returns list with format: [{keyword, intent, position}, ...]
    """
    login = os.environ.get('DATAFORSEO_LOGIN')
    password = os.environ.get('DATAFORSEO_PASSWORD')
    
    if not login or not password:
        log_debug("DataForSEO credentials missing for Ranked Keywords API")
        return []
    
    try:
        url = "https://api.dataforseo.com/v3/dataforseo_labs/google/ranked_keywords/live"
        payload = [{
            "target": target_url,
            "location_code": location_code,
            "language_code": "en",
            "limit": limit,
            "filters": [
                ["metrics.organic.pos_1", "<", 30]  # Only keywords ranking in top 30
            ]
        }]
        
        log_debug(f"Ranked Keywords API: Getting keywords for '{target_url[:50]}...'")
        response = requests.post(url, auth=(login, password), json=payload, timeout=30)
        log_debug(f"Ranked Keywords API status: {response.status_code}")
        
        data = response.json()
        
        keywords = []
        if data.get('tasks') and data['tasks'][0].get('result'):
            results = data['tasks'][0]['result']
            if results and len(results) > 0 and results[0].get('items'):
                for item in results[0]['items']:
                    keyword = item.get('keyword')
                    
                    # Get keyword info
                    metrics = item.get('metrics', {}).get('organic', {})
                    position = metrics.get('pos_1', 999)
                    
                    # Get keyword data for intent
                    kw_data = item.get('keyword_data', {})
                    kw_info = kw_data.get('keyword_info', {})
                    
                    # Classify intent based on keyword patterns
                    intent = classify_keyword_intent(keyword, kw_info)
                    
                    if keyword:
                        keywords.append({
                            'keyword': keyword,
                            'intent': intent,
                            'position': position,
                            'search_volume': kw_info.get('search_volume', 0)
                        })
        
        log_debug(f"Ranked Keywords API returned {len(keywords)} keywords")
        return keywords
        
    except Exception as e:
        log_debug(f"Ranked Keywords API error: {type(e).__name__} - {str(e)}")
        print(f"Ranked Keywords API error: {e}")
        return []


def classify_keyword_intent(keyword, kw_info=None):
    """
    Classifies keyword intent as: informational, commercial, transactional
    Returns string with primary and optional secondary intents
    """
    kw_lower = keyword.lower()
    intents = []
    
    # Transactional indicators
    if any(word in kw_lower for word in ['buy', 'price', 'shop', 'purchase', 'order', 'discount', 'coupon', 'deal']):
        intents.append('transactional')
    
    # Commercial indicators
    if any(word in kw_lower for word in ['best', 'top', 'review', 'vs', 'versus', 'alternative', 'compare', 'comparison']):
        intents.append('commercial')
    
    # Informational indicators
    if any(word in kw_lower for word in ['what is', 'how to', 'benefits', 'guide', 'tutorial', 'tips', 'made from', 'function of']):
        intents.append('informational')
    
    # Default to informational if no clear signals
    if not intents:
        intents.append('informational')
    
    # Format: "primary | secondary" or just "primary |"
    if len(intents) == 1:
        return f"{intents[0]}"
    else:
        return f"{intents[0]}, {', '.join(intents[1:])}"
