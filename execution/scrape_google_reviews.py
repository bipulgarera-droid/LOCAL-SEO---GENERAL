"""
Apify Google Reviews Scraper

Uses the Apify Google Maps Reviews actor to scrape reviews for a business.
Requires APIFY_API_KEY in environment variables.
"""

import os
import requests
import time
from typing import Optional

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
ACTOR_ID = "Xb8osYTtOjlsgI6k9"  # Google Maps Reviews Scraper


def scrape_google_reviews(
    google_maps_url: str,
    max_reviews: int = 20,
    language: str = "en",
    sort_by: str = "newest"
) -> dict:
    """
    Scrape Google reviews using Apify actor.
    
    Args:
        google_maps_url: Full Google Maps URL for the business
        max_reviews: Maximum number of reviews to fetch (default 100)
        language: Language filter (default "en")
        sort_by: Sort order - "newest", "highest", "lowest" (default "newest")
    
    Returns:
        dict with 'success', 'reviews', 'error' keys
    """
    
    if not APIFY_API_KEY:
        return {"success": False, "error": "APIFY_API_KEY not configured", "reviews": []}
    
    # Apify API endpoint to run actor
    run_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_API_KEY}"
    
    # Actor input configuration
    actor_input = {
        "startUrls": [{"url": google_maps_url}],
        "maxReviews": max_reviews,
        "language": language,
        "sort": sort_by,
        "personalDataOptions": "personalDataDisabled"  # Privacy-safe
    }
    
    try:
        # Start the actor run
        print(f"Starting Apify actor for: {google_maps_url}")
        run_response = requests.post(
            run_url,
            json=actor_input,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if run_response.status_code != 201:
            return {
                "success": False, 
                "error": f"Failed to start actor: {run_response.text}",
                "reviews": []
            }
        
        run_data = run_response.json()
        run_id = run_data["data"]["id"]
        print(f"Actor run started: {run_id}")
        
        # Poll for completion (max 5 minutes)
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_API_KEY}"
        max_wait = 300  # 5 minutes
        wait_time = 0
        poll_interval = 5
        
        while wait_time < max_wait:
            time.sleep(poll_interval)
            wait_time += poll_interval
            
            status_response = requests.get(status_url, timeout=30)
            status_data = status_response.json()
            status = status_data["data"]["status"]
            
            print(f"Actor status: {status} (waited {wait_time}s)")
            
            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
                return {
                    "success": False,
                    "error": f"Actor run {status}",
                    "reviews": []
                }
        
        if wait_time >= max_wait:
            return {"success": False, "error": "Timeout waiting for results", "reviews": []}
        
        # Fetch results from dataset
        dataset_id = status_data["data"]["defaultDatasetId"]
        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_API_KEY}"
        
        dataset_response = requests.get(dataset_url, timeout=30)
        results = dataset_response.json()
        
        # Parse reviews from results
        reviews = []
        for item in results:
            # The actor returns reviews in a nested structure
            if "reviews" in item:
                for review in item["reviews"][:max_reviews]:
                    reviews.append({
                        "reviewer_name": review.get("name", "Anonymous"),
                        "star_rating": review.get("rating", review.get("stars", 5)),
                        "review_date": review.get("publishedAtDate", review.get("date")),
                        "review_text": review.get("text", "")
                    })
            else:
                # Single review format
                reviews.append({
                    "reviewer_name": item.get("name", item.get("reviewerName", "Anonymous")),
                    "star_rating": item.get("rating", item.get("stars", 5)),
                    "review_date": item.get("publishedAtDate", item.get("date")),
                    "review_text": item.get("text", item.get("reviewText", ""))
                })
        
        print(f"Scraped {len(reviews)} reviews")
        return {
            "success": True,
            "reviews": reviews[:max_reviews],
            "total_found": len(reviews),
            "error": None
        }
        
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": str(e), "reviews": []}
    except Exception as e:
        return {"success": False, "error": str(e), "reviews": []}


if __name__ == "__main__":
    # Test with a sample Google Maps URL
    test_url = "https://www.google.com/maps/place/The+Cancer+%26+Hematology+Centers/@38.7504096,-121.2880389,17z"
    result = scrape_google_reviews(test_url, max_reviews=10)
    print(f"Result: {result}")
