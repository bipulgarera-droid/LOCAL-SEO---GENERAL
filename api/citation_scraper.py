"""
Real Web Scraping Module for Citation Audit
Uses concurrent requests for speed and actual page visits for accuracy.
"""
import asyncio
import aiohttp
from urllib.parse import quote_plus, urljoin
import re
from typing import Dict, List, Optional, Tuple
import time

# Search URL patterns for each directory
DIRECTORY_SCRAPERS = {
    # === BUSINESS DIRECTORIES ===
    "Google Business Profile": {
        "search_url": "https://www.google.com/maps/search/{business}+{city}+{state}",
        "type": "business"
    },
    "Yelp": {
        "search_url": "https://www.yelp.com/search?find_desc={business}&find_loc={city}%2C+{state}",
        "type": "business"
    },
    "Yellow Pages": {
        "search_url": "https://www.yellowpages.com/search?search_terms={business}&geo_location_terms={city}%2C+{state}",
        "type": "business"
    },
    "BBB": {
        "search_url": "https://www.bbb.org/search?find_country=USA&find_text={business}&find_loc={city}%2C+{state}",
        "type": "business"
    },
    "Manta": {
        "search_url": "https://www.manta.com/search?search={business}&search_location={city}%2C+{state}",
        "type": "business"
    },
    "Facebook": {
        "search_url": "https://www.facebook.com/search/pages/?q={business}+{city}+{state}",
        "type": "business"
    },
    
    # === PROVIDER DIRECTORIES ===
    "Healthgrades": {
        "search_url": "https://www.healthgrades.com/search?what={doctor}&where={city}%2C+{state}",
        "type": "provider"
    },
    "Vitals": {
        "search_url": "https://www.vitals.com/search?q={doctor}&loc={city}%2C+{state}",
        "type": "provider"
    },
    "Zocdoc": {
        "search_url": "https://www.zocdoc.com/search?dr_query={doctor}&address={city}%2C+{state}",
        "type": "provider"
    },
    "WebMD": {
        "search_url": "https://doctor.webmd.com/results?q={doctor}&city={city}&state={state}",
        "type": "provider"
    },
    "RateMDs": {
        "search_url": "https://www.ratemds.com/best-doctors/{state}/{city}/?text={doctor}",
        "type": "provider"
    },
    "CareDash": {
        "search_url": "https://www.caredash.com/search?q={doctor}&location={city}%2C+{state}",
        "type": "provider"
    },
    
    # === DENTAL ===
    "1-800-DENTIST": {
        "search_url": "https://www.1800dentist.com/dentists/{state}/{city}",
        "type": "dental"
    },
    "Dentistry.com": {
        "search_url": "https://www.dentistry.com/dentist/search?q={business}&location={city}%2C+{state}",
        "type": "dental"
    },
    
    # === MENTAL HEALTH ===
    "Psychology Today": {
        "search_url": "https://www.psychologytoday.com/us/therapists/{state}/{city}?search={doctor}",
        "type": "mental_health"
    },
    "GoodTherapy": {
        "search_url": "https://www.goodtherapy.org/therapists/{state}/{city}?search={doctor}",
        "type": "mental_health"
    },
    
    # === AESTHETICS ===
    "RealSelf": {
        "search_url": "https://www.realself.com/find/{city}--{state}?query={doctor}",
        "type": "aesthetics"
    },
    
    # === CHIROPRACTIC ===
    "Find a Chiropractor": {
        "search_url": "https://www.findachiro.org/find/?q={business}&loc={city}%2C+{state}",
        "type": "chiropractic"
    },
}

# Service type to directory mapping
SERVICE_TO_DIRECTORIES = {
    "_core": ["Google Business Profile", "Yelp", "Yellow Pages", "BBB", "Manta", "Healthgrades", "Vitals"],
    
    "general_practice": ["Zocdoc", "WebMD", "RateMDs", "CareDash"],
    "internal_medicine": ["Zocdoc", "WebMD", "RateMDs", "CareDash"],
    "pediatrics": ["Zocdoc", "WebMD", "Healthgrades"],
    "geriatrics": ["Zocdoc", "CareDash"],
    
    "general_dentistry": ["1-800-DENTIST", "Dentistry.com", "Zocdoc"],
    "orthodontics": ["1-800-DENTIST", "Dentistry.com"],
    "periodontics": ["1-800-DENTIST", "Dentistry.com"],
    "endodontics": ["1-800-DENTIST"],
    "oral_surgery": ["1-800-DENTIST", "RealSelf"],
    "pediatric_dentistry": ["1-800-DENTIST"],
    "cosmetic_dentistry": ["1-800-DENTIST", "RealSelf"],
    "prosthodontics": ["1-800-DENTIST"],
    
    "psychiatry": ["Psychology Today", "Zocdoc"],
    "psychology": ["Psychology Today", "GoodTherapy"],
    "counseling": ["Psychology Today", "GoodTherapy"],
    "addiction": ["Psychology Today"],
    
    "optometry": ["Zocdoc", "Healthgrades"],
    "ophthalmology": ["Zocdoc", "Healthgrades"],
    "lasik": ["RealSelf", "Zocdoc"],
    
    "dermatology": ["RealSelf", "Zocdoc"],
    "plastic_surgery": ["RealSelf", "Zocdoc"],
    "cosmetic": ["RealSelf", "Zocdoc"],
    
    "orthopedics": ["Zocdoc", "Healthgrades"],
    "chiropractic": ["Find a Chiropractor", "Healthgrades"],
    "physical_therapy": ["Zocdoc", "Healthgrades"],
    "pain_management": ["Zocdoc", "Healthgrades"],
    
    "obgyn": ["Zocdoc", "Healthgrades"],
    "fertility": ["Zocdoc"],
    "midwifery": ["Zocdoc"],
    
    "cardiology": ["Zocdoc", "Healthgrades"],
    "neurology": ["Zocdoc", "Healthgrades"],
    "gastroenterology": ["Zocdoc", "Healthgrades"],
    "oncology": ["Zocdoc", "Healthgrades"],
    "urology": ["Zocdoc", "Healthgrades"],
    "pulmonology": ["Zocdoc", "Healthgrades"],
    "nephrology": ["Zocdoc"],
    "rheumatology": ["Zocdoc", "Healthgrades"],
    "endocrinology": ["Zocdoc", "Healthgrades"],
    "allergy_immunology": ["Zocdoc", "Healthgrades"],
    "ent": ["Zocdoc", "Healthgrades"],
    
    "general_surgery": ["Zocdoc", "Healthgrades"],
    "vascular_surgery": ["Zocdoc"],
    "bariatric": ["RealSelf", "Zocdoc"],
    
    "urgent_care": ["Zocdoc", "Healthgrades"],
    "emergency": ["Healthgrades"],
    
    "acupuncture": ["Healthgrades"],
    "naturopathy": ["Healthgrades"],
    "functional_medicine": ["Healthgrades"],
    "massage_therapy": ["Yelp"],
    
    "home_health": ["Healthgrades"],
    "pharmacy": ["Yelp", "Google Business Profile"],
    "medical_equipment": ["Yelp", "Google Business Profile"],
    "other": ["Zocdoc", "CareDash"],
}


def get_directories_for_scraping(service_type: str) -> List[str]:
    """Get list of directories to scrape for a service type."""
    dirs = SERVICE_TO_DIRECTORIES.get("_core", []).copy()
    service_dirs = SERVICE_TO_DIRECTORIES.get(service_type, [])
    for d in service_dirs:
        if d not in dirs:
            dirs.append(d)
    return dirs


async def check_directory(
    session: aiohttp.ClientSession,
    directory_name: str,
    doctor_name: str,
    business_name: str,
    city: str,
    state: str,
    phone: str,
    address: str
) -> Dict:
    """Check a single directory for the business listing."""
    
    result = {
        "directory": directory_name,
        "directory_type": "business",
        "status": "not_found",
        "profile_url": None,
        "nap_check": {
            "name_match": "n/a",
            "address_match": "n/a",
            "phone_match": "n/a",
            "details": ""
        },
        "notes": ""
    }
    
    scraper_config = DIRECTORY_SCRAPERS.get(directory_name)
    if not scraper_config:
        result["notes"] = "No scraper configured"
        return result
    
    result["directory_type"] = scraper_config.get("type", "business")
    
    # Build search URL
    search_url_template = scraper_config["search_url"]
    
    # Use doctor name for provider directories, business name for business directories
    search_name = doctor_name if doctor_name and scraper_config["type"] == "provider" else business_name
    
    search_url = search_url_template.format(
        business=quote_plus(business_name),
        doctor=quote_plus(doctor_name or business_name),
        city=quote_plus(city),
        state=quote_plus(state)
    )
    
    result["search_url"] = search_url
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        
        async with session.get(search_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as response:
            if response.status == 200:
                text = await response.text()
                
                # Check if business name appears in results
                business_lower = business_name.lower()
                doctor_lower = (doctor_name or "").lower()
                text_lower = text.lower()
                
                # Look for signs of a listing
                name_found = business_lower in text_lower or (doctor_lower and doctor_lower in text_lower)
                phone_found = phone.replace("-", "").replace("(", "").replace(")", "").replace(" ", "") in text.replace("-", "").replace("(", "").replace(")", "").replace(" ", "")
                
                if name_found:
                    result["status"] = "found"
                    result["profile_url"] = str(response.url)
                    result["nap_check"]["name_match"] = "exact" if name_found else "n/a"
                    result["nap_check"]["phone_match"] = "exact" if phone_found else "n/a"
                    
                    # Check for address components
                    city_found = city.lower() in text_lower
                    state_found = state.lower() in text_lower
                    if city_found and state_found:
                        result["nap_check"]["address_match"] = "partial"
                    
                    result["notes"] = f"Found listing on {directory_name}"
                else:
                    result["status"] = "not_found"
                    result["notes"] = "Business not found in search results"
                    
            elif response.status == 403:
                result["notes"] = "Access blocked (403)"
            elif response.status == 404:
                result["notes"] = "Directory page not found"
            else:
                result["notes"] = f"HTTP {response.status}"
                
    except asyncio.TimeoutError:
        result["notes"] = "Request timed out"
    except aiohttp.ClientError as e:
        result["notes"] = f"Connection error: {str(e)[:50]}"
    except Exception as e:
        result["notes"] = f"Error: {str(e)[:50]}"
    
    return result


async def run_citation_audit(
    doctor_name: str,
    business_name: str,
    service_type: str,
    city: str,
    state: str,
    phone: str,
    address: str
) -> Dict:
    """Run citation audit across all relevant directories concurrently."""
    
    start_time = time.time()
    
    # Get directories to check
    directories = get_directories_for_scraping(service_type)
    
    # Create session with connection pooling
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=2)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # Create tasks for all directories
        tasks = [
            check_directory(
                session,
                directory,
                doctor_name,
                business_name,
                city,
                state,
                phone,
                address
            )
            for directory in directories
        ]
        
        # Run all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results
    processed_results = []
    found_count = 0
    nap_correct = 0
    nap_issues = 0
    
    for r in results:
        if isinstance(r, Exception):
            processed_results.append({
                "directory": "Unknown",
                "status": "error",
                "notes": str(r)[:100]
            })
        else:
            processed_results.append(r)
            if r.get("status") == "found":
                found_count += 1
                # Check NAP
                nap = r.get("nap_check", {})
                if nap.get("name_match") == "exact" and nap.get("phone_match") == "exact":
                    nap_correct += 1
                elif nap.get("name_match") in ["partial", "mismatch"] or nap.get("phone_match") == "mismatch":
                    nap_issues += 1
    
    elapsed = time.time() - start_time
    
    return {
        "service_type": service_type,
        "ground_truth": {
            "doctor_name": doctor_name,
            "clinic_name": business_name,
            "full_address": address,
            "phone": phone
        },
        "city_analysis": {
            "city": city,
            "state": state,
            "country": "USA"
        },
        "results": processed_results,
        "summary": {
            "total_directories_checked": len(directories),
            "found_count": found_count,
            "missing_count": len(directories) - found_count,
            "nap_correct_count": nap_correct,
            "nap_issues_count": nap_issues,
            "scan_time_seconds": round(elapsed, 1)
        },
        "priority_actions": [
            {"action": "create", "directory": r["directory"], "issue": "No listing found"}
            for r in processed_results
            if r.get("status") == "not_found" and r.get("directory_type") in ["business", "provider"]
        ][:10]  # Top 10 priorities
    }


def run_citation_audit_sync(
    doctor_name: str,
    business_name: str,
    service_type: str,
    city: str,
    state: str,
    phone: str,
    address: str
) -> Dict:
    """Synchronous wrapper for the async citation audit."""
    return asyncio.run(run_citation_audit(
        doctor_name, business_name, service_type, city, state, phone, address
    ))
