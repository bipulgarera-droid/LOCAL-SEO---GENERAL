"""
Citation Audit - Find Submit URLs

Finds submission/claim URLs for directories where business listing was not found.
Uses hardcoded mapping for known directories, falls back to Serper search.
"""

import os
import requests

# Hardcoded mapping of directory domains to their submit/claim URLs
DIRECTORY_SUBMIT_URLS = {
    # Major Business Directories
    'yelp.com': 'https://biz.yelp.com/claim',
    'yellowpages.com': 'https://adsolutions.yp.com/free-listing',
    'bbb.org': 'https://www.bbb.org/get-accredited',
    'manta.com': 'https://www.manta.com/claim',
    'mapquest.com': 'https://www.mapquest.com/my-business',
    'superpages.com': 'https://www.superpages.com/claim',
    'citysearch.com': 'https://www.citysearch.com/claim',
    'foursquare.com': 'https://business.foursquare.com/',
    'hotfrog.com': 'https://www.hotfrog.com/add-your-business',
    'brownbook.net': 'https://www.brownbook.net/business/add/',
    'chamberofcommerce.com': 'https://www.chamberofcommerce.com/add-your-business',
    'merchantcircle.com': 'https://www.merchantcircle.com/signup',
    'judysbook.com': 'https://www.judysbook.com/claim',
    'showmelocal.com': 'https://www.showmelocal.com/Businesses/Add',
    'dexknows.com': 'https://www.dexknows.com/claim',
    'ezlocal.com': 'https://www.ezlocal.com/claim',
    'cylex.us': 'https://www.cylex.us/add-company/',
    'tupalo.com': 'https://www.tupalo.com/add-your-business',
    'localdatabase.com': 'https://www.localdatabase.com/add-business',
    'yellowbot.com': 'https://www.yellowbot.com/claim',
    
    # Healthcare Directories
    'healthgrades.com': 'https://update.healthgrades.com/',
    'vitals.com': 'https://www.vitals.com/doctors/claim',
    'zocdoc.com': 'https://www.zocdoc.com/join',
    'webmd.com': 'https://doctor.webmd.com/providers/claim',
    'ratemds.com': 'https://www.ratemds.com/doctors/claim/',
    'doctoroogle.com': 'https://www.doctoroogle.com/claim',
    'wellness.com': 'https://www.wellness.com/provider/claim',
    'sharecare.com': 'https://www.sharecare.com/doctor/claim',
    'caredash.com': 'https://www.caredash.com/doctors/claim',
    'docspot.com': 'https://www.docspot.com/claim',
    'healthline.com': 'https://www.healthline.com/health/find-doctor-claim',
    'doctoralia.com': 'https://pro.doctoralia.com/',
    'doximity.com': 'https://www.doximity.com/register',
    'castleconnolly.com': 'https://www.castleconnolly.com/register',
    'usnews.com': 'https://health.usnews.com/doctors/claim',
    'realself.com': 'https://www.realself.com/doctors/join',
    'plasticsurgery.org': 'https://find.plasticsurgery.org/update',
    'asps.org': 'https://find.plasticsurgery.org/update',
    
    # Dental Directories
    'dentistry.com': 'https://www.dentistry.com/claim',
    'dentalplans.com': 'https://www.dentalplans.com/claim',
    '1800dentist.com': 'https://www.1800dentist.com/dentist/register',
    'everydayhealth.com': 'https://www.everydayhealth.com/claim',
    'findadentist.ada.org': 'https://www.ada.org/member-center/update-profile',
    'dentist.com': 'https://www.dentist.com/claim',
    'askthedentist.com': 'https://askthedentist.com/claim',
    
    # Insurance/Medicare Directories
    'medicare.gov': 'https://www.medicare.gov/manage-your-health/information-for-providers',
    'healthinsurance.org': 'https://www.healthinsurance.org/claim',
    'ehealthinsurance.com': 'https://www.ehealthinsurance.com/partner',
    
    # Mental Health Directories
    'psychologytoday.com': 'https://www.psychologytoday.com/us/therapists/signup',
    'goodtherapy.org': 'https://www.goodtherapy.org/therapist-signup/',
    'therapytribe.com': 'https://www.therapytribe.com/add-listing/',
    'betterhelp.com': 'https://www.betterhelp.com/counselor-signup/',
    'talkspace.com': 'https://www.talkspace.com/providers',
    'zencare.co': 'https://zencare.co/join',
    'theravive.com': 'https://www.theravive.com/therapist-login',
    
    # Chiropractic/Physical Therapy
    'chirodirectory.com': 'https://www.chirodirectory.com/add-listing/',
    'chirobase.org': 'https://www.chirobase.org/add',
    'spine-health.com': 'https://www.spine-health.com/directory/claim',
    
    # Eye Care
    'allaboutvision.com': 'https://www.allaboutvision.com/claim',
    'aao.org': 'https://www.aao.org/find-ophthalmologist/update',
    
    # Nursing/Home Health
    'caring.com': 'https://www.caring.com/partners/provider-sign-up',
    'senioradvisor.com': 'https://www.senioradvisor.com/claim',
    'aplaceformom.com': 'https://www.aplaceformom.com/partners',
    
    # Local/Maps
    'google.com': 'https://business.google.com/',
    'bing.com': 'https://www.bingplaces.com/',
    'apple.com': 'https://mapsconnect.apple.com/',
    'here.com': 'https://www.here.com/business',
    'waze.com': 'https://biz.waze.com/',
    
    # Social/Review
    'facebook.com': 'https://www.facebook.com/pages/create/',
    'nextdoor.com': 'https://business.nextdoor.com/',
    'alignable.com': 'https://www.alignable.com/signup',
}


def find_submit_url(directory_name, directory_domain):
    """
    Finds the submit/claim URL for a directory.
    
    1. Check hardcoded mapping first (instant)
    2. Fall back to Serper search (2-3 seconds)
    
    Returns: URL string or None
    """
    # Normalize domain
    domain = directory_domain.lower().replace('www.', '').strip('/')
    
    # Check hardcoded mapping
    for known_domain, submit_url in DIRECTORY_SUBMIT_URLS.items():
        if known_domain in domain or domain in known_domain:
            print(f"DEBUG: Found hardcoded submit URL for {directory_name}: {submit_url}", flush=True)
            return submit_url
    
    # Fallback to Serper search
    print(f"DEBUG: No hardcoded URL for {directory_name}, searching with Serper...", flush=True)
    return search_submit_url_serper(directory_name, domain)


def search_submit_url_serper(directory_name, domain):
    """
    Uses Serper to search for submit/claim URL on the directory domain.
    Query: site:domain.com add listing OR claim business OR submit
    """
    serper_key = os.getenv('SERPER_API_KEY')
    if not serper_key:
        print("DEBUG: No SERPER_API_KEY, cannot search for submit URL", flush=True)
        return None
    
    try:
        # Build search query
        query = f"site:{domain} add listing OR claim business OR submit your business OR get listed"
        
        response = requests.post(
            'https://google.serper.dev/search',
            headers={
                'X-API-KEY': serper_key,
                'Content-Type': 'application/json'
            },
            json={
                'q': query,
                'num': 5
            },
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            organic = data.get('organic', [])
            
            if organic:
                # Return first result URL
                first_url = organic[0].get('link', '')
                print(f"DEBUG: Serper found submit URL for {directory_name}: {first_url}", flush=True)
                return first_url
            else:
                print(f"DEBUG: Serper found no results for {directory_name}", flush=True)
                return None
        else:
            print(f"DEBUG: Serper error {response.status_code} for {directory_name}", flush=True)
            return None
            
    except Exception as e:
        print(f"DEBUG: Serper search failed for {directory_name}: {e}", flush=True)
        return None


if __name__ == "__main__":
    # Test
    test_cases = [
        ("Yelp", "yelp.com"),
        ("Healthgrades", "healthgrades.com"),
        ("Unknown Directory", "unknowndirectory.com"),
    ]
    
    for name, domain in test_cases:
        url = find_submit_url(name, domain)
        print(f"{name} ({domain}): {url}")
