"""
Comprehensive Citation Audit Test Suite

Tests the full citation audit flow:
1. Create projects with different business types
2. Step 1: Discover directories
3. Step 2: Find URLs
4. Step 3: Verify NAP
5. Report results and issues
"""

import requests
import json
import time
from datetime import datetime

BASE_URL = "http://localhost:3000"

# Test businesses - 3 different medical service types, different locations
TEST_BUSINESSES = [
    {
        "name": "Dermatologist in LA",
        "project_name": "Cedars-Sinai Dermatology",
        "service_type": "dermatology",
        "doctor_name": "",
        "street_address": "8635 W 3rd St",
        "city": "Los Angeles",
        "state": "CA",
        "zip_code": "90048",
        "phone": "310-423-3277",
        "focus": "Medical"
    },
    {
        "name": "Cardiologist in Chicago",
        "project_name": "Northwestern Medicine Cardiology",
        "service_type": "cardiology",
        "doctor_name": "",
        "street_address": "251 E Huron St",
        "city": "Chicago",
        "state": "IL",
        "zip_code": "60611",
        "phone": "312-926-2000",
        "focus": "Medical"
    },
    {
        "name": "Pediatrician in Miami",
        "project_name": "Nicklaus Children's Hospital",
        "service_type": "pediatrics",
        "doctor_name": "",
        "street_address": "3100 SW 62nd Ave",
        "city": "Miami",
        "state": "FL",
        "zip_code": "33155",
        "phone": "305-666-6511",
        "focus": "Medical"
    },
]

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

def create_project(business):
    """Create a new project."""
    log(f"Creating project: {business['project_name']}")
    
    payload = {
        "domain": business["project_name"],
        "project_name": business["project_name"],
        "language": "English",
        "location": f"{business['city']}, {business['state']}",
        "focus": business["focus"],
        "doctor_name": business.get("doctor_name", ""),
        "service_type": business["service_type"],
        "street_address": business["street_address"],
        "city": business["city"],
        "state": business["state"],
        "zip_code": business["zip_code"],
        "phone": business["phone"]
    }
    
    try:
        resp = requests.post(f"{BASE_URL}/api/create-project", json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            project_id = data.get("project_id")
            log(f"  ✅ Created project_id: {project_id}")
            return project_id
        else:
            log(f"  ❌ Failed: {resp.status_code} - {resp.text[:200]}", "ERROR")
            return None
    except Exception as e:
        log(f"  ❌ Exception: {e}", "ERROR")
        return None

def run_step1_discover(project_id, business):
    """Step 1: Discover directories."""
    log(f"Step 1: Discovering directories for {business['project_name']}")
    
    payload = {
        "project_id": project_id,
        "service_type": business["service_type"],
        "city": business["city"],
        "state": business["state"]
    }
    
    try:
        resp = requests.post(f"{BASE_URL}/api/citation-audit/discover", json=payload, timeout=120)
        if resp.status_code == 200:
            data = resp.json()
            audit_id = data.get("audit_id", "")
            count = data.get("directories_count", 0)
            log(f"  ✅ Found {count} directories, audit_id: {audit_id[:8]}...")
            return audit_id, count
        else:
            log(f"  ❌ Failed: {resp.status_code} - {resp.text[:200]}", "ERROR")
            return None, 0
    except Exception as e:
        log(f"  ❌ Exception: {e}", "ERROR")
        return None, 0

def run_step2_find_urls(project_id, business):
    """Step 2: Find URLs (runs in batches of 5)."""
    log(f"Step 2: Finding URLs for {business['project_name']}")
    
    payload = {"project_id": project_id}
    total_found = 0
    batches = 0
    max_batches = 6  # 6 batches * 15 = 90 directories max
    
    while batches < max_batches:
        try:
            resp = requests.post(f"{BASE_URL}/api/citation-audit/find-urls", json=payload, timeout=180)
            if resp.status_code == 200:
                data = resp.json()
                
                if "message" in data and "No pending" in data.get("message", ""):
                    log(f"  ✅ All directories processed")
                    break
                
                processed = data.get("processed", 0)
                found = data.get("found", 0)
                total_found += found
                batches += 1
                
                log(f"  Batch {batches}: Processed {processed}, Found {found}")
                
                if processed == 0:
                    break
            else:
                log(f"  ❌ Failed: {resp.status_code} - {resp.text[:200]}", "ERROR")
                break
        except Exception as e:
            log(f"  ❌ Exception: {e}", "ERROR")
            break
    
    log(f"  ✅ Step 2 complete: Found URLs for {total_found} directories")
    return total_found

def run_step3_verify_nap(project_id, business):
    """Step 3: Verify NAP for found URLs."""
    log(f"Step 3: Verifying NAP for {business['project_name']}")
    
    payload = {"project_id": project_id}
    
    try:
        resp = requests.post(f"{BASE_URL}/api/citation-audit/verify-nap", json=payload, timeout=300)
        if resp.status_code == 200:
            data = resp.json()
            verified = data.get("verified_count", 0)
            issues = data.get("nap_issues_count", 0)
            log(f"  ✅ Verified {verified} listings, {issues} with NAP issues")
            return verified, issues
        elif resp.status_code == 404:
            log(f"  ⚠️ No found listings to verify", "WARN")
            return 0, 0
        else:
            log(f"  ❌ Failed: {resp.status_code} - {resp.text[:200]}", "ERROR")
            return 0, 0
    except Exception as e:
        log(f"  ❌ Exception: {e}", "ERROR")
        return 0, 0

def get_audit_summary(project_id):
    """Get summary of audit results."""
    try:
        # First get the audit_id from citation_audits
        resp = requests.get(f"{BASE_URL}/api/citation-audit/project/{project_id}", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            audits = data.get("audits", [])
            
            summary = {
                "total": len(audits),
                "pending": sum(1 for a in audits if a.get("status") == "pending"),
                "found": sum(1 for a in audits if a.get("status") == "found"),
                "not_found": sum(1 for a in audits if a.get("status") == "not_found"),
                "verified": sum(1 for a in audits if a.get("status") == "verified"),
                "not_searchable": sum(1 for a in audits if a.get("status") == "not_searchable"),
                "nap_ok": sum(1 for a in audits if a.get("nap_name_ok") and a.get("nap_address_ok") and a.get("nap_phone_ok")),
            }
            return summary
    except:
        pass
    return None

def run_full_test():
    """Run complete test suite."""
    log("=" * 60)
    log("COMPREHENSIVE CITATION AUDIT TEST")
    log("=" * 60)
    
    results = []
    
    for i, business in enumerate(TEST_BUSINESSES):
        log("")
        log(f"{'='*60}")
        log(f"TEST {i+1}/{len(TEST_BUSINESSES)}: {business['name']}")
        log(f"{'='*60}")
        
        result = {
            "business": business["name"],
            "project_name": business["project_name"],
            "project_id": None,
            "step1_directories": 0,
            "step2_found": 0,
            "step3_verified": 0,
            "step3_issues": 0,
            "errors": []
        }
        
        # Create project
        project_id = create_project(business)
        if not project_id:
            result["errors"].append("Failed to create project")
            results.append(result)
            continue
        result["project_id"] = project_id
        
        # Step 1: Discover
        audit_id, dir_count = run_step1_discover(project_id, business)
        result["step1_directories"] = dir_count
        if not audit_id:
            result["errors"].append("Step 1 failed")
            results.append(result)
            continue
        
        time.sleep(2)  # Brief pause between steps
        
        # Step 2: Find URLs
        found_count = run_step2_find_urls(project_id, business)
        result["step2_found"] = found_count
        
        time.sleep(2)
        
        # Step 3: Verify NAP
        verified, issues = run_step3_verify_nap(project_id, business)
        result["step3_verified"] = verified
        result["step3_issues"] = issues
        
        # Get final summary
        summary = get_audit_summary(project_id)
        if summary:
            result["summary"] = summary
            log(f"  Summary: {summary}")
        
        results.append(result)
        log(f"\n  ✅ Test complete for {business['project_name']}")
    
    # Final Report
    log("")
    log("=" * 60)
    log("FINAL REPORT")
    log("=" * 60)
    
    for r in results:
        status = "✅ PASS" if not r["errors"] else "❌ FAIL"
        log(f"\n{status} {r['project_name']}")
        log(f"   Directories: {r['step1_directories']}")
        log(f"   URLs Found: {r['step2_found']}")
        log(f"   NAP Verified: {r['step3_verified']} ({r['step3_issues']} issues)")
        if r["errors"]:
            log(f"   Errors: {r['errors']}")
        if "summary" in r:
            log(f"   Summary: {r['summary']}")
    
    # Success rate
    passed = sum(1 for r in results if not r["errors"])
    log("")
    log(f"TOTAL: {passed}/{len(results)} tests passed")
    
    return results

if __name__ == "__main__":
    run_full_test()
