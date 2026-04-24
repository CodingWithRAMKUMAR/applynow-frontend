import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from supabase import create_client, Client
from jobspy import scrape_jobs
import time
import random
import math

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

# Search terms – focus on fresher roles
ROLES = [
    "Software Engineer fresher", "Data Analyst entry level", "Python Developer fresher",
    "DevOps Engineer entry level", "Cyber Security fresher", "Java Developer fresher",
    "Frontend Developer fresher", "Backend Developer fresher", "Full Stack fresher",
    "Android Developer fresher", "iOS Developer fresher", "Cloud Engineer fresher",
    "AWS fresher", "Azure fresher", "Network Engineer fresher", "Support Engineer fresher",
    "QA Tester fresher", "Manual Testing fresher", "Automation Testing fresher", "IT Support fresher"
]
LOCATIONS = ["Hyderabad, India", "Bangalore, India", "Chennai, India"]
RESULTS_WANTED = 20   # Reduced for speed
HOURS_OLD = 72        # Last 3 days

# Strict fresher keywords (including 2024 batch)
FRESHER_KEYWORDS = [
    "fresher", "entry level", "graduate", "trainee", "junior", 
    "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"
]
SENIOR_KEYWORDS = [
    "senior", "lead", "principal", "architect", "manager", 
    "director", "head", "vp", "cto", "staff", "expert"
]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def is_fresher_job(title, description):
    if not isinstance(title, str): title = ""
    if not isinstance(description, str): description = ""
    text = (title + " " + description).lower()
    has_fresher = any(kw in text for kw in FRESHER_KEYWORDS)
    has_senior = any(kw in text for kw in SENIOR_KEYWORDS)
    return has_fresher and not has_senior

def clean_value(v):
    """Convert NaN, inf, None to None (JSON null) and ensure strings are valid."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if pd.isna(v):
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.isoformat() if hasattr(v, 'isoformat') else str(v)
    return v

def get_existing_urls():
    response = supabase.table("ApplyMore").select("url").execute()
    return {row["url"] for row in response.data} if response.data else set()

def batch_upsert_jobs(jobs, batch_size=50):
    if not jobs:
        return
    for i in range(0, len(jobs), batch_size):
        batch = jobs[i:i+batch_size]
        # Clean each job dict
        cleaned_batch = []
        for job in batch:
            cleaned = {k: clean_value(v) for k, v in job.items()}
            cleaned_batch.append(cleaned)
        supabase.table("ApplyMore").insert(cleaned_batch).execute()
        print(f"Inserted batch {i//batch_size + 1} ({len(cleaned_batch)} jobs)")

def scrape_all_jobs():
    all_new_jobs = []
    seen_urls = get_existing_urls()
    print(f"Existing jobs in DB: {len(seen_urls)}")
    
    for location in LOCATIONS:
        for role in ROLES:
            print(f"\n--- {role} in {location} ---")
            try:
                jobs_df = scrape_jobs(
                    site_name=["linkedin"],  # Only LinkedIn for speed (Indeed often returns none)
                    search_term=role,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD,
                    country_indeed='india',
                    verbose=0,  # Reduce log noise
                )
                if jobs_df.empty:
                    print("  No jobs found")
                    continue
                
                print(f"  Raw jobs: {len(jobs_df)}")
                for _, job in jobs_df.iterrows():
                    title = job.get('title')
                    company = job.get('company')
                    url = job.get('job_url')
                    desc = job.get('description')
                    posted = job.get('date_posted')
                    
                    # Basic validation
                    if not title or not company or not url:
                        continue
                    if url in seen_urls:
                        continue
                    if not is_fresher_job(title, desc):
                        continue
                    
                    # Handle posted date
                    if pd.isna(posted):
                        posted_iso = datetime.now(timezone.utc).isoformat()
                    elif isinstance(posted, (datetime, pd.Timestamp)):
                        posted_iso = posted.isoformat()
                    else:
                        posted_iso = str(posted)
                    
                    new_job = {
                        "title": title,
                        "company": company,
                        "location": location.split(',')[0],
                        "url": url,
                        "description": desc,
                        "posted_date": posted_iso,
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }
                    all_new_jobs.append(new_job)
                    seen_urls.add(url)
                
                print(f"  New jobs so far: {len(all_new_jobs)}")
                # Short delay to be polite (remove if you want faster, but may get blocked)
                time.sleep(random.uniform(1, 3))
                
            except Exception as e:
                print(f"  Error: {e}")
                continue
    
    print(f"\n=== Total new fresher jobs: {len(all_new_jobs)} ===")
    if all_new_jobs:
        batch_upsert_jobs(all_new_jobs)
    else:
        print("No new jobs to insert.")

if __name__ == "__main__":
    scrape_all_jobs()
