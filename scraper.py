import os
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client
import time

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Better search terms that actually return Indian fresher jobs
SEARCH_TERMS = [
    "fresher software engineer",
    "graduate engineer trainee",
    "entry level developer",
    "fresher data analyst",
    "trainee engineer"
]
CITIES = ["Hyderabad", "Bangalore", "Chennai"]
DAYS_BACK = 3

FRESHER_WORDS = ["fresher", "entry level", "graduate", "trainee", "junior", "0-2", "2024", "2025"]
SENIOR_WORDS = ["senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto"]

def is_fresher(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in FRESHER_WORDS) and not any(w in text for w in SENIOR_WORDS)

def get_existing_urls():
    resp = supabase.table("ApplyMore").select("url").execute()
    return {row["url"] for row in resp.data} if resp.data else set()

def fetch_jobs(query, city):
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} {city}",
        "page": 1,
        "num_pages": 1,
        "country": "in",
        "date_posted": "week"
    }
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    headers = {k: v.replace('\n', '').replace('\r', '').strip() for k, v in headers.items()}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        print(f"Error {resp.status_code} for {query} in {city}")
        return []
    data = resp.json()
    return data.get("data", [])

def main():
    print("Starting JSearch scraper with better queries...")
    existing = get_existing_urls()
    print(f"Existing URLs: {len(existing)}")
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
    new_jobs = []
    seen = set()

    for city in CITIES:
        for query in SEARCH_TERMS:
            print(f"Fetching '{query}' in {city}...")
            jobs = fetch_jobs(query, city)
            print(f"  Raw jobs: {len(jobs)}")
            for job in jobs:
                title = job.get("job_title")
                company = job.get("employer_name")
                url = job.get("job_apply_link")
                desc = job.get("job_description", "")
                posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
                if not title or not company or not url:
                    continue
                if url in existing or url in seen:
                    continue
                # Date filter
                try:
                    if posted_str:
                        posted = datetime.fromisoformat(posted_str.replace('Z', '+00:00'))
                        if posted < cutoff:
                            continue
                except:
                    pass
                if not is_fresher(title, desc):
                    continue
                new_jobs.append({
                    "title": title,
                    "company": company,
                    "location": city,
                    "url": url,
                    "description": desc,
                    "posted_date": posted_str or datetime.now(timezone.utc).isoformat(),
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
                seen.add(url)
            time.sleep(1)  # slight delay between queries

    print(f"New fresher jobs: {len(new_jobs)}")
    if new_jobs:
        for i in range(0, len(new_jobs), 50):
            supabase.table("ApplyMore").insert(new_jobs[i:i+50]).execute()
            print(f"Inserted batch {i//50 + 1}")
    else:
        print("No new jobs.")

if __name__ == "__main__":
    main()
