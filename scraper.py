import os
import asyncio
import aiohttp
import random
import re
import pandas as pd
from datetime import datetime, timezone, timedelta
from supabase import create_client
from jobspy import scrape_jobs

# ================= CONFIGURATION =================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# Reliable job boards (Google Jobs aggregates many sources)
SITES_TO_SCRAPE = ["linkedin", "indeed", "google"]

SEARCH_TERMS = [
    "fresher software engineer",
    "graduate engineer trainee",
    "entry level developer",
    "fresher data analyst",
    "trainee engineer",
    "fresher devops",
    "fresher cybersecurity",
    "fresher qa"
]
CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India"]
RESULTS_WANTED = 15          # per site per search term
HOURS_OLD = 72               # last 3 days

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior",
                 "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager",
                "director", "head", "vp", "cto", "staff"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ================= HELPER FUNCTIONS =================
def safe_str(value):
    """Convert any value to string, handling NaN, None, and non-string types."""
    if pd.isna(value):
        return ""
    if value is None:
        return ""
    return str(value)

def is_fresher_job(title, description):
    text = (safe_str(title) + " " + safe_str(description)).lower()
    has_fresher = any(w in text for w in FRESHER_WORDS)
    has_senior = any(w in text for w in SENIOR_WORDS)
    return has_fresher and not has_senior

def extract_experience_level(title, description):
    text = (safe_str(title) + " " + safe_str(description)).lower()
    if any(w in text for w in ["fresher", "entry level", "graduate", "trainee"]):
        return "Fresher (0-2 years)"
    match = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text)
    if match:
        return f"{match.group(1)}-{match.group(2)} years"
    return "Fresher (0-2 years)"

async def send_telegram_individual(session, job, job_id):
    """Send a detailed Telegram message per job with ApplyMore internal link."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    applymore_link = f"https://applymore.vercel.app/job.html?id={job_id}"
    desc_short = safe_str(job.get("description", ""))[:300]
    if len(desc_short) > 297:
        desc_short += "..."
    message = (
        f"🚀 *New Job: {safe_str(job.get('title'))}*\n\n"
        f"🏢 *Company:* {safe_str(job.get('company'))}\n"
        f"📍 *Location:* {safe_str(job.get('location'))}\n"
        f"🎓 *Experience:* {safe_str(job.get('experience_level'))}\n\n"
        f"📝 *Description:*\n{desc_short}\n\n"
        f"🔗 *Apply here:* {applymore_link}\n\n"
        f"⚠️ *APPLY ASAP!*"
    )
    try:
        await session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        })
        return True
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

# ================= MAIN SCRAPING ROUTINE =================
async def main():
    print("🚀 ApplyMore – Reliable Scraper (LinkedIn, Indeed, Google Jobs)")
    start_time = datetime.now(timezone.utc)

    # Fetch existing URLs to avoid duplicates
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    print(f"Existing jobs in DB: {len(existing_urls)}")

    new_jobs_raw = []
    seen_urls = set()

    for location in CITIES:
        for term in SEARCH_TERMS:
            print(f"\n🔍 Scraping '{term}' in {location}")
            try:
                jobs_df = scrape_jobs(
                    site_name=SITES_TO_SCRAPE,
                    search_term=term,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD,
                    country_indeed='india',
                    verbose=0
                )
                print(f"  Raw jobs fetched: {len(jobs_df)}")
                if jobs_df.empty:
                    continue

                for _, job in jobs_df.iterrows():
                    title = safe_str(job.get('title'))
                    company = safe_str(job.get('company'))
                    url = safe_str(job.get('job_url'))
                    description = safe_str(job.get('description'))
                    posted_str = job.get('date_posted')

                    if not title or not company or not url:
                        continue
                    if url in existing_urls or url in seen_urls:
                        continue
                    if not is_fresher_job(title, description):
                        continue

                    # Normalise posted date
                    if pd.isna(posted_str):
                        posted_iso = datetime.now(timezone.utc).isoformat()
                    elif isinstance(posted_str, datetime):
                        posted_iso = posted_str.isoformat()
                    else:
                        posted_iso = safe_str(posted_str)

                    exp_level = extract_experience_level(title, description)
                    new_jobs_raw.append({
                        "title": title,
                        "company": company,
                        "location": location.split(',')[0],
                        "url": url,
                        "description": description,
                        "posted_date": posted_iso,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "experience_level": exp_level
                    })
                    seen_urls.add(url)

                # Polite delay between searches
                await asyncio.sleep(random.uniform(3, 6))
            except Exception as e:
                print(f"  ⚠️ Error for '{term}' in {location}: {e}")
                continue

    print(f"\n📋 New fresher jobs found (before insert): {len(new_jobs_raw)}")
    if not new_jobs_raw:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            await session.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": "⚠️ ApplyMore scraper ran but found no new fresher jobs.",
                "parse_mode": "Markdown"
            })
        return

    # Insert in batches and capture returned IDs
    inserted_ids = []
    for i in range(0, len(new_jobs_raw), 50):
        batch = new_jobs_raw[i:i+50]
        result = supabase.table("ApplyMore").insert(batch).execute()
        if result.data:
            inserted_ids.extend([row['id'] for row in result.data])
        print(f"✅ Inserted batch {i//50+1} ({len(batch)} jobs)")

    print(f"Total inserted jobs with IDs: {len(inserted_ids)}")

    # Send individual Telegram alerts with ApplyMore links
    async with aiohttp.ClientSession() as session:
        for idx, job_id in enumerate(inserted_ids):
            job_data = new_jobs_raw[idx]
            await send_telegram_individual(session, job_data, job_id)
            await asyncio.sleep(1)  # avoid flooding

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\n✅ Scraping finished in {elapsed:.2f}s. Inserted {len(inserted_ids)} jobs.")

if __name__ == "__main__":
    asyncio.run(main())
