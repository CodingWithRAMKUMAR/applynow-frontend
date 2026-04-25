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
SITES = ["linkedin", "indeed", "glassdoor", "naukri"]
RESUME_LINKS = ["linkedin", "indeed"]
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
RESULTS_WANTED = 15
HOURS_OLD = 72
SENIOR_KEYWORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto", "staff"}
FRESHER_KEYWORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ================= HELPER FUNCTIONS =================
def is_fresher_job(title, description):
    """Return True if the job is for a fresher/entry-level candidate."""
    text = (title + " " + description).lower()
    return any(k in text for k in FRESHER_KEYWORDS) and not any(k in text for k in SENIOR_KEYWORDS)

def extract_experience_level(title, description):
    """Extract experience level from the job title or description."""
    text = (title + " " + description).lower()
    if any(k in text for k in ["fresher", "entry level", "graduate", "trainee"]):
        return "Fresher (0-2 years)"
    match = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text)
    if match:
        return f"{match.group(1)}-{match.group(2)} years"
    return "Fresher (0-2 years)"

async def send_individual_telegram_alert(session, job):
    """Sends a separate, detailed Telegram message for each job."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    company_display = job.get('company', 'N/A')
    location_display = job.get('location', 'N/A')
    experience_display = job.get('experience_level', 'N/A')
    description_clean = job.get('description').replace('\n', ' ').strip() if job.get('description') else "No description provided."
    short_description = (description_clean[:250] + '...') if len(description_clean) > 250 else description_clean
    message = (
        f"🚀 *New Job Alert: {job['title']}*\n\n"
        f"🏢 *Company:* {company_display}\n"
        f"📍 *Location:* {location_display}\n"
        f"🎓 *Experience:* {experience_display}\n\n"
        f"📝 *Description:*\n{short_description}\n\n"
        f"🔗 *How to Apply:* This is a direct listing from the original job board. "
        f"Please visit our website to see the original posting and a detailed guide on how to apply.\n\n"
        f"🌐 *Visit ApplyMore:* https://applymore.vercel.app\n"
        f"⚠️ *APPLY ASAP!*"
    )
    try:
        await session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"})
        return True
    except Exception as e:
        print(f"Telegram error for {job['title']}: {e}")
        return False

# ================= MAIN SCRAPING ROUTINE =================
async def main():
    print("🚀 ApplyMore – Enhanced Adaptive Scraper Started")
    start_time = datetime.now(timezone.utc)
    # Fetch existing URLs to avoid duplicates
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    print(f"Existing jobs in DB: {len(existing_urls)}")
    new_jobs_raw = []
    seen_urls = set()
    for location in CITIES:
        for term in SEARCH_TERMS:
            print(f"\n🔍 Scraping: '{term}' in {location}")
            try:
                jobs_df = scrape_jobs(
                    site_name=SITES,
                    search_term=term,
                    location=location,
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD,
                    country_indeed='india',
                    verbose=0
                )
                print(f"  ✅ {location}: {len(jobs_df)} raw jobs fetched.")
                if jobs_df.empty:
                    continue
                for _, job in jobs_df.iterrows():
                    title = job.get('title')
                    company = job.get('company')
                    url = job.get('job_url')
                    desc = job.get('description')
                    posted_str = job.get('date_posted')
                    job_source = job.get('site')
                    if not title or not company or not url:
                        continue
                    if url in existing_urls or url in seen_urls:
                        continue
                    if not is_fresher_job(title, desc):
                        continue
                    if pd.isna(posted_str):
                        posted_iso = datetime.now(timezone.utc).isoformat()
                    elif isinstance(posted_str, datetime):
                        posted_iso = posted_str.isoformat()
                    else:
                        posted_iso = str(posted_str)
                    exp_level = extract_experience_level(title, desc)
                    is_ats_job_source = job_source not in RESUME_LINKS
                    new_jobs_raw.append({
                        "title": title,
                        "company": company,
                        "location": location.split(',')[0],
                        "url": url,
                        "description": str(desc) if desc else "",
                        "posted_date": posted_iso,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "experience_level": exp_level,
                        "is_ats": is_ats_job_source
                    })
                    seen_urls.add(url)
                await asyncio.sleep(random.uniform(4, 8))  # Polite delay between searches
            except Exception as e:
                print(f"  ⚠️ An error occurred while scraping '{term}' in {location}: {e}. Moving to next...")
                continue
    print(f"\n📋 Total potential new jobs after filtering: {len(new_jobs_raw)}")
    if not new_jobs_raw:
        print("ℹ️ No new jobs found.")
        return
    # Batch insert and get IDs
    inserted_ids_for_telegram = []
    for i in range(0, len(new_jobs_raw), 50):
        batch = new_jobs_raw[i:i+50]
        result = supabase.table("ApplyMore").insert(batch).execute()
        if result.data:
            inserted_ids_for_telegram.extend([row['id'] for row in result.data])
        print(f"Inserted batch {i//50+1} ({len(batch)} jobs).")
    print(f"Total inserted jobs: {len(inserted_ids_for_telegram)}")
    if not inserted_ids_for_telegram:
        print("No jobs to alert.")
        return
    # Send individual Telegram alerts
    async with aiohttp.ClientSession() as session:
        for i, job_id in enumerate(inserted_ids_for_telegram):
            job_data = new_jobs_raw[i]
            success = await send_individual_telegram_alert(session, job_data)
            if success:
                await asyncio.sleep(1)  # Throttle alerts
            else:
                print(f"Failed to send alert for {job_data['title']}. Skipping...")
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"\n✅ Scraping and alerting finished in {elapsed:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())
