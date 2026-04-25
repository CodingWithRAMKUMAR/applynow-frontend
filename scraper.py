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
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

SEARCH_TERMS = [
    "fresher software engineer", "graduate engineer trainee", "entry level developer",
    "fresher data analyst", "trainee engineer", "fresher devops",
    "fresher cybersecurity", "fresher qa"
]
# Note: Naukri.com primarily works with Indian cities.
# The city name is used in the search term, but the location parameter might be ignored.
CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India"]
RESULTS_WANTED = 20          # per site per search
HOURS_OLD = 72               # last 3 days

# COMPLETE JobBoards List (Updated for Naukri)
SITES_TO_SCRAPE = ["linkedin", "indeed", "glassdoor", "naukri"]

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior",
                 "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager",
                "director", "head", "vp", "cto", "staff"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def is_fresher_job(title, description):
    text = (title + " " + description).lower()
    return any(w in text for w in FRESHER_WORDS) and not any(w in text for w in SENIOR_WORDS)

def extract_experience_level(title, description):
    text = (title + " " + description).lower()
    if any(w in text for w in ["fresher", "entry level", "graduate", "trainee"]):
        return "Fresher (0-2 years)"
    match = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text)
    if match:
        return f"{match.group(1)}-{match.group(2)} years"
    return "Fresher (0-2 years)"

async def send_telegram(session, text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        await session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        })
    except Exception as e:
        print(f"Telegram error: {e}")

async def main():
    print("🚀 Unlimited direct scraper started (Updated with Naukri)")
    start_time = datetime.now(timezone.utc)

    # Fetch existing URLs to avoid duplicates
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    print(f"Existing jobs: {len(existing_urls)}")

    new_jobs_raw = []   # will store dicts before insertion
    seen_urls = set()

    # Scrape sequentially to be polite
    for location in CITIES:
        for term in SEARCH_TERMS:
            print(f"Scraping {term} in {location}...")
            try:
                jobs_df = scrape_jobs(
                    site_name=SITES_TO_SCRAPE,
                    search_term=term,
                    location=location,         # used by Indeed/LinkedIn/Glassdoor
                    results_wanted=RESULTS_WANTED,
                    hours_old=HOURS_OLD,
                    country_indeed='india',
                    verbose=0                  # reduce log noise
                )
                print(f"  Raw jobs found: {len(jobs_df)}")
                if jobs_df.empty:
                    continue

                for _, job in jobs_df.iterrows():
                    title = job.get('title')
                    company = job.get('company')
                    url = job.get('job_url')
                    desc = job.get('description')
                    posted_str = job.get('date_posted')
                    # job_source = job.get('site') # ['linkedin', 'indeed', 'glassdoor', 'naukri']

                    if not title or not company or not url:
                        continue
                    if url in existing_urls or url in seen_urls:
                        continue
                    if not is_fresher_job(title, desc):
                        continue

                    # Normalise date
                    if pd.isna(posted_str):
                        posted_iso = datetime.now(timezone.utc).isoformat()
                    elif isinstance(posted_str, datetime):
                        posted_iso = posted_str.isoformat()
                    else:
                        posted_iso = str(posted_str)

                    exp_level = extract_experience_level(title, desc)
                    new_jobs_raw.append({
                        "title": title,
                        "company": company,
                        "location": location.split(',')[0],
                        "url": url,
                        "description": str(desc) if desc else "",
                        "posted_date": posted_iso,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "experience_level": exp_level
                    })
                    seen_urls.add(url)

                # Delay to avoid being blocked
                await asyncio.sleep(random.uniform(3, 6))
            except Exception as e:
                print(f"  Error scraping {term} in {location}: {e}")
                continue

    print(f"New fresher jobs (before insert): {len(new_jobs_raw)}")
    if not new_jobs_raw:
        async with aiohttp.ClientSession() as session:
            await send_telegram(session, "⚠️ ApplyMore scraper ran but found no new fresher jobs.")
        return

    # Insert in batches and capture returned IDs
    inserted_ids = []
    for i in range(0, len(new_jobs_raw), 50):
        batch = new_jobs_raw[i:i+50]
        result = supabase.table("ApplyMore").insert(batch).execute()
        if result.data:
            inserted_ids.extend([row['id'] for row in result.data])
        print(f"Inserted batch {i//50+1} ({len(batch)} jobs)")

    print(f"Total inserted jobs with IDs: {len(inserted_ids)}")

    # Prepare Telegram alerts – one message per job (to avoid truncation)
    # But to prevent spam, we send a single message with the first 10 jobs + link to site
    async with aiohttp.ClientSession() as session:
        if inserted_ids:
            # Map inserted_ids back to the raw job data (same order)
            # Since we inserted in the same order, we can zip
            # However, we must ensure we have the same number
            jobs_with_ids = list(zip(inserted_ids, new_jobs_raw[:len(inserted_ids)]))
            lines = [f"✅ <b>ApplyMore – {len(jobs_with_ids)} new fresher jobs</b>\n"]
            for idx, (jid, job) in enumerate(jobs_with_ids[:10], 1):
                link = f"https://applymore.vercel.app/job.html?id={jid}"
                lines.append(
                    f"{idx}. <b>{job['title']}</b>\n"
                    f"   🏢 {job['company']} | 📍 {job['location']}\n"
                    f"   🔗 <a href='{link}'>Apply on ApplyMore</a>\n"
                    f"   ⚠️ <b>APPLY ASAP</b>"
                )
            if len(jobs_with_ids) > 10:
                lines.append(f"\n... and {len(jobs_with_ids)-10} more. <a href='https://applymore.vercel.app'>Browse all jobs</a>")
            else:
                lines.append(f"\n🌐 <a href='https://applymore.vercel.app'>Visit ApplyMore</a>")
            await send_telegram(session, "\n\n".join(lines))
        else:
            await send_telegram(session, "⚠️ Jobs were found but could not retrieve IDs.")

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"Finished in {elapsed:.2f}s. Inserted {len(inserted_ids)} jobs.")

if __name__ == "__main__":
    asyncio.run(main())
