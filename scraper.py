import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone
from supabase import create_client
from jobspy import scrape_jobs
import logging

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== ENVIRONMENT ==========
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

logger.info("=" * 80)
logger.info("🔍 ENVIRONMENT CHECK")
logger.info(f"SUPABASE_URL     : {'✅' if SUPABASE_URL else '❌'} | Len: {len(SUPABASE_URL)}")
logger.info(f"SUPABASE_KEY     : {'✅' if SUPABASE_KEY else '❌'} | Len: {len(SUPABASE_KEY)}")
logger.info("=" * 80)

# ========== SUPABASE CLIENT ==========
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Supabase client created successfully")
except Exception as e:
    logger.error(f"❌ Supabase failed: {e}")
    raise

# ========== CONFIG ==========
SITES = ["linkedin", "indeed", "naukri", "monster", "glassdoor"]
CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India", "Mumbai, India", "Delhi, India", "Pune, India"]

RESULTS_WANTED = 8
HOURS_OLD = 72

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-2", "2024", "2025", "2026"}
SENIOR_WORDS = {"senior", "lead", "principal", "manager", "director", "head"}
REMOTE_WORDS = {"remote", "work from home", "wfh", "hybrid", "telecommute"}

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

def is_fresher(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(w in text for w in FRESHER_WORDS) and not any(w in text for w in SENIOR_WORDS)

def is_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in ["software", "developer", "engineer", "python", "java", "react", "data analyst", "full stack"])

def is_non_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in ["accountant", "hr", "sales", "marketing", "customer support", "data entry", "finance"])

def is_remote(title, desc, location):
    text = (safe_str(title) + " " + safe_str(desc) + " " + safe_str(location)).lower()
    return any(w in text for w in REMOTE_WORDS)

def extract_exp(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    if any(w in text for w in SENIOR_WORDS):
        return "Senior"
    if any(w in text for w in FRESHER_WORDS):
        return "Fresher"
    return "Entry Level"

async def remove_expired_jobs():
    """Remove jobs whose URLs are no longer active"""
    logger.info("🧹 Checking for expired jobs...")
    try:
        resp = supabase.table("ApplyMore").select("id, url").execute()
        if not resp.data:
            return

        deleted = 0
        async with aiohttp.ClientSession() as session:
            for job in resp.data:
                try:
                    async with session.head(job['url'], timeout=7, allow_redirects=True) as r:
                        if r.status >= 400:
                            supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                            deleted += 1
                except:
                    supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                    deleted += 1
                await asyncio.sleep(0.4)
        logger.info(f"🗑️ Removed {deleted} expired jobs")
    except Exception as e:
        logger.error(f"Expired job check failed: {e}")

async def send_telegram(session, job, job_id):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    job_type = job.get("job_type", "Job")
    remote_tag = " 🌐 Remote" if job.get("is_remote") else ""
    emoji = "💻" if job_type == "IT" else "📊"

    message = (
        f"{emoji} *{job_type} Job{remote_tag}*\n"
        f"🏢 {job['company']}\n"
        f"📍 {job['location']}\n"
        f"🎓 {job['experience_level']}\n"
        f"📅 Posted: {job.get('posted_date', 'Recent')}\n\n"
        f"🔗 [Apply Now](https://applymore.vercel.app/job.html?id={job_id})\n"
        f"⚡ Apply Fast!"
    )

    try:
        await session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
        )
        logger.info(f"📨 Telegram sent: {job['title'][:50]}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

async def main():
    logger.info("🚀 ApplyMore Job Scraper Started")

    await remove_expired_jobs()

    all_new_jobs = []
    existing_urls = set()

    try:
        resp = supabase.table("ApplyMore").select("url").execute()
        existing_urls = {row["url"] for row in resp.data} if resp.data else set()
    except:
        pass

    async with aiohttp.ClientSession() as session:
        for city in CITIES:
            for is_it in [True, False]:
                job_type = "IT" if is_it else "Non-IT"
                logger.info(f"🔍 Scraping {job_type} jobs in {city}...")

                search_terms = [
                    "fresher software engineer", "entry level developer", "graduate engineer trainee",
                    "fresher data analyst", "junior developer"
                ] if is_it else [
                    "fresher accountant", "entry level hr", "fresher sales", "fresher marketing"
                ]

                for term in search_terms:
                    try:
                        df = scrape_jobs(
                            site_name=SITES,
                            search_term=term,
                            location=city,
                            results_wanted=RESULTS_WANTED,
                            hours_old=HOURS_OLD,
                            country_indeed='india',
                            verbose=0
                        )

                        if df.empty:
                            continue

                        for _, row in df.iterrows():
                            title = safe_str(row.get('title'))
                            url = safe_str(row.get('job_url'))

                            if not title or not url or url in existing_urls:
                                continue

                            desc = safe_str(row.get('description'))
                            if not is_fresher(title, desc):
                                continue

                            if is_it and not is_it_job(title, desc):
                                continue
                            if not is_it and not is_non_it_job(title, desc):
                                continue

                            job_data = {
                                "title": title[:500],
                                "company": safe_str(row.get('company'))[:255],
                                "location": city.split(',')[0],
                                "url": url[:500],
                                "description": desc[:3000],
                                "posted_date": datetime.now(timezone.utc).isoformat(),
                                "created_at": datetime.now(timezone.utc).isoformat(),
                                "experience_level": extract_exp(title, desc),
                                "job_type": job_type,
                                "is_remote": is_remote(title, desc, city)
                            }

                            all_new_jobs.append(job_data)
                            existing_urls.add(url)

                    except Exception as e:
                        logger.error(f"Error scraping {term} in {city}: {e}")

    # Insert new jobs
    if all_new_jobs:
        logger.info(f"📦 Found {len(all_new_jobs)} new jobs. Inserting...")

        inserted_ids = []
        for i in range(0, len(all_new_jobs), 40):
            batch = all_new_jobs[i:i+40]
            try:
                res = supabase.table("ApplyMore").insert(batch).execute()
                if res.data:
                    inserted_ids.extend([r['id'] for r in res.data])
            except Exception as e:
                logger.error(f"Insert batch error: {e}")

        # Send Telegram notifications
        async with aiohttp.ClientSession() as tg_session:
            for idx, job_id in enumerate(inserted_ids):
                if idx < len(all_new_jobs):
                    await send_telegram(tg_session, all_new_jobs[idx], job_id)
                    await asyncio.sleep(0.5)

        logger.info(f"🎉 Successfully inserted {len(inserted_ids)} new jobs!")
    else:
        logger.info("✅ No new jobs found.")

if __name__ == "__main__":
    asyncio.run(main())
