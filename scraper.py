import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timezone
from supabase import create_client
from jobspy import scrape_jobs
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ====================== ENVIRONMENT ======================
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# ====================== SUPABASE CLIENT ======================
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("✅ Supabase client initialized successfully")
except Exception as e:
    logger.error(f"❌ Supabase connection failed: {e}")
    raise

# ====================== CONFIG ======================
SITES = ["indeed", "linkedin", "naukri"]
CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India", "Pune, India", "Delhi, India", "Mumbai, India"]

RESULTS_WANTED = 12
HOURS_OLD = 120   # Last 5 days

def safe_str(v):
    return "" if pd.isna(v) or v is None else str(v)

def is_fresher(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    fresher_keywords = ["fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2"]
    senior_keywords = ["senior", "lead", "principal", "manager"]
    return any(k in text for k in fresher_keywords) and not any(k in text for k in senior_keywords)

def get_job_type(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    if any(k in text for k in ["software", "developer", "engineer", "python", "java", "react", "data analyst", "full stack"]):
        return "IT"
    return "Non-IT"

def is_remote(title, desc, location):
    text = (safe_str(title) + " " + safe_str(desc) + " " + safe_str(location)).lower()
    return any(k in text for k in ["remote", "wfh", "work from home", "hybrid"])

async def remove_expired_jobs():
    logger.info("🧹 Checking and removing expired jobs...")
    try:
        resp = supabase.table("ApplyMore").select("id, url").execute()
        if not resp.data:
            logger.info("No jobs to check")
            return
        deleted = 0
        async with aiohttp.ClientSession() as session:
            for job in resp.data:
                try:
                    async with session.head(job['url'], timeout=8, allow_redirects=True) as r:
                        if r.status >= 400:
                            supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                            deleted += 1
                except:
                    supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                    deleted += 1
                await asyncio.sleep(0.4)
        logger.info(f"🗑️ Removed {deleted} expired/unreachable jobs")
    except Exception as e:
        logger.error(f"Expired jobs cleanup error: {e}")

async def send_telegram(session, job, job_id):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    remote_tag = " 🌐 Remote" if job.get("is_remote") else ""
    emoji = "💻" if job.get("job_type") == "IT" else "📊"

    message = (
        f"{emoji} *{job['job_type']} Job{remote_tag}*\n"
        f"🏢 {job['company']}\n"
        f"📍 {job['location']}\n"
        f"🎓 {job['experience_level']}\n\n"
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
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

async def main():
    logger.info("🚀 ApplyMore Daily Job Scraper Started")

    await remove_expired_jobs()

    new_jobs = []
    # Get existing URLs
    try:
        existing = supabase.table("ApplyMore").select("url").execute()
        existing_urls = {row["url"] for row in existing.data} if existing.data else set()
    except:
        existing_urls = set()

    async with aiohttp.ClientSession() as session:
        for city in CITIES:
            for search_term in ["fresher", "entry level", "graduate trainee", "junior engineer", "fresher developer"]:
                try:
                    logger.info(f"Scraping '{search_term}' in {city}...")
                    df = scrape_jobs(
                        site_name=SITES,
                        search_term=search_term,
                        location=city,
                        results_wanted=RESULTS_WANTED,
                        hours_old=HOURS_OLD,
                        country_indeed='india',
                        verbose=0
                    )

                    if df.empty:
                        continue

                    logger.info(f"   Found {len(df)} raw jobs")

                    for _, row in df.iterrows():
                        url = safe_str(row.get('job_url'))
                        if not url or url in existing_urls:
                            continue

                        title = safe_str(row.get('title'))
                        if not is_fresher(title, safe_str(row.get('description'))):
                            continue

                        job = {
                            "title": title[:500],
                            "company": safe_str(row.get('company'))[:255],
                            "location": city.split(',')[0],
                            "url": url[:500],
                            "description": safe_str(row.get('description'))[:3000],
                            "posted_date": datetime.now(timezone.utc).isoformat(),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "experience_level": "Fresher / Entry Level",
                            "job_type": get_job_type(title, safe_str(row.get('description'))),
                            "is_remote": is_remote(title, safe_str(row.get('description')), city)
                        }

                        new_jobs.append(job)
                        existing_urls.add(url)

                except Exception as e:
                    logger.warning(f"Error scraping {search_term} in {city}: {e}")

    # Insert new jobs
    if new_jobs:
        logger.info(f"📥 Found {len(new_jobs)} new jobs. Inserting into database...")
        inserted_ids = []
        for i in range(0, len(new_jobs), 40):
            batch = new_jobs[i:i+40]
            try:
                res = supabase.table("ApplyMore").insert(batch).execute()
                if res.data:
                    inserted_ids.extend([r.get('id') for r in res.data])
            except Exception as e:
                logger.error(f"Insert error: {e}")

        # Send Telegram notifications
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID and inserted_ids:
            async with aiohttp.ClientSession() as tg_session:
                for idx, job_id in enumerate(inserted_ids):
                    if idx < len(new_jobs):
                        await send_telegram(tg_session, new_jobs[idx], job_id)
                        await asyncio.sleep(0.6)

        logger.info(f"🎉 SUCCESS: Added {len(inserted_ids)} new jobs!")
    else:
        logger.info("⚠️ No new fresher jobs found in this run.")

if __name__ == "__main__":
    asyncio.run(main())
