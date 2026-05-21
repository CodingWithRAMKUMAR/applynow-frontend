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

# ========== ENVIRONMENT VARIABLES ==========
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# ========== DEBUG ENVIRONMENT ==========
logger.info("=" * 80)
logger.info("🔍 ENVIRONMENT DEBUG")
logger.info(f"SUPABASE_URL          : {'✅ Loaded' if SUPABASE_URL else '❌ Missing'} | Length: {len(SUPABASE_URL)}")
logger.info(f"SERVICE_ROLE_KEY      : {'✅ Loaded' if SUPABASE_SERVICE_ROLE_KEY else '❌ Missing'} | Length: {len(SUPABASE_SERVICE_ROLE_KEY)}")
if SUPABASE_SERVICE_ROLE_KEY:
    logger.info(f"Key Preview           : {SUPABASE_SERVICE_ROLE_KEY[:40]}...")
logger.info(f"TELEGRAM_TOKEN        : {'✅ Loaded' if TELEGRAM_TOKEN else '❌ Missing'}")
logger.info(f"TELEGRAM_CHAT_ID      : {'✅ Loaded' if TELEGRAM_CHAT_ID else '❌ Missing'}")
logger.info("=" * 80)

# ========== SUPABASE CLIENT ==========
supabase = None
try:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise ValueError("Missing Supabase URL or Service Role Key in environment variables")

    logger.info("🔄 Creating Supabase client...")
    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    logger.info("✅ Supabase client created successfully")

    # Test connection
    test = supabase.table("ApplyMore").select("id").limit(1).execute()
    logger.info("✅ Supabase connection test passed")

except Exception as e:
    logger.error(f"❌ CRITICAL: Supabase initialization failed -> {e}")
    logger.error("Please verify you are using the **service_role / secret key** (not publishable key)")
    raise

# ========== CONFIGURATION ==========
SITES = ["linkedin", "indeed", "naukri", "monster", "glassdoor"]

IT_SEARCH_TERMS = [
    "fresher software engineer", "graduate engineer trainee", "entry level developer",
    "fresher data analyst", "trainee engineer", "junior developer",
    "associate software engineer", "entry level programmer", "fresher python developer",
    "trainee data scientist", "junior web developer", "fresher java developer"
]

NON_IT_SEARCH_TERMS = [
    "fresher accountant", "entry level accountant", "trainee accountant",
    "fresher hr executive", "entry level hr", "trainee hr",
    "fresher sales executive", "entry level sales", "trainee sales",
    "fresher marketing", "entry level marketing", "trainee marketing",
    "fresher customer support", "entry level customer support", "trainee customer support",
    "fresher data entry", "entry level data entry", "trainee data entry",
    "fresher finance", "entry level finance", "trainee finance",
    "fresher operations", "entry level operations", "trainee operations",
    "fresher content writer", "entry level content writer", "trainee content writer",
    "fresher social media", "entry level social media", "trainee social media",
    "fresher designer", "entry level designer", "trainee designer",
    "fresher digital marketing", "entry level digital marketing", "trainee digital marketing",
    "fresher project coordinator", "entry level project coordinator", "trainee project coordinator"
]

CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India", "Mumbai, India", "Delhi, India", "Pune, India"]

RESULTS_WANTED = 6
HOURS_OLD = 72

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-2", "2024", "2025", "2026", "0-1"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "chief"}
REMOTE_WORDS = {"remote", "work from home", "wfh", "hybrid", "telecommute"}

def safe_str(v):
    if pd.isna(v) or v is None:
        return ""
    return str(v)

def is_fresher(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in FRESHER_WORDS) and not any(k in text for k in SENIOR_WORDS)

def is_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in ["software", "developer", "engineer", "data", "python", "java", "react", "javascript"])

def is_non_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in ["accountant", "hr", "sales", "marketing", "customer support", "data entry"])

def is_remote_job(title, desc, location):
    text = (safe_str(title) + " " + safe_str(desc) + " " + safe_str(location)).lower()
    return any(k in text for k in REMOTE_WORDS)

def extract_exp(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    if any(k in text for k in SENIOR_WORDS):
        return "Senior (3+ years)"
    if any(k in text for k in FRESHER_WORDS):
        return "Fresher (0-2 years)"
    return "Entry Level"

def extract_skills(desc):
    if not desc:
        return []
    text = desc.lower()
    skills = [s for s in ["python", "java", "react", "sql", "aws", "javascript", "django"] if s in text]
    return list(set(skills))[:5]

def format_posted_date(posted):
    if not posted:
        return "Recently"
    try:
        if isinstance(posted, datetime):
            diff = (datetime.now(timezone.utc) - posted).days
        else:
            posted_dt = datetime.fromisoformat(safe_str(posted).replace('Z', '+00:00'))
            diff = (datetime.now(timezone.utc) - posted_dt).days
        if diff == 0: return "Today"
        elif diff == 1: return "Yesterday"
        else: return f"{diff} days ago"
    except:
        return "Recently"

async def send_telegram(session, job, job_id, job_type):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    link = f"https://applymore.vercel.app/job.html?id={job_id}"
    title = safe_str(job.get('title'))
    company = safe_str(job.get('company'))
    location = safe_str(job.get('location'))
    desc = safe_str(job.get("description", ""))[:200].replace('\n', ' ')
    if len(desc) > 197:
        desc += "..."

    message = (
        f"{'💻' if job_type == 'IT' else '📊'} *{job_type} Job*: {title}\n"
        f"🏢 {company} | 📍 {location}\n"
        f"🔗 [Apply Here]({link})"
    )

    try:
        await session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
        )
        logger.info(f"✅ Telegram sent: {title}")
    except Exception as e:
        logger.error(f"Telegram failed: {e}")

# ==================== MAIN FUNCTIONS ====================

async def check_links_activity():
    logger.info("🔍 Checking expired job links...")
    try:
        resp = supabase.table("ApplyMore").select("id, url").execute()
        if not resp.data:
            return 0

        deleted = 0
        async with aiohttp.ClientSession() as session:
            for job in resp.data:
                try:
                    async with session.head(job['url'], timeout=6, allow_redirects=True) as r:
                        if r.status >= 400:
                            supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                            deleted += 1
                except:
                    supabase.table("ApplyMore").delete().eq("id", job['id']).execute()
                    deleted += 1
                await asyncio.sleep(0.3)
        logger.info(f"🗑️ Deleted {deleted} expired jobs")
        return deleted
    except Exception as e:
        logger.error(f"Link check error: {e}")
        return 0

async def scrape_city(session, city, is_it=True):
    new_jobs = []
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    seen = set()

    search_terms = IT_SEARCH_TERMS if is_it else NON_IT_SEARCH_TERMS

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

            for _, job in df.iterrows():
                title = safe_str(job.get('title'))
                company = safe_str(job.get('company'))
                url = safe_str(job.get('job_url'))
                desc = safe_str(job.get('description'))

                if not title or not company or not url or url in existing_urls or url in seen:
                    continue
                if not is_fresher(title, desc):
                    continue
                if (is_it and not is_it_job(title, desc)) or (not is_it and not is_non_it_job(title, desc)):
                    continue

                new_jobs.append({
                    "title": title[:500],
                    "company": company[:255],
                    "location": city.split(',')[0],
                    "url": url[:500],
                    "description": desc[:3000],
                    "posted_date": datetime.now(timezone.utc).isoformat(),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "experience_level": extract_exp(title, desc),
                    "job_type": "IT" if is_it else "Non-IT",
                    "is_remote": is_remote_job(title, desc, city)
                })
                seen.add(url)
        except Exception as e:
            logger.error(f"Error in {city} - {term}: {e}")

    return new_jobs

async def main():
    logger.info("🚀 ApplyMore Job Scraper Started")
    start = datetime.now(timezone.utc)

    await check_links_activity()

    # Scrape Jobs
    async with aiohttp.ClientSession() as session:
        it_tasks = [scrape_city(session, city, True) for city in CITIES]
        it_results = await asyncio.gather(*it_tasks)
        it_jobs = [job for sublist in it_results for job in sublist]

        non_it_tasks = [scrape_city(session, city, False) for city in CITIES]
        non_it_results = await asyncio.gather(*non_it_tasks)
        non_it_jobs = [job for sublist in non_it_results for job in sublist]

    all_jobs = it_jobs + non_it_jobs
    logger.info(f"📦 Total new jobs: {len(all_jobs)} (IT: {len(it_jobs)}, Non-IT: {len(non_it_jobs)})")

    if not all_jobs:
        logger.info("No new jobs found.")
        return

    # Insert to Supabase
    inserted = 0
    for i in range(0, len(all_jobs), 40):
        batch = all_jobs[i:i+40]
        try:
            res = supabase.table("ApplyMore").insert(batch).execute()
            inserted += len(res.data) if res.data else 0
            logger.info(f"✅ Inserted batch {i//40 + 1}")
        except Exception as e:
            logger.error(f"Insert error: {e}")

    logger.info(f"🎉 Scraper finished successfully! Inserted {inserted} jobs.")

if __name__ == "__main__":
    asyncio.run(main())
