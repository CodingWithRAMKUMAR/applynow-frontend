import os
import asyncio
import aiohttp
import re
import pandas as pd
from datetime import datetime, timezone, timedelta
from supabase import create_client
from jobspy import scrape_jobs
import logging
from urllib.parse import urlparse

# ========== LOGGING ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== ENVIRONMENT ==========
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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

IT_KEYWORDS = {
    "software", "developer", "programmer", "engineer", "data", "analyst", "cloud",
    "python", "java", "javascript", "react", "angular", "aws", "azure", "docker",
    "kubernetes", "sql", "excel", "tableau", "power bi", "git", "selenium",
    "django", "flask", "node.js", "typescript", "mongodb", "postgresql", "mysql",
    "linux", "rest", "graphql", "tensorflow", "pytorch", "scikit-learn", "jenkins",
    "ansible", "terraform", "devops", "ai", "ml", "machine learning", "deep learning"
}
NON_IT_KEYWORDS = {
    "accountant", "hr", "sales", "marketing", "customer support", "data entry",
    "finance", "operations", "content writer", "social media", "designer",
    "digital marketing", "project coordinator", "administrative", "receptionist",
    "office assistant", "executive assistant", "client services", "accounts",
    "bookkeeping", "recruiter", "talent acquisition", "business development",
    "account manager", "customer service", "technical support", "quality analyst",
    "process associate", "verification", "logistics", "supply chain", "procurement",
    "audit", "tax", "billing", "payroll", "training", "development", "learning",
    "employee relations", "compensation", "benefits"
}
REMOTE_WORDS = {"remote", "work from home", "wfh", "hybrid", "telecommute", "virtual", "anywhere"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def safe_str(v):
    if pd.isna(v) or v is None:
        return ""
    return str(v)

def is_fresher(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in FRESHER_WORDS) and not any(k in text for k in SENIOR_WORDS)

def is_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in IT_KEYWORDS)

def is_non_it_job(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in NON_IT_KEYWORDS)

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
    found = [skill for skill in IT_KEYWORDS if skill in text]
    return list(set(found))[:5]

def format_posted_date(posted):
    if not posted:
        return "Recently"
    try:
        if isinstance(posted, datetime):
            diff = (datetime.now(timezone.utc) - posted).days
        else:
            posted_dt = datetime.fromisoformat(safe_str(posted).replace('Z', '+00:00'))
            diff = (datetime.now(timezone.utc) - posted_dt).days
        if diff == 0:
            return "Today"
        elif diff == 1:
            return "Yesterday"
        else:
            return f"{diff} days ago"
    except:
        return "Recently"

async def send_telegram(session, job, job_id, job_type):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    link = f"https://applymore.vercel.app/job.html?id={job_id}"
    title = safe_str(job['title'])
    company = safe_str(job['company'])
    location = safe_str(job['location'])
    desc = safe_str(job.get("description", ""))[:200].replace('\n', ' ')
    if len(desc) > 197:
        desc += "..."
    posted_str = format_posted_date(job.get("posted_date"))
    skills = extract_skills(job.get("description", ""))
    skills_text = ", ".join(skills) if skills else "Not listed"
    exp_level = extract_exp(title, job.get("description", ""))
    remote = is_remote_job(title, job.get("description", ""), location)
    remote_tag = " 🌐 Remote" if remote else ""
    
    emoji = "💻" if job_type == "IT" else "📊"
    
    message = (
        f"{emoji} *{job_type} Job{remote_tag}: {title}*\n"
        f"🏢 {company} | 📍 {location}\n"
        f"📅 Posted: {posted_str}\n"
        f"🎓 Experience: {exp_level}\n"
        f"🔧 Skills: {skills_text}\n\n"
        f"📝 *Description:*\n{desc}\n\n"
        f"🔗 [Apply on ApplyMore]({link})\n"
        f"⚡ *APPLY ASAP*"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        await session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        })
        logger.info(f"Telegram sent: {title}")
    except Exception as e:
        logger.error(f"Telegram error: {e}")

async def check_links_activity():
    """Check if existing job links are still active and delete expired ones"""
    logger.info("🔍 Checking for expired job links...")
    try:
        resp = supabase.table("ApplyMore").select("id, url").execute()
        if not resp.data:
            logger.info("No jobs to check")
            return 0
        
        deleted = 0
        active = 0
        
        async with aiohttp.ClientSession() as session:
            for job in resp.data:
                job_id = job['id']
                url = job['url']
                
                if not url:
                    continue
                
                try:
                    async with session.head(url, timeout=5, allow_redirects=True) as response:
                        if response.status >= 400:
                            supabase.table("ApplyMore").delete().eq("id", job_id).execute()
                            deleted += 1
                            logger.info(f"🗑️ Deleted expired job: {url}")
                        else:
                            active += 1
                except:
                    supabase.table("ApplyMore").delete().eq("id", job_id).execute()
                    deleted += 1
                    logger.info(f"🗑️ Deleted unreachable job: {url}")
                
                await asyncio.sleep(0.2)
        
        logger.info(f"✅ Link check: {active} active, {deleted} deleted")
        return deleted
    except Exception as e:
        logger.error(f"Error checking links: {e}")
        return 0

async def scrape_city(session, city, is_it=True):
    """Scrape jobs for a specific city"""
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
                posted = job.get('date_posted')
                
                if not title or not company or not url:
                    continue
                if url in existing_urls or url in seen:
                    continue
                if not is_fresher(title, desc):
                    continue
                
                # Validate job type
                if is_it and not is_it_job(title, desc):
                    continue
                if not is_it and not is_non_it_job(title, desc):
                    continue
                
                posted_iso = datetime.now(timezone.utc).isoformat()
                if posted is not None and not pd.isna(posted):
                    try:
                        if isinstance(posted, datetime):
                            posted_iso = posted.isoformat()
                        else:
                            posted_iso = datetime.fromisoformat(safe_str(posted).replace('Z', '+00:00')).isoformat()
                    except:
                        pass
                
                new_jobs.append({
                    "title": title[:500],
                    "company": company[:255],
                    "location": city.split(',')[0],
                    "url": url[:500],
                    "description": desc[:3000],
                    "posted_date": posted_iso,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "experience_level": extract_exp(title, desc),
                    "job_type": "IT" if is_it else "Non-IT",
                    "is_remote": is_remote_job(title, desc, city)
                })
                seen.add(url)
        except Exception as e:
            logger.error(f"Error in {city} for {term}: {e}")
    return new_jobs

async def main():
    logger.info("🚀 ApplyMore Scraper Started (LinkedIn + Indeed + Others)")
    start = datetime.now(timezone.utc)
    
    # Step 1: Check expired links
    deleted_count = await check_links_activity()
    logger.info(f"🗑️ Deleted {deleted_count} expired jobs")
    
    # Step 2: Scrape IT jobs
    logger.info("💻 Scraping IT jobs...")
    async with aiohttp.ClientSession() as session:
        it_tasks = [scrape_city(session, city, is_it=True) for city in CITIES]
        it_results = await asyncio.gather(*it_tasks)
    it_jobs = [job for city_jobs in it_results for job in city_jobs]
    logger.info(f"💻 Found {len(it_jobs)} new IT jobs")
    
    # Step 3: Scrape Non-IT jobs
    logger.info("📊 Scraping Non-IT jobs...")
    async with aiohttp.ClientSession() as session:
        non_it_tasks = [scrape_city(session, city, is_it=False) for city in CITIES]
        non_it_results = await asyncio.gather(*non_it_tasks)
    non_it_jobs = [job for city_jobs in non_it_results for job in city_jobs]
    logger.info(f"📊 Found {len(non_it_jobs)} new Non-IT jobs")
    
    all_new = it_jobs + non_it_jobs
    logger.info(f"📦 Total new jobs found: {len(all_new)}")
    
    if not all_new:
        logger.info("No new jobs found. Skipping insert.")
        return
    
    # Step 4: Insert into Supabase
    inserted_ids = []
    for i in range(0, len(all_new), 50):
        batch = all_new[i:i+50]
        res = supabase.table("ApplyMore").insert(batch).execute()
        if res.data:
            inserted_ids.extend([row['id'] for row in res.data])
        logger.info(f"📥 Inserted batch {i//50+1} ({len(batch)} jobs)")
    
    # Step 5: Send Telegram notifications
    async with aiohttp.ClientSession() as session:
        for idx, jid in enumerate(inserted_ids):
            job = all_new[idx]
            await send_telegram(session, job, jid, job['job_type'])
            await asyncio.sleep(0.3)
    
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"✅ Completed in {elapsed:.1f}s. Inserted {len(inserted_ids)} jobs.")
    logger.info(f"📊 IT: {len(it_jobs)}, Non-IT: {len(non_it_jobs)}")

if __name__ == "__main__":
    asyncio.run(main())
