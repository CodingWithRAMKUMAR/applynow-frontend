import os
import asyncio
import aiohttp
import re
import pandas as pd
from datetime import datetime, timezone, timedelta
from supabase import create_client
from jobspy import scrape_jobs

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

SITES = ["linkedin", "indeed"]
SEARCH_TERMS = [
    "fresher software engineer", "graduate engineer trainee", "entry level developer",
    "fresher data analyst", "trainee engineer"
]
CITIES = ["Hyderabad, India", "Bangalore, India", "Chennai, India"]
RESULTS_WANTED = 8
HOURS_OLD = 72

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-2", "2024", "2025"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head"}

SKILLS_KEYWORDS = {"python", "sql", "java", "javascript", "react", "angular", "aws", "azure", "docker", "kubernetes", "excel", "tableau", "power bi", "git", "selenium", "django", "flask"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def safe_str(v):
    if pd.isna(v) or v is None:
        return ""
    return str(v)

def is_fresher(title, desc):
    text = (safe_str(title) + " " + safe_str(desc)).lower()
    return any(k in text for k in FRESHER_WORDS) and not any(k in text for k in SENIOR_WORDS)

def extract_exp(title, desc):
    return "Fresher (0-2 years)"  # all jobs are fresher anyway

def extract_skills(desc):
    if not desc:
        return []
    text = desc.lower()
    found = [skill for skill in SKILLS_KEYWORDS if skill in text]
    return found[:4]

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

async def send_telegram(session, job, job_id):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    link = f"https://applymore.vercel.app/job.html?id={job_id}"
    title = safe_str(job['title'])
    company = safe_str(job['company'])
    location = safe_str(job['location'])
    desc = safe_str(job.get("description", ""))[:300].replace('\n', ' ')
    if len(desc) > 297:
        desc += "..."
    posted_str = format_posted_date(job.get("posted_date"))
    skills = extract_skills(job.get("description", ""))
    skills_text = ", ".join(skills) if skills else "Not listed"
    
    message = (
        f"🚀 *New: {title}*\n"
        f"🏢 {company} | 📍 {location}\n"
        f"📅 Posted: {posted_str}\n"
        f"🎓 Experience: Fresher (0-2 years)\n"
        f"🔧 Skills: {skills_text}\n\n"
        f"📝 *Description:*\n{desc}\n\n"
        f"🔗 [Apply on ApplyMore]({link})\n"
        f"⚠️ *APPLY ASAP*"
    )
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        await session.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        })
    except Exception as e:
        print(f"Telegram error: {e}")

async def scrape_city(session, city):
    new_jobs = []
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    seen = set()
    for term in SEARCH_TERMS:
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
                posted_iso = posted.isoformat() if isinstance(posted, datetime) else (safe_str(posted) if posted else datetime.now(timezone.utc).isoformat())
                new_jobs.append({
                    "title": title,
                    "company": company,
                    "location": city.split(',')[0],
                    "url": url,
                    "description": desc,
                    "posted_date": posted_iso,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "experience_level": extract_exp(title, desc)
                })
                seen.add(url)
        except Exception as e:
            print(f"Error in {city} for {term}: {e}")
    return new_jobs

async def main():
    print("⚡ ApplyMore – Enhanced Alerts Scraper Started (Fixed DB schema)")
    start = datetime.now(timezone.utc)

    async with aiohttp.ClientSession() as session:
        tasks = [scrape_city(session, city) for city in CITIES]
        results = await asyncio.gather(*tasks)
    all_new = [job for city_jobs in results for job in city_jobs]
    print(f"New jobs found: {len(all_new)}")

    if not all_new:
        async with aiohttp.ClientSession() as session:
            await session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": "⚠️ No new fresher jobs found."})
        return

    inserted_ids = []
    for i in range(0, len(all_new), 50):
        batch = all_new[i:i+50]
        res = supabase.table("ApplyMore").insert(batch).execute()
        if res.data:
            inserted_ids.extend([row['id'] for row in res.data])
        print(f"Inserted batch {i//50+1} ({len(batch)} jobs)")

    async with aiohttp.ClientSession() as session:
        for idx, jid in enumerate(inserted_ids):
            await send_telegram(session, all_new[idx], jid)
            await asyncio.sleep(0.5)

    elapsed = (datetime.now(timezone.utc)-start).total_seconds()
    print(f"✅ Finished in {elapsed:.1f}s. Inserted {len(inserted_ids)} jobs.")

if __name__ == "__main__":
    asyncio.run(main())
