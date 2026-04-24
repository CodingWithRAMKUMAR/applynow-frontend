import os
import asyncio
import aiohttp
from datetime import datetime, timezone
from supabase import create_client
import re

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"]

SEARCHES = [
    ("Data Analyst", "Hyderabad"), ("Data Analyst", "Bangalore"),
    ("Apprenticeship", "Chennai"), ("Java Developer", "Mumbai"),
    ("Python Developer", "Pune"), ("Cyber Security", "Gurugram"),
    ("DevOps Engineer", "Noida"), ("Data Scientist", "Delhi"),
    ("Software Engineer", "Hyderabad"), ("Software Engineer", "Bangalore"),
]

ALLOWED_CITIES = {c.lower() for c in ["Hyderabad","Chennai","Mumbai","Pune","Gurugram","Bangalore","Delhi","Kolkata","Ahmedabad","Noida"]}
SENIOR_KEYWORDS = ["senior","lead","principal","architect","manager","director","head"]

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

async def fetch(session, role, city):
    url = "https://jsearch.p.rapidapi.com/search"
    params = {"query": f"{role} {city}", "page": 1, "num_pages": 1, "country": "in", "date_posted": "all"}
    headers = {"x-rapidapi-key": RAPIDAPI_KEY, "x-rapidapi-host": "jsearch.p.rapidapi.com"}
    async with session.get(url, headers=headers, params=params) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()
        return data.get("data", [])

def parse_location(loc):
    if not loc:
        return None
    city = loc.split(",")[0].strip()
    return city if city.lower() in ALLOWED_CITIES else None

def is_senior(title):
    return any(kw in title.lower() for kw in SENIOR_KEYWORDS)

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, role, city) for role, city in SEARCHES]
        results = await asyncio.gather(*tasks)
    all_jobs = [job for sublist in results for job in sublist]
    print(f"Raw: {len(all_jobs)}")

    existing = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing.data} if existing.data else set()

    new_jobs = []
    seen = set()
    for job in all_jobs:
        title = job.get("job_title")
        company = job.get("employer_name")
        url = job.get("job_apply_link")
        loc_raw = job.get("job_location")
        desc = job.get("job_description", "")
        posted = job.get("job_posted_at_datetime_utc") or datetime.now(timezone.utc).isoformat()

        if not title or not company or not url:
            continue
        if url in existing_urls or url in seen:
            continue
        city = parse_location(loc_raw)
        if not city:
            continue
        if is_senior(title):
            continue
        seen.add(url)
        new_jobs.append({
            "title": title, "company": company, "location": city,
            "url": url, "description": desc, "posted_date": posted,
            "created_at": datetime.now(timezone.utc).isoformat()
        })

    print(f"New: {len(new_jobs)}")
    if new_jobs:
        for i in range(0, len(new_jobs), 50):
            supabase.table("ApplyMore").insert(new_jobs[i:i+50]).execute()
        print("Inserted.")

    # Telegram
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id and new_jobs:
        async with aiohttp.ClientSession() as session:
            msg = f"✅ ApplyMore: {len(new_jobs)} new jobs added."
            await session.post(f"https://api.telegram.org/bot{token}/sendMessage", json={"chat_id": chat_id, "text": msg})

if __name__ == "__main__":
    asyncio.run(main())
