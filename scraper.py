import os
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
from supabase import create_client

# ================= CONFIGURATION =================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
RAPIDAPI_KEY = os.environ["RAPIDAPI_KEY"].strip()
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

SEARCH_TERMS = [
    "fresher software engineer", "graduate engineer trainee", "entry level developer",
    "fresher data analyst", "trainee engineer", "fresher devops", "fresher cybersecurity", "fresher qa"
]
CITIES = ["Hyderabad", "Bangalore", "Chennai", "Pune", "Mumbai", "Noida"]
DAYS_BACK = 3
RESULTS_PER_PAGE = 20

FRESHER_WORDS = {"fresher", "entry level", "graduate", "trainee", "junior", "0-1", "0-2", "1 year", "2 years", "2024", "2025", "recent graduate"}
SENIOR_WORDS = {"senior", "lead", "principal", "architect", "manager", "director", "head", "vp", "cto", "staff"}

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ================= HELPER FUNCTIONS =================
def is_fresher_job(title: str, description: str) -> bool:
    text = (title + " " + description).lower()
    has_fresher = any(word in text for word in FRESHER_WORDS)
    has_senior = any(word in text for word in SENIOR_WORDS)
    return has_fresher and not has_senior

async def fetch_jobs(session: aiohttp.ClientSession, query: str, city: str) -> list:
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{query} {city}",
        "page": 1,
        "num_pages": 1,
        "country": "in",
        "date_posted": "week",
        "results_per_page": RESULTS_PER_PAGE
    }
    headers = {
        "x-rapidapi-key": RAPIDAPI_KEY,
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }
    headers = {k: v.replace('\n', '').replace('\r', '').strip() for k, v in headers.items()}
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as resp:
            if resp.status != 200:
                print(f"API error {resp.status} for {query} in {city}")
                return []
            data = await resp.json()
            return data.get("data", [])
    except Exception as e:
        print(f"Request failed for {query} in {city}: {e}")
        return []

async def send_telegram_message(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials missing – skipping notification.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        await session.post(url, json=payload)
    except Exception as e:
        print(f"Telegram send error: {e}")

# ================= MAIN WORKFLOW =================
async def main():
    print("🚀 ApplyMore – parallel job scraper started")
    start_time = datetime.now(timezone.utc)

    # 1. Fetch existing URLs
    existing_resp = supabase.table("ApplyMore").select("url").execute()
    existing_urls = {row["url"] for row in existing_resp.data} if existing_resp.data else set()
    print(f"📊 Existing jobs: {len(existing_urls)}")

    # 2. Parallel API calls
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_jobs(session, term, city) for term in SEARCH_TERMS for city in CITIES]
        all_results = await asyncio.gather(*tasks)

    # 3. Filter and collect new jobs
    seen_urls = set()
    new_jobs = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    for job_list in all_results:
        for job in job_list:
            title = job.get("job_title")
            company = job.get("employer_name")
            url = job.get("job_apply_link")
            desc = job.get("job_description", "")
            posted_str = job.get("job_posted_at_datetime_utc") or job.get("job_posted_at")
            if not title or not company or not url:
                continue
            if url in existing_urls or url in seen_urls:
                continue
            if posted_str:
                try:
                    posted_dt = datetime.fromisoformat(posted_str.replace('Z', '+00:00'))
                    if posted_dt < cutoff:
                        continue
                except:
                    pass
            if not is_fresher_job(title, desc):
                continue
            new_jobs.append({
                "title": title,
                "company": company,
                "url": url,
                "description": desc,
                "posted_date": posted_str or datetime.now(timezone.utc).isoformat(),
                "created_at": datetime.now(timezone.utc).isoformat()
            })
            seen_urls.add(url)

    print(f"✨ New fresher jobs found: {len(new_jobs)}")

    # 4. Insert and retrieve IDs for ApplyMore links
    inserted_jobs_with_ids = []
    if new_jobs:
        # Insert in batches
        for i in range(0, len(new_jobs), 50):
            batch = new_jobs[i:i+50]
            result = supabase.table("ApplyMore").insert(batch).execute()
            # Supabase returns inserted rows; if not, fallback to fetch by created_at
            if result.data:
                inserted_jobs_with_ids.extend(result.data)
            else:
                # Fallback: fetch newly inserted rows using created_at timestamp
                # (use the last few seconds to be safe)
                fetch_after = datetime.now(timezone.utc) - timedelta(seconds=5)
                fetch_resp = supabase.table("ApplyMore").select("*").gte("created_at", fetch_after.isoformat()).execute()
                if fetch_resp.data:
                    inserted_jobs_with_ids.extend(fetch_resp.data)
            print(f"   Inserted batch {i//50 + 1} ({len(batch)} jobs)")

        print(f"✅ Inserted {len(inserted_jobs_with_ids)} jobs with IDs.")

        # 5. Send Telegram alert using ApplyMore internal links + "APPLY ASAP"
        async with aiohttp.ClientSession() as session:
            lines = [f"✅ <b>ApplyMore – {len(inserted_jobs_with_ids)} new fresher jobs</b>\n"]
            for idx, job_record in enumerate(inserted_jobs_with_ids[:10], 1):
                job_id = job_record.get("id")
                title = job_record.get("title")
                company = job_record.get("company")
                if not job_id or not title or not company:
                    continue
                applymore_url = f"https://applymore.vercel.app/job.html?id={job_id}"
                lines.append(
                    f"{idx}. <b>{title}</b>\n"
                    f"   🏢 {company}\n"
                    f"   🔗 <a href='{applymore_url}'>View & Apply on ApplyMore</a>\n"
                    f"   ⚠️ <b>APPLY ASAP</b>"
                )
            if len(inserted_jobs_with_ids) > 10:
                lines.append(f"\n... and {len(inserted_jobs_with_ids)-10} more. <a href='https://applymore.vercel.app'>Browse all jobs</a>")
            else:
                lines.append(f"\n🌐 <a href='https://applymore.vercel.app'>Visit ApplyMore</a>")
            await send_telegram_message(session, "\n\n".join(lines))
    else:
        print("ℹ️ No new jobs to insert.")
        async with aiohttp.ClientSession() as session:
            await send_telegram_message(session, "⚠️ ApplyMore scraper ran but found no new fresher jobs.")

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    print(f"✅ Finished in {elapsed:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
