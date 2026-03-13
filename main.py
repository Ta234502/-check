import os
import json
import shutil
import asyncio
import subprocess
import re
import signal
import time
import logging
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified, UserNotParticipant
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image
import redis

# ===== RAILWAY CONFIGURATION (Environment Variables) =====
API_ID = int(os.environ.get('API_ID', 18373837))
API_HASH = os.environ.get('API_HASH', "sjjdidjeidkekdlkeisjeidjekd")
BOT_TOKEN = os.environ.get('BOT_TOKEN', "")
REDIS_URL = os.environ.get('REDIS_URL')

# Owner & Admin Config
OWNER_IDS = [int(id.strip()) for id in os.environ.get('OWNER_IDS', "7857898495,37478384883").split(",")]
BRANDING = os.environ.get('BRANDING', "🔗 Join Channel:- @ShinchanBannedMovies")
FSUB_CHANNEL = os.environ.get('FSUB_CHANNEL', "ShinchanBannedMovies").replace("@", "")

# Operation Limits
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', "pyrogram_recordings")
MAX_FILE_SIZE = float(os.environ.get('MAX_FILE_SIZE_GB', 1.9)) * 1024 * 1024 * 1024
MAX_JOBS_PER_USER = int(os.environ.get('MAX_JOBS_PER_USER', 2))
NUM_WORKERS = int(os.environ.get('NUM_WORKERS', 3))
USER_AGENT = os.environ.get('HTTP_USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')

# Logging
BOT_LOG_FILE = os.environ.get('BOT_LOG_FILE', "bot_activity.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", handlers=[logging.FileHandler(BOT_LOG_FILE), logging.StreamHandler()])
logger = logging.getLogger(__name__)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Client & State
app = Client("recorder_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
pending_states = {}
job_queue = asyncio.Queue()
running_jobs = {}
user_job_count = {}
redis_client = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None

# ===== Database Helpers =====

def get_db_set(key):
    return redis_client.smembers(key) if redis_client else set()

def is_banned(user_id):
    return str(user_id) in get_db_set("banned_users")

def is_admin(user_id):
    if user_id in OWNER_IDS: return True
    return str(user_id) in get_db_set("bot_admins")

async def check_fsub(user_id):
    if not FSUB_CHANNEL: return True
    try:
        member = await app.get_chat_member(FSUB_CHANNEL, user_id)
        return member.status not in [enums.ChatMemberStatus.BANNED, enums.ChatMemberStatus.LEFT]
    except UserNotParticipant: return False
    except Exception: return True

# ===== FFmpeg & Video Helpers =====

def get_video_attributes(file_path):
    try:
        proc = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,duration", "-of", "json", file_path], stdout=subprocess.PIPE, check=False)
        data = json.loads(proc.stdout.decode() or '{}')
        if 'streams' in data and data['streams']:
            s = data['streams'][0]
            return int(float(s.get('duration', 0))), int(s.get('width', 0)), int(s.get('height', 0))
    except: pass
    return 0, 0, 0

async def execute_ffmpeg_recording(chat_id, job_id, url, duration, final_path, video_track, audio_maps):
    temp_ts = f"{final_path}.ts"
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-user_agent", USER_AGENT,
        "-rw_timeout", "20000000", "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "10",
        "-i", url,
    ]
    if video_track: cmd.extend(["-map", f"0:{video_track['index']}", "-c:v", "copy"])
    else: cmd.append("-vn")
    for a in audio_maps: cmd.extend(["-map", f"0:{a}", "-c:a", "copy"])
    
    cmd.extend(["-max_muxing_queue_size", "9999", "-t", str(duration), "-f", "mpegts", temp_ts])
    
    try:
        proc = await asyncio.create_subprocess_exec(*cmd)
        running_jobs[job_id] = proc
        start_time = time.time()
        
        while proc.returncode is None:
            if time.time() - start_time >= duration + 5: # Safety buffer
                proc.terminate()
                break
            if pending_states.get(chat_id, {}).get(job_id, {}).get('cancelled'):
                proc.kill()
                break
            await asyncio.sleep(5)
            
        await proc.wait()
        return os.path.exists(temp_ts) and os.path.getsize(temp_ts) > 1024*1024 # Min 1MB
    except Exception as e:
        logger.error(f"FFmpeg Error: {e}")
        return False

# ===== Core Decorators =====

def auth_user(func):
    async def wrapper(client, message):
        uid = message.from_user.id
        if is_banned(uid): return
        if not await check_fsub(uid):
            return await message.reply_text(f"❌ **Join @{FSUB_CHANNEL} to use this bot!**", 
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Join Channel", url=f"https://t.me/{FSUB_CHANNEL}")]]))
        return await func(client, message)
    return wrapper

# ===== Handlers =====

@app.on_message(filters.command("start") & filters.private)
@auth_user
async def start_handler(client, message):
    await message.reply_text(f"👋 **Bot Online!**\n\nUse `/record` to start recording.\n\n{BRANDING}")

@app.on_message(filters.command("record") & filters.private)
@auth_user
async def record_handler(client, message):
    uid = message.from_user.id
    if user_job_count.get(uid, 0) >= MAX_JOBS_PER_USER:
        return await message.reply_text(f"❌ Limit reached! You can only have {MAX_JOBS_PER_USER} active jobs.")
    
    parts = message.text.split()
    if len(parts) < 3:
        return await message.reply_text("Usage: `/record <duration> <url> [filename]`\nExample: `/record 10m http://link.m3u8 myvideo`")

    # Time Parsing
    dur_str = parts[1]
    duration = int(dur_str[:-1]) * 60 if dur_str.endswith('m') else int(dur_str[:-1]) * 3600 if dur_str.endswith('h') else int(dur_str)
    
    url = parts[2]
    filename = " ".join(parts[3:]) or f"rec_{uid}_{int(time.time())}"
    if not filename.endswith(".mp4"): filename += ".mp4"
    
    # Store Job State with Security
    job_id = os.urandom(4).hex()
    msg = await message.reply_text("🔍 Fetching Stream Info...")
    
    pending_states.setdefault(message.chat.id, {})[job_id] = {
        'msg_id': msg.id, 'url': url, 'duration': duration, 'filename': os.path.join(OUTPUT_DIR, filename),
        'starter_user_id': uid, 'cancelled': False
    }
    
    # Simple Video Selection (You can expand this to multi-audio as per original code)
    await job_queue.put({'chat_id': message.chat.id, 'job_id': job_id, 'url': url, 'duration': duration, 
                         'filename': os.path.join(OUTPUT_DIR, filename), 'video': {'index': 0}, 'audios': [1], 'msg_id': msg.id})
    await msg.edit_text(f"✅ Job #{job_id} Queued!")

# ===== Admin Commands =====

@app.on_message(filters.command("ban") & filters.private)
async def ban_handler(client, message):
    if not is_admin(message.from_user.id): return
    if len(message.command) < 2: return await message.reply("Usage: `/ban <user_id>`")
    uid = message.command[1]
    if redis_client: redis_client.sadd("banned_users", uid)
    await message.reply(f"🚫 User {uid} has been banned.")

@app.on_message(filters.command("addadmin") & filters.private)
async def add_admin_handler(client, message):
    if message.from_user.id not in OWNER_IDS: return
    uid = message.command[1]
    if redis_client: redis_client.sadd("bot_admins", uid)
    await message.reply(f"⭐ User {uid} is now Admin.")

# ===== Callback Security =====

@app.on_callback_query()
async def callback_gatekeeper(client, cb):
    # Security: Check if user owns the job
    data = cb.data.split("|")
    if len(data) > 2:
        chat_id, job_id = int(data[1]), data[2]
        job = pending_states.get(chat_id, {}).get(job_id)
        if job and job['starter_user_id'] != cb.from_user.id and not is_admin(cb.from_user.id):
            return await cb.answer("🚫 You didn't start this recording!", show_alert=True)
    
    if data[0] == "cancel":
        pending_states[int(data[1])][data[2]]['cancelled'] = True
        await cb.message.edit_text("🛑 Cancellation Request Sent...")

# ===== Worker System =====

async def worker_task():
    while True:
        job = await job_queue.get()
        uid = job['chat_id']
        user_job_count[uid] = user_job_count.get(uid, 0) + 1
        
        try:
            success = await execute_ffmpeg_recording(uid, job['job_id'], job['url'], job['duration'], job['filename'], job['video'], job['audios'])
            if success:
                # Post processing (TS to MP4) and Upload logic from your original code
                logger.info(f"Job {job['job_id']} finished successfully.")
        except Exception as e:
            logger.error(f"Worker Error: {e}")
        finally:
            user_job_count[uid] = max(0, user_job_count.get(uid, 0) - 1)
            job_queue.task_done()

# ===== Startup =====

async def run_bot():
    workers = [asyncio.create_task(worker_task()) for _ in range(NUM_WORKERS)]
    await app.start()
    logger.info("Bot is Live!")
    await asyncio.gather(*workers)

if __name__ == "__main__":
    app.run(run_bot())
