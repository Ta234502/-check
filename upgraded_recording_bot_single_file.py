
import os
import asyncio
import subprocess
import time
import uuid
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait

# ===== ENV CONFIG =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")

FORCE_SUB = "DorutoChan"
LOG_CHANNEL = int(os.getenv("LOG_CHANNEL", "0"))

OWNER_IDS = [int(x) for x in os.getenv("OWNER_IDS","").split(",") if x]

OUTPUT_DIR = "recordings"
MAX_FILE_SIZE = 1.9 * 1024 * 1024 * 1024

os.makedirs(OUTPUT_DIR, exist_ok=True)

app = Client(
    "record_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
)

ADMINS = set()
BANNED = set()

job_queue = asyncio.Queue()
running_jobs = {}

# ===== Force Subscribe =====
async def check_sub(client, user_id):
    try:
        member = await client.get_chat_member(FORCE_SUB, user_id)
        return member.status not in ["kicked","left"]
    except:
        return False


# ===== Logging =====
async def log(text):
    if LOG_CHANNEL:
        try:
            await app.send_message(LOG_CHANNEL, text)
        except:
            pass


# ===== Safe Send =====
async def safe_send(msg, text):
    try:
        return await msg.reply(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await msg.reply(text)


# ===== START =====
@app.on_message(filters.command("start"))
async def start(client, message):

    if message.from_user.id in BANNED:
        return

    if not await check_sub(client, message.from_user.id):
        btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Join Channel", url=f"https://t.me/{FORCE_SUB}")]]
        )
        return await message.reply("⚠️ Please join channel first", reply_markup=btn)

    await message.reply(
        "🎬 M3U8 Recording Bot Ready\n\n"
        "/record 60 <m3u8_url>"
    )


# ===== ADMIN COMMANDS =====

@app.on_message(filters.command("addadmin") & filters.user(OWNER_IDS))
async def addadmin(_, message):
    try:
        uid = int(message.command[1])
        ADMINS.add(uid)
        await message.reply("✅ Admin Added")
    except:
        await message.reply("Usage: /addadmin user_id")


@app.on_message(filters.command("ban") & filters.user(OWNER_IDS))
async def ban(_, message):
    try:
        uid = int(message.command[1])
        BANNED.add(uid)
        await message.reply("🚫 User Banned")
    except:
        await message.reply("Usage: /ban user_id")


@app.on_message(filters.command("unban") & filters.user(OWNER_IDS))
async def unban(_, message):
    try:
        uid = int(message.command[1])
        BANNED.discard(uid)
        await message.reply("✅ User Unbanned")
    except:
        await message.reply("Usage: /unban user_id")


# ===== RECORD COMMAND =====

@app.on_message(filters.command("record"))
async def record(client, message):

    if message.from_user.id in BANNED:
        return

    if len(message.command) < 3:
        return await message.reply(
            "Usage:\n/record 60 https://link.m3u8"
        )

    duration = message.command[1]
    url = message.command[2]

    job_id = str(uuid.uuid4())

    await job_queue.put((job_id, message.chat.id, duration, url, message.from_user.id))

    await message.reply(
        f"📥 Added to queue\nJob ID: {job_id}"
    )


# ===== Worker =====

async def worker():
    while True:

        job_id, chat_id, duration, url, user_id = await job_queue.get()

        filename = f"{OUTPUT_DIR}/{int(time.time())}_{job_id}.mp4"

        ffmpeg_cmd = [
            "ffmpeg",
            "-reconnect","1",
            "-reconnect_streamed","1",
            "-reconnect_delay_max","5",
            "-i",url,
            "-t",duration,
            "-c","copy",
            filename
        ]

        proc = subprocess.Popen(ffmpeg_cmd)
        running_jobs[job_id] = proc

        await log(f"🎬 Recording\nUser: {user_id}\nURL: {url}")

        proc.wait()

        running_jobs.pop(job_id, None)

        if os.path.exists(filename):

            try:
                await app.send_video(chat_id, filename)
            except FloodWait as e:
                await asyncio.sleep(e.value)
                await app.send_video(chat_id, filename)

            os.remove(filename)


# ===== CANCEL =====

@app.on_message(filters.command("cancel"))
async def cancel(_, message):

    if len(message.command) < 2:
        return await message.reply("Usage: /cancel job_id")

    job_id = message.command[1]

    if job_id in running_jobs:
        running_jobs[job_id].terminate()
        running_jobs.pop(job_id, None)
        await message.reply("❌ Recording Cancelled")
    else:
        await message.reply("Job not running")


# ===== MAIN =====

async def main():
    asyncio.create_task(worker())
    print("✅ Recording bot running...")
    await app.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
