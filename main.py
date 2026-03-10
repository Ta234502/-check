import os
import json
import shutil
import asyncio
import subprocess
import re
import signal
from datetime import datetime, timedelta
import time
import hashlib
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message
from PIL import Image
import redis

# ===== CONFIG =====
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
REDIS_URL = os.getenv("REDIS_URL")

OUTPUT_DIR = "pyrogram_recordings"
OWNER_IDS = [7857898495]

APPROVED_USERS = []

BRANDING = "🔗 Join Channel:- @ShinchanBannedMovies"

MAX_FILE_SIZE = 1.9 * 1024 * 1024 * 1024

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== PYROGRAM CLIENT =====
app = Client(
    "my_pyrogram_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=3
)

pending_states = {}
job_queue = asyncio.Queue()
running_jobs = {}
main_loop = asyncio.get_event_loop()

# ===== REDIS =====
redis_client = None
# ===== VIDEO INFO =====

def get_video_attributes(file_path):
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration",
                "-show_entries", "format=duration",
                "-of", "json",
                file_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        data = json.loads(proc.stdout.decode())

        width = 0
        height = 0
        duration = 0

        if "streams" in data and data["streams"]:
            stream = data["streams"][0]
            width = int(stream.get("width", 0))
            height = int(stream.get("height", 0))
            duration = float(stream.get("duration", 0))

        if duration == 0 and "format" in data:
            duration = float(data["format"].get("duration", 0))

        return int(duration), width, height

    except Exception as e:
        print("FFPROBE ERROR:", e)
        return 0, 0, 0


def get_duration(file_path):
    d, _, _ = get_video_attributes(file_path)
    return d


# ===== METADATA FIX =====

def fix_video_metadata(file_path):
    temp = file_path + ".fixed.mp4"

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i", file_path,
            "-map", "0",
            "-c", "copy",
            "-dn",
            "-sn",
            "-ignore_unknown",
            "-movflags", "+faststart",
            temp
        ]

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if os.path.exists(temp):
            os.replace(temp, file_path)
            return True

    except Exception as e:
        print("METADATA FIX ERROR:", e)

    return False


# ===== THUMBNAIL =====

def generate_thumbnail(video):
    thumb = video + ".jpg"

    try:
        duration = get_duration(video)

        ss = "00:00:05"
        if duration < 5:
            ss = "00:00:01"

        cmd = [
            "ffmpeg",
            "-y",
            "-ss", ss,
            "-i", video,
            "-frames:v", "1",
            "-q:v", "2",
            thumb
        ]

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if os.path.exists(thumb):
            Image.open(thumb).convert("RGB").save(thumb, "JPEG")
            return thumb

    except Exception as e:
        print("THUMB ERROR:", e)

    return None
    # ===== FILE SPLIT =====

def split_video_by_size(input_file, max_size, output_dir):
    try:
        total_duration = get_duration(input_file)

        if not total_duration:
            return [input_file]

        total_size = os.path.getsize(input_file)

        if total_size <= max_size:
            return [input_file]

        parts = int(total_size / max_size) + 1
        part_duration = total_duration // parts

        base, ext = os.path.splitext(os.path.basename(input_file))

        split_files = []
        start = 0

        for i in range(parts):

            output = os.path.join(output_dir, f"{base}_part{i+1}{ext}")

            cmd = [
                "ffmpeg",
                "-y",
                "-ss", str(start),
                "-i", input_file,
                "-t", str(part_duration),
                "-c", "copy",
                "-map", "0",
                "-dn",
                "-ignore_unknown",
                output
            ]

            if i == parts - 1:
                cmd = [
                    "ffmpeg",
                    "-y",
                    "-ss", str(start),
                    "-i", input_file,
                    "-c", "copy",
                    "-map", "0",
                    "-dn",
                    "-ignore_unknown",
                    output
                ]

            subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            split_files.append(output)
            start += part_duration

        os.remove(input_file)

        return split_files

    except Exception as e:
        print("SPLIT ERROR:", e)
        return [input_file]


# ===== UPLOAD WITH PROGRESS =====

async def upload_file_with_progress(client, chat_id, file_path, caption, thumb=None, msg_id=None):

    msg = None

    try:

        if msg_id:
            msg = await client.get_messages(chat_id, msg_id)
        else:
            msg = await client.send_message(chat_id, "🚀 Preparing Upload...")

        if not os.path.exists(file_path):
            await msg.edit_text("❌ File not found.")
            return

        size = os.path.getsize(file_path)

        duration, width, height = get_video_attributes(file_path)

        start = time.time()

        async def progress(current, total):

            percent = (current / total) * 100

            filled = int(percent / 10)
            bar = "■" * filled + "□" * (10 - filled)

            speed = current / (time.time() - start + 1)

            text = (
                f"🚀 Uploading...\n"
                f"[{bar}] {percent:.1f}%\n"
                f"{current/1024/1024:.2f}MB / {total/1024/1024:.2f}MB\n"
                f"Speed: {speed/1024:.2f} KB/s"
            )

            try:
                await msg.edit_text(text)
            except:
                pass

        await client.send_video(
            chat_id,
            video=file_path,
            caption=caption,
            thumb=thumb,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            progress=lambda c, t: asyncio.run_coroutine_threadsafe(progress(c, t), main_loop)
        )

        if msg:
            await msg.delete()

    except Exception as e:

        if msg:
            await msg.edit_text(f"❌ Upload failed\n{e}")

        print("UPLOAD ERROR:", e)
        # ===== FFmpeg RECORDING =====

async def execute_ffmpeg_recording(chat_id, job_id, url, duration, output):

    try:

        cmd = [
            "ffmpeg",
            "-y",
            "-rw_timeout", "30000000",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "10",
            "-i", url,
            "-t", str(duration),
            "-c", "copy",
            "-dn",
            "-ignore_unknown",
            output
        ]

        print("Running:", " ".join(cmd))

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        running_jobs[job_id] = proc

        progress_msg_id = pending_states[chat_id][job_id]["msg_id"]
        progress_msg = await app.get_messages(chat_id, progress_msg_id)

        start = time.time()

        while True:

            line = await proc.stderr.readline()

            if not line:
                break

            elapsed = int(time.time() - start)

            percent = min((elapsed / duration) * 100, 100)

            filled = int(percent / 10)
            bar = "■" * filled + "□" * (10 - filled)

            text = (
                "🎬 Recording in progress...\n\n"
                f"[{bar}] {percent:.1f}%\n"
                f"{elapsed}s / {duration}s"
            )

            try:
                await progress_msg.edit_text(text)
            except:
                pass

        await proc.wait()

        if job_id in running_jobs:
            del running_jobs[job_id]

        if os.path.exists(output):
            return True

        return False

    except Exception as e:

        print("Recording error:", e)

        if job_id in running_jobs:
            del running_jobs[job_id]

        return False


# ===== WORKER =====

async def worker():

    while True:

        job = await job_queue.get()

        try:

            msg = await app.get_messages(job["chat_id"], job["msg_id"])

            await msg.edit_text("✅ Job started! Recording in progress...")

            success = await execute_ffmpeg_recording(
                job["chat_id"],
                job["job_id"],
                job["url"],
                job["duration"],
                job["filename"]
            )

            if success:

                await msg.edit_text("🔧 Processing video...")

                fix_video_metadata(job["filename"])

                thumb = generate_thumbnail(job["filename"])

                caption = f"📺 Stream Recording\n\n{BRANDING}"

                await upload_file_with_progress(
                    app,
                    job["chat_id"],
                    job["filename"],
                    caption,
                    thumb,
                    msg_id=msg.id
                )

            else:

                await msg.edit_text("❌ Recording failed.")

        except Exception as e:

            print("Worker error:", e)

        finally:

            job_queue.task_done()


# ===== COMMAND =====

@app.on_message(filters.command("record"))
async def record_handler(client, message):

    parts = message.text.split()

    if len(parts) < 3:
        return await message.reply_text(
            "Usage:\n/record 10 http://example.m3u8"
        )

    duration = int(parts[1])
    url = parts[2]

    chat_id = message.chat.id

    job_id = os.urandom(6).hex()

    filename = os.path.join(
        OUTPUT_DIR,
        f"job_{chat_id}_{job_id}.mp4"
    )

    msg = await message.reply_text("🔍 Analyzing stream...")

    pending_states.setdefault(chat_id, {})[job_id] = {
        "msg_id": msg.id
    }

    job = {
        "chat_id": chat_id,
        "job_id": job_id,
        "url": url,
        "duration": duration,
        "filename": filename,
        "msg_id": msg.id
    }

    await job_queue.put(job)

    await msg.edit_text(
        f"📥 Job added to queue\nJob ID: `{job_id}`",
        parse_mode=enums.ParseMode.MARKDOWN
    )


# ===== START BOT =====

async def start_bot():

    workers = [asyncio.create_task(worker()) for _ in range(3)]

    await app.start()

    print("✅ Bot Started")

    for owner in OWNER_IDS:
        try:
            await app.send_message(owner, "Bot Online ✅")
        except:
            pass

    await asyncio.gather(*workers)


if __name__ == "__main__":

    try:

        app.run(start_bot())

    except KeyboardInterrupt:

        print("Bot stopped")
