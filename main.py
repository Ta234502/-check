import os
import json
import shutil
import asyncio
import subprocess
import re
import signal
from datetime import datetime, timedelta
import time
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image
import redis

# ===== CONFIG =====

API_ID = 13516702
API_HASH = "bf0cc3f062841935d3d5da65134ca4cf"
BOT_TOKEN = "8672443548:AAEtMfETNHyp-8DbxIkuSSJJBRUtbX7br_M"
REDIS_URL = os.environ.get('REDIS_URL')

OUTPUT_DIR = "pyrogram_recordings"
OWNER_IDS = [6407533831]
APPROVED_USERS = []
BRANDING = "🔗 Join Channel:- @DorutoChan"

MAX_FILE_SIZE = 1.9 * 1024 * 1024 * 1024

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== THUMBNAIL SYSTEM =====

user_thumbnails = {}
THUMB_TIMEOUT = 43200  # 12 hours

# ===== CLIENT SETUP =====

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

redis_client = None

# ===== VIDEO ATTRIBUTES =====

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
        stderr=subprocess.PIPE,
        check=False
    )

    output = proc.stdout.decode().strip()

    if not output:
        return 0, 0, 0

    data = json.loads(output)

    width = 0
    height = 0
    duration = 0

    if 'streams' in data and data['streams']:

        stream = data['streams'][0]

        width = int(stream.get('width', 0))
        height = int(stream.get('height', 0))

        try:
            duration = float(stream.get('duration', 0))
        except:
            pass

    if duration == 0 and 'format' in data and 'duration' in data['format']:

        try:
            duration = float(data['format']['duration'])
        except:
            pass

    return int(duration), width, height

except Exception as e:

    print(f"Error getting attributes with ffprobe: {e}")

    return 0, 0, 0

def get_duration(file_path):

d, _, _ = get_video_attributes(file_path)

return d

# ===== TS → MP4 CONVERTER =====

def convert_ts_to_mp4(ts_path, mp4_path):

try:

    print(f"🔧 Converting TS to MP4: {ts_path}")

    cmd = [
        "ffmpeg", "-y",
        "-i", ts_path,
        "-c", "copy",
        "-bsf:a", "aac_adtstoasc",
        "-movflags", "+faststart",
        mp4_path
    ]

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    if os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0:

        print("✅ Conversion successful")

        return True

    else:

        return False

except Exception as e:

    print(f"⚠️ Conversion failed: {e}")

    return False

# ===== METADATA FIX =====

def fix_video_metadata(file_path):

temp_path = f"{file_path}.temp.mp4"

try:

    cmd = [
        "ffmpeg", "-y",
        "-i", file_path,
        "-c", "copy",
        "-map", "0",
        "-dn", "-sn", "-ignore_unknown",
        "-movflags", "+faststart",
        temp_path
    ]

    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    if os.path.exists(temp_path):

        os.replace(temp_path, file_path)

        return True

    return False

except Exception as e:

    print(f"⚠️ Metadata fix failed: {e}")

    if os.path.exists(temp_path):
        os.remove(temp_path)

    return False

# ===== THUMBNAIL GENERATOR =====

def generate_thumbnail(video_path):

thumb_path = f"{video_path}.jpg"

try:

    if not os.path.exists(video_path):
        return None

    duration = get_duration(video_path)

    ss_time = "00:00:05"

    if duration < 5:
        ss_time = "00:00:01"

    subprocess.run(
        [
            "ffmpeg",
            "-y",
            "-ss", ss_time,
            "-i", video_path,
            "-frames:v", "1",
            "-q:v", "2",
            thumb_path
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    if os.path.exists(thumb_path):

        Image.open(thumb_path).convert("RGB").save(thumb_path, "JPEG")

        return thumb_path

except Exception as e:

    print(f"Thumbnail error: {e}")

return None

# ===== VIDEO SPLITTER =====

def split_video_by_size(input_file, max_size, output_dir):

try:

    total_duration = get_duration(input_file)

    if not total_duration:

        return [input_file]

    total_size = os.path.getsize(input_file)

    if total_size <= max_size:

        return [input_file]

    num_parts = int(total_size / max_size) + 1

    part_duration = total_duration // num_parts

    base_name, ext = os.path.splitext(os.path.basename(input_file))

    split_files = []

    start_time = 0

    for i in range(num_parts):

        output_file = os.path.join(
            output_dir,
            f"{base_name}_part{i+1}{ext}"
        )

        cmd = [
            "ffmpeg",
            "-y",
            "-ss", str(start_time),
            "-i", input_file,
            "-t", str(part_duration),
            "-c", "copy",
            "-map", "0",
            "-dn",
            "-ignore_unknown",
            output_file
        ]

        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        split_files.append(output_file)

        start_time += part_duration

    os.remove(input_file)

    return split_files

except Exception as e:

    print(f"Split error: {e}")

    return [input_file]
   # ===== UPLOAD SYSTEM WITH PROGRESS =====

async def upload_file_with_progress(client, chat_id, file_path, caption, thumb_path=None, msg_id=None, is_temp=False):

msg = None

try:

    if msg_id:
        try:
            msg = await client.get_messages(chat_id, message_ids=msg_id)
            await msg.edit_text("🚀 Preparing Upload...")
        except:
            msg = await client.send_message(chat_id, "🚀 Preparing Upload...")
    else:
        msg = await client.send_message(chat_id, "🚀 Preparing Upload...")

    if not os.path.exists(file_path):

        await msg.edit_text("❌ File not found.")

        return

    file_size = os.path.getsize(file_path)

    if file_size > MAX_FILE_SIZE:

        await msg.edit_text("🔄 File too large, splitting...")

        split_files = split_video_by_size(file_path, MAX_FILE_SIZE, OUTPUT_DIR)

        for i, part_file in enumerate(split_files):

            part_caption = f"Part {i+1}\n{caption}"

            part_thumb = generate_thumbnail(part_file)

            await upload_single_file(client, chat_id, part_file, part_caption, part_thumb)

            if os.path.exists(part_file):
                os.remove(part_file)

            if part_thumb and os.path.exists(part_thumb):
                os.remove(part_thumb)

            await asyncio.sleep(3)

    else:

        await upload_single_file(client, chat_id, file_path, caption, thumb_path, msg_id, is_temp)

    if msg:
        try:
            await msg.delete()
        except:
            pass

except Exception as e:

    if msg:
        await msg.edit_text(f"❌ Upload failed: {e}")

async def upload_single_file(client, chat_id, file_path, caption, thumb_path=None, msg_id=None, is_temp=False):

msg = None

if msg_id:
    try:
        msg = await client.get_messages(chat_id, message_ids=msg_id)
    except:
        msg = await client.send_message(chat_id, "🚀 Uploading...")
else:
    msg = await client.send_message(chat_id, "🚀 Uploading...")

# ===== THUMBNAIL CHECK =====

user_id = chat_id

thumb_to_send = thumb_path

if user_id in user_thumbnails:

    data = user_thumbnails[user_id]

    if time.time() - data["time"] < THUMB_TIMEOUT:

        thumb_to_send = data["path"]

    else:

        if os.path.exists(data["path"]):
            os.remove(data["path"])

        del user_thumbnails[user_id]

duration, width, height = get_video_attributes(file_path)

if duration == 0:
    duration = 1

start_time = time.time()

last_edit = time.time()

async def update_progress(current, total):

    nonlocal last_edit

    now = time.time()

    if current == total or now - last_edit > 5:

        percentage = current * 100 / total

        elapsed = now - start_time

        speed = current / elapsed if elapsed > 0 else 0

        eta = (total - current) / speed if speed > 0 else 0

        bar_len = 10

        filled = int(percentage / 100 * bar_len)

        bar = "■" * filled + "□" * (bar_len - filled)

        text = (
            f"🚀 Uploading...\n"
            f"[{bar}] {percentage:.2f}%\n"
            f"📦 {current/1024/1024:.2f}MB / {total/1024/1024:.2f}MB\n"
            f"⚡ {speed/1024/1024:.2f} MB/s\n"
            f"⏱ ETA {str(timedelta(seconds=int(eta)))}"
        )

        try:

            await msg.edit_text(text)

            last_edit = now

        except (FloodWait, MessageNotModified):
            pass

        except:
            pass

try:

    await client.send_video(

        chat_id=chat_id,
        video=file_path,
        caption=caption,
        thumb=thumb_to_send,
        duration=duration,
        width=width,
        height=height,
        supports_streaming=True,

        progress=lambda current, total: main_loop.create_task(
            update_progress(current, total)
        )
    )

    if msg:
        await msg.delete()

except Exception as e:

    if msg:
        await msg.edit_text(f"❌ Upload error: {e}")

finally:

    if is_temp and os.path.exists(file_path):
        os.remove(file_path)

    if thumb_path and os.path.exists(thumb_path):
        os.remove(thumb_path)
===== FFMPEG RECORDING ENGINE =====

async def execute_ffmpeg_recording(chat_id, job_id, m3u8_url, duration, final_output_path, video_track, audio_maps):

temp_ts_path = final_output_path + ".ts"

try:

    cmd = [
        "ffmpeg", "-y",
        "-hide_banner", "-loglevel", "error",
        "-rw_timeout", "20000000",
        "-analyzeduration", "20000000",
        "-probesize", "20000000",
        "-user_agent", "Mozilla/5.0",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "10",
        "-reconnect_at_eof", "1",
        "-fflags", "+genpts+discardcorrupt",
        "-err_detect", "ignore_err",
        "-i", m3u8_url,
    ]

    if video_track:
        cmd.extend([
            "-map", f"0:{video_track['index']}",
            "-c:v", "copy"
        ])
    else:
        cmd.extend(["-vn"])

    if audio_maps:

        for a in audio_maps:

            cmd.extend([
                "-map", f"0:{a}",
                "-c:a", "copy"
            ])
    else:
        cmd.extend(["-an"])

    cmd.extend([
        "-max_muxing_queue_size", "9999",
        "-dn", "-sn",
        "-ignore_unknown",
        "-map_metadata", "-1",
        "-t", str(duration + 10),
        "-f", "mpegts",
        temp_ts_path
    ])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    running_jobs[job_id] = proc

    progress_message_id = pending_states[chat_id][job_id]['msg_id']

    progress_message = await app.get_messages(chat_id, message_ids=progress_message_id)

    start_time = time.time()

    stop_time = start_time + duration

    last_update = time.time()

    while True:

        now = time.time()

        if now >= stop_time:

            try:
                proc.terminate()
            except:
                pass

            break

        try:

            line = await asyncio.wait_for(proc.stderr.readline(), timeout=5)

        except asyncio.TimeoutError:

            if proc.returncode is not None:
                break

            continue

        if not line:
            break

        elapsed = now - start_time

        percent = (elapsed / duration) * 100

        if percent > 100:
            percent = 99

        if now - last_update > 10:

            bar_len = 10

            filled = int(percent / 100 * bar_len)

            bar = "■" * filled + "□" * (bar_len - filled)

            size = 0

            if os.path.exists(temp_ts_path):
                size = os.path.getsize(temp_ts_path) / (1024 * 1024)

            text = (
                f"🎬 Recording...\n"
                f"[{bar}] {percent:.2f}%\n"
                f"📦 {size:.2f} MB\n"
                f"⏱ {str(timedelta(seconds=int(elapsed)))} / {str(timedelta(seconds=duration))}"
            )

            try:
                await progress_message.edit_text(text)
                last_update = now
            except:
                pass

        if pending_states.get(chat_id, {}).get(job_id, {}).get('cancelled', False):

            try:
                proc.send_signal(signal.SIGINT)
            except:
                pass

            await asyncio.sleep(5)

            break

    await proc.wait()

    if job_id in running_jobs:
        del running_jobs[job_id]

    if os.path.exists(temp_ts_path) and os.path.getsize(temp_ts_path) > 2 * 1024 * 1024:
        return True

    return False

except Exception as e:

    print(f"FFmpeg error {e}")

    if job_id in running_jobs:
        del running_jobs[job_id]

    return False

# ===== QUEUE WORKER =====

async def worker():

while True:

    job = await job_queue.get()

    temp_ts_path = job['filename'] + ".ts"

    final_output_path = job['filename']

    try:

        msg = await app.get_messages(job['chat_id'], message_ids=job['msg_id'])

        await msg.edit_text("🎬 Recording started...")

        success = await execute_ffmpeg_recording(
            job['chat_id'],
            job['job_id'],
            job['url'],
            job['duration'],
            final_output_path,
            job['video'],
            job['audios']
        )

        if success:

            await msg.edit_text("🔧 Converting TS → MP4...")

            if os.path.exists(temp_ts_path):

                convert_ts_to_mp4(temp_ts_path, final_output_path)

                os.remove(temp_ts_path)

            duration, width, height = get_video_attributes(final_output_path)

            caption = (
                f"📺 Stream Recording\n\n"
                f"📁 {os.path.basename(final_output_path)}\n"
                f"⏱ {str(timedelta(seconds=duration))}\n"
                f"{BRANDING}"
            )

            thumb = generate_thumbnail(final_output_path)

            await upload_file_with_progress(
                app,
                job['chat_id'],
                final_output_path,
                caption,
                thumb,
                msg_id=msg.id
            )

        else:

            await msg.edit_text("❌ Recording failed.")

    except Exception as e:

        print(f"Worker error {e}")

    finally:

        job_queue.task_done()

        if os.path.exists(temp_ts_path):
            os.remove(temp_ts_path)
         # ===== USER MANAGEMENT =====

def load_approved_users():

global APPROVED_USERS, redis_client

if not redis_client and REDIS_URL:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

try:

    if redis_client:

        redis_client.sadd("approved_users", *[str(uid) for uid in OWNER_IDS])

        approved_user_ids = redis_client.smembers("approved_users")

        APPROVED_USERS = [int(uid) for uid in approved_user_ids]

    else:

        APPROVED_USERS = list(OWNER_IDS)

except Exception as e:

    print(f"Redis error {e}")

    APPROVED_USERS = list(OWNER_IDS)

def is_owner(func):

async def wrapper(client, message):

    if message.from_user.id not in OWNER_IDS:

        await message.reply_text("🚫 Owner only command")

        return

    return await func(client, message)

return wrapper

def is_approved_user(func):

async def wrapper(client, message):

    if message.from_user.id not in APPROVED_USERS:

        await message.reply_text("🚫 You are not approved.")

        return

    return await func(client, message)

return wrapper

# ===== START COMMAND =====

@app.on_message(filters.command("start") & filters.private)
@is_approved_user
async def start_handler(client, message):

await message.reply_text(

    "👋 Bot Online\n\n"
    "/record <duration> <m3u8>\n"
    "/cancel\n"
    "/status\n"
    "/set_thumbnail\n"
    "/remove_thumbnail"

)

# ===== SET THUMBNAIL =====

@app.on_message(filters.command("set_thumbnail") & filters.private)
@is_approved_user
async def set_thumbnail(client, message):

await message.reply_text("📸 Send the image you want as thumbnail")

pending_states[message.from_user.id] = {"waiting_thumb": True}

@app.on_message(filters.photo & filters.private)
async def receive_thumbnail(client, message):

state = pending_states.get(message.from_user.id)

if not state or not state.get("waiting_thumb"):
    return

path = f"{OUTPUT_DIR}/thumb_{message.from_user.id}.jpg"

await message.download(path)

user_thumbnails[message.from_user.id] = {
    "path": path,
    "time": time.time()
}

pending_states.pop(message.from_user.id)

await message.reply_text("✅ Thumbnail saved for 12 hours")

# ===== REMOVE THUMBNAIL =====

@app.on_message(filters.command("remove_thumbnail") & filters.private)
@is_approved_user
async def remove_thumb(client, message):

user_id = message.from_user.id

if user_id in user_thumbnails:

    path = user_thumbnails[user_id]["path"]

    if os.path.exists(path):
        os.remove(path)

    del user_thumbnails[user_id]

    await message.reply_text("🗑 Thumbnail removed")

else:

    await message.reply_text("❌ No thumbnail set")

# ===== RECORD COMMAND =====

@app.on_message(filters.command("record") & filters.private)
@is_approved_user
async def record_handler(client, message):

parts = message.text.split()

if len(parts) < 3:

    return await message.reply_text(
        "/record <duration> <m3u8_url>"
    )

duration_str = parts[1]

if duration_str.endswith("m"):

    duration = int(duration_str[:-1]) * 60

elif duration_str.endswith("h"):

    duration = int(duration_str[:-1]) * 3600

else:

    duration = int(duration_str)

url = parts[2]

filename = f"record_{int(time.time())}.mp4"

final_output_path = os.path.join(OUTPUT_DIR, filename)

job_id = os.urandom(8).hex()

msg = await message.reply_text("🔍 Fetching stream info...")

try:

    proc = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v", "error",
        "-show_streams",
        "-print_format", "json",
        url,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    out, err = await proc.communicate()

    info = json.loads(out.decode())

except Exception as e:

    return await msg.edit_text(f"❌ ffprobe error {e}")

videos = []
audios = []

for stream in info.get("streams", []):

    if stream.get("codec_type") == "video":

        videos.append({
            "index": stream["index"],
            "width": stream.get("width"),
            "height": stream.get("height")
        })

    elif stream.get("codec_type") == "audio":

        audios.append(stream["index"])

if not videos:

    return await msg.edit_text("❌ No video tracks found")

pending_states.setdefault(message.chat.id, {})[job_id] = {

    "msg_id": msg.id,
    "url": url,
    "duration": duration,
    "filename": final_output_path,
    "video": videos[0],
    "audios": audios,
    "cancelled": False

}

await job_queue.put({

    "chat_id": message.chat.id,
    "job_id": job_id,
    "url": url,
    "duration": duration,
    "filename": final_output_path,
    "video": videos[0],
    "audios": audios,
    "msg_id": msg.id

})

await msg.edit_text("✅ Recording added to queue")

# ===== STATUS =====

@app.on_message(filters.command("status") & filters.private)
@is_approved_user
async def status_handler(client, message):

queue_size = job_queue.qsize()

running_size = len(running_jobs)

await message.reply_text(

    f"🎬 Running Jobs: {running_size}\n"
    f"🕰 Queue Jobs: {queue_size}"

)

# ===== CANCEL =====

@app.on_message(filters.command("cancel") & filters.private)
@is_approved_user
async def cancel_handler(client, message):

for job_id in list(running_jobs.keys()):

    proc = running_jobs[job_id]

    try:
        proc.kill()
    except:
        pass

await message.reply_text("❌ All running jobs cancelled")

# ===== BOT START =====

async def start_bot():

load_approved_users()

workers = [asyncio.create_task(worker()) for _ in range(3)]

await app.start()

print("✅ Bot started")

await asyncio.gather(*workers)

# ===== QUEUE COMMAND =====

@app.on_message(filters.command("queue") & filters.private)
@is_approved_user
async def queue_handler(client, message):

queue_size = job_queue.qsize()

running_size = len(running_jobs)

text = (
    f"📊 Queue Status\n\n"
    f"🎬 Running Jobs: {running_size}\n"
    f"🕰 Jobs in Queue: {queue_size}\n"
)

if running_size > 0:

    text += "\nRunning IDs:\n"

    for jid in running_jobs.keys():

        text += f"• `{jid[:8]}`\n"

await message.reply_text(text)

# ===== AUTO CLEANUP SYSTEM =====

async def cleanup_temp_files():

while True:

    try:

        for file in os.listdir(OUTPUT_DIR):

            path = os.path.join(OUTPUT_DIR, file)

            if file.endswith(".ts"):

                if os.path.exists(path):

                    size = os.path.getsize(path)

                    if size < 1024 * 1024:

                        os.remove(path)

            if file.endswith(".temp.mp4"):

                if os.path.exists(path):

                    os.remove(path)

    except Exception as e:

        print(f"Cleanup error {e}")

    await asyncio.sleep(600)

# ===== BEST QUALITY VIDEO SELECT =====

def select_best_video(videos):

if not videos:

    return None

best = videos[0]

for v in videos:

    if v.get("width", 0) > best.get("width", 0):

        best = v

return best

# ===== BOT START IMPROVED =====

async def start_bot():

load_approved_users()

workers = [asyncio.create_task(worker()) for _ in range(3)]

asyncio.create_task(cleanup_temp_files())

await app.start()

print("✅ Bot started successfully")

for owner in OWNER_IDS:

    try:
        await app.send_message(owner, "✅ Bot Online")
    except:
        pass

await asyncio.gather(*workers)

if name == "main":

try:

    app.run(start_bot())

except KeyboardInterrupt:

    print("Bot stopped")
