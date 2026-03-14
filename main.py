import asyncio
import contextlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import redis
from PIL import Image
from pyrogram import Client, enums, filters, idle
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# ===== CONFIG =====
API_ID = int(os.environ.get("API_ID", "12345"))
API_HASH = os.environ.get("API_HASH", "CHANGE_ME")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "CHANGE_ME")
REDIS_URL = os.environ.get("REDIS_URL")
OWNER_IDS = [int(i.strip()) for i in os.environ.get("OWNER_IDS", "").split(",") if i.strip()]
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "pyrogram_recordings")
BOT_LOG_FILE = os.environ.get("BOT_LOG_FILE", "bot_activity.log")
FSUB_CHANNEL = os.environ.get("FSUB_CHANNEL", "").replace("@", "").strip()
BRANDING = os.environ.get("BRANDING", "🔗 Join Channel:- @DoraemonBro")
MAX_FILE_SIZE = float(os.environ.get("MAX_FILE_SIZE_GB", "1.9")) * 1024 * 1024 * 1024
MAX_JOBS_PER_USER = int(os.environ.get("MAX_JOBS_PER_USER", "2"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "2"))
HTTP_USER_AGENT = os.environ.get(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
AUTO_DELETE_AFTER_UPLOAD = os.environ.get("AUTO_DELETE_AFTER_UPLOAD", "true").lower() in {"1", "true", "yes", "on"}

Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(BOT_LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("ripbot")

app = Client(
    "my_pyrogram_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=3,
)

redis_client = None
APPROVED_USERS = set(OWNER_IDS)
pending_states: Dict[int, Dict[str, Dict[str, Any]]] = {}
running_jobs: Dict[str, asyncio.subprocess.Process] = {}
job_queue: asyncio.Queue = asyncio.Queue()
queue_lock = asyncio.Lock()
worker_tasks: List[asyncio.Task] = []


# ===== REDIS / AUTH =====
def get_redis_client():
    global redis_client
    if redis_client is not None:
        return redis_client
    if not REDIS_URL:
        return None
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        return redis_client
    except Exception as exc:
        logger.warning("Redis unavailable: %s", exc)
        redis_client = None
        return None


def load_approved_users() -> None:
    APPROVED_USERS = set(OWNER_IDS)
    rc = get_redis_client()
    if not rc:
        return
    try:
        if OWNER_IDS:
            rc.sadd("approved_users", *[str(uid) for uid in OWNER_IDS])
        approved = rc.smembers("approved_users")
        APPROVED_USERS.update(int(uid) for uid in approved)
    except Exception as exc:
        logger.warning("Failed loading approved users: %s", exc)


def is_banned(user_id: int) -> bool:
    rc = get_redis_client()
    if not rc:
        return False
    try:
        return bool(rc.sismember("banned_users", str(user_id)))
    except Exception:
        return False


def is_admin(user_id: int) -> bool:
    if user_id in OWNER_IDS:
        return True
    rc = get_redis_client()
    if not rc:
        return False
    try:
        return bool(rc.sismember("bot_admins", str(user_id)))
    except Exception:
        return False


async def check_fsub(user_id: int) -> bool:
    if not FSUB_CHANNEL:
        return True
    try:
        member = await app.get_chat_member(FSUB_CHANNEL, user_id)
        return member.status not in {enums.ChatMemberStatus.LEFT, enums.ChatMemberStatus.BANNED}
    except Exception:
        return True


async def ensure_user_allowed(message):
    return True


async def ensure_owner(message) -> bool:
    if not message.from_user or message.from_user.id not in OWNER_IDS:
        await message.reply_text("🚫 Sorry, this command is only for the bot owner(s).")
        return False
    return True

# ===== HELPERS =====
def sanitize_filename(name: str, fallback: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|\n\r\t]+", "_", name).strip().strip(".")
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        cleaned = fallback
    if not cleaned.lower().endswith(".mp4"):
        cleaned += ".mp4"
    return cleaned[:180]


def parse_duration_to_seconds(raw: str) -> Optional[int]:
    raw = raw.strip().lower()
    match = re.fullmatch(r"(\d+)([smhd]?)", raw)
    if not match:
        return None
    value = int(match.group(1))
    suffix = match.group(2) or "s"
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix]
    return value * factor


def get_video_attributes(file_path: str) -> Tuple[int, int, int]:
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height,duration",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if not proc.stdout.strip():
            return 0, 0, 0
        data = json.loads(proc.stdout)
        width = 0
        height = 0
        duration = 0.0
        streams = data.get("streams") or []
        if streams:
            stream = streams[0]
            width = int(stream.get("width") or 0)
            height = int(stream.get("height") or 0)
            with contextlib.suppress(Exception):
                duration = float(stream.get("duration") or 0)
        if duration <= 0:
            with contextlib.suppress(Exception):
                duration = float((data.get("format") or {}).get("duration") or 0)
        return int(duration), width, height
    except Exception as exc:
        logger.warning("ffprobe attribute read failed for %s: %s", file_path, exc)
        return 0, 0, 0


def get_duration(file_path: str) -> int:
    duration, _, _ = get_video_attributes(file_path)
    return duration


def minimum_acceptable_duration(requested_seconds: int) -> int:
    if requested_seconds >= 300:
        return int(requested_seconds * 0.90)
    if requested_seconds >= 120:
        return int(requested_seconds * 0.85)
    if requested_seconds >= 30:
        return int(requested_seconds * 0.80)
    return int(requested_seconds * 0.65)


def convert_ts_to_mp4(ts_path: str, mp4_path: str) -> bool:
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            ts_path,
            "-c",
            "copy",
            "-map",
            "0",
            "-dn",
            "-ignore_unknown",
            "-bsf:a",
            "aac_adtstoasc",
            "-movflags",
            "+faststart",
            mp4_path,
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        if result.returncode != 0:
            logger.error("TS->MP4 conversion failed: %s", result.stderr[-500:])
            return False
        return os.path.exists(mp4_path) and os.path.getsize(mp4_path) > 0
    except Exception as exc:
        logger.error("TS->MP4 conversion exception: %s", exc)
        return False


def fix_video_metadata(file_path: str) -> bool:
    temp_path = f"{file_path}.meta_fix.mp4"
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            file_path,
            "-c",
            "copy",
            "-map",
            "0",
            "-dn",
            "-sn",
            "-ignore_unknown",
            "-movflags",
            "+faststart",
            temp_path,
        ]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
        if result.returncode != 0:
            logger.error("Metadata repair failed: %s", result.stderr[-500:])
            return False
        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            os.replace(temp_path, file_path)
            return True
        return False
    except Exception as exc:
        logger.error("Metadata repair exception: %s", exc)
        return False
    finally:
        if os.path.exists(temp_path):
            with contextlib.suppress(Exception):
                os.remove(temp_path)


def generate_thumbnail(video_path: str) -> Optional[str]:
    thumb_path = f"{video_path}.jpg"
    try:
        if not os.path.exists(video_path) or os.path.getsize(video_path) <= 0:
            return None
        duration = get_duration(video_path)
        ss_time = "00:00:05" if duration >= 5 else "00:00:01"
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                ss_time,
                "-i",
                video_path,
                "-frames:v",
                "1",
                "-q:v",
                "2",
                thumb_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        if os.path.exists(thumb_path):
            Image.open(thumb_path).convert("RGB").save(thumb_path, "JPEG")
            return thumb_path
    except Exception as exc:
        logger.warning("Thumbnail generation failed for %s: %s", video_path, exc)
    return None


def split_video_by_size(input_file: str, max_size: float, output_dir: str) -> List[str]:
    try:
        total_duration = get_duration(input_file)
        total_size = os.path.getsize(input_file)
        if not total_duration or total_size <= max_size:
            return [input_file]

        num_parts = int(total_size / max_size) + 1
        part_duration = max(1, total_duration // num_parts)
        base_name, ext = os.path.splitext(os.path.basename(input_file))
        split_files: List[str] = []

        for i in range(num_parts):
            start_time = i * part_duration
            output_file = os.path.join(output_dir, f"{base_name}_part{i + 1}{ext}")
            cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-ss",
                str(start_time),
                "-i",
                input_file,
            ]
            if i < num_parts - 1:
                cmd.extend(["-t", str(part_duration)])
            cmd.extend(["-c", "copy", "-map", "0", "-dn", "-ignore_unknown", output_file])
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            if result.returncode != 0 or not os.path.exists(output_file) or os.path.getsize(output_file) == 0:
                logger.error("Split failed on part %s: %s", i + 1, result.stderr[-500:])
                for part in split_files:
                    with contextlib.suppress(Exception):
                        os.remove(part)
                return [input_file]
            split_files.append(output_file)

        with contextlib.suppress(Exception):
            os.remove(input_file)
        return split_files
    except Exception as exc:
        logger.error("Video split failed: %s", exc)
        return [input_file]


def build_job_paths(chat_id: int, job_id: str, final_name: str) -> Dict[str, str]:
    job_prefix = f"job_{chat_id}_{job_id}"
    work_dir = os.path.join(OUTPUT_DIR, job_prefix)
    os.makedirs(work_dir, exist_ok=True)
    final_path = os.path.join(work_dir, final_name)
    temp_ts_path = os.path.join(work_dir, f"{Path(final_name).stem}.ts")
    stderr_path = os.path.join(work_dir, "ffmpeg_stderr.log")
    return {"work_dir": work_dir, "final_path": final_path, "temp_ts_path": temp_ts_path, "stderr_path": stderr_path}


def cleanup_paths(*paths: Optional[str]) -> None:
    for path in paths:
        if not path:
            continue
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        elif os.path.exists(path):
            with contextlib.suppress(Exception):
                os.remove(path)


def user_active_job_count(user_id: int) -> int:
    count = 0
    for chat_jobs in pending_states.values():
        for state in chat_jobs.values():
            if state.get("starter_user_id") == user_id and not state.get("completed"):
                count += 1
    return count


async def get_state_from_callback(callback_query, require_owner: bool = True) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[str]]:
    parts = callback_query.data.split("|")
    if len(parts) < 3:
        await callback_query.answer("Invalid action.", show_alert=True)
        return None, None, None
    _, chat_id_str, job_id = parts[:3]
    try:
        chat_id = int(chat_id_str)
    except ValueError:
        await callback_query.answer("Invalid job id.", show_alert=True)
        return None, None, None
    state = pending_states.get(chat_id, {}).get(job_id)
    if not state:
        await callback_query.answer("Session expired!", show_alert=True)
        return None, None, None
    if callback_query.from_user.id not in APPROVED_USERS:
        await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)
        return None, None, None
    if require_owner:
        starter_user_id = state.get("starter_user_id")
        if starter_user_id and callback_query.from_user.id != starter_user_id and not is_admin(callback_query.from_user.id):
            await callback_query.answer("🚫 These controls are not yours.", show_alert=True)
            return None, None, None
    return state, chat_id, job_id


# ===== UPLOAD =====
async def upload_single_file(client: Client, chat_id: int, file_path: str, caption: str, thumb_path: Optional[str] = None):
    msg = await client.send_message(chat_id, "🚀 Uploading...")
    thumb_to_send = thumb_path if thumb_path and os.path.exists(thumb_path) else None
    duration, width, height = get_video_attributes(file_path)
    if duration <= 0:
        duration = 1
    loop = asyncio.get_running_loop()
    start_time = time.time()
    last_edit_time = 0.0
    last_sent_bytes = 0

    async def update_progress(current: int, total: int):
        nonlocal last_edit_time, last_sent_bytes
        current = max(current, last_sent_bytes)
        now = time.time()
        if current != total and now - last_edit_time < 8:
            return
        percentage = (current / total) * 100 if total else 0
        elapsed = max(now - start_time, 0.001)
        speed = current / elapsed
        eta = int((total - current) / speed) if speed > 0 and total >= current else 0
        filled = min(10, int(percentage // 10))
        progress_bar = f"[{'■' * filled}{'□' * (10 - filled)}]"
        text = (
            "🚀 Uploading...\n"
            f"**{progress_bar}** **{percentage:.2f}%**\n"
            f"📦 `{current / 1024 / 1024:.2f} MB / {total / 1024 / 1024:.2f} MB`\n"
            f"⚡ Speed: `{speed / 1024:.2f} KB/s`\n"
            f"⏱️ ETA: `{str(timedelta(seconds=eta))}`"
        )
        try:
            await msg.edit_text(text)
            last_edit_time = now
            last_sent_bytes = current
        except (FloodWait, MessageNotModified):
            pass
        except Exception:
            pass

    try:
        progress_cb = lambda current, total: asyncio.run_coroutine_threadsafe(update_progress(current, total), loop)
        if width > 0 and height > 0:
            await client.send_video(
                chat_id=chat_id,
                video=file_path,
                caption=caption,
                thumb=thumb_to_send,
                duration=duration,
                width=width,
                height=height,
                supports_streaming=True,
                progress=progress_cb,
            )
        else:
            await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption,
                progress=progress_cb,
            )
    finally:
        with contextlib.suppress(Exception):
            await msg.delete()
        cleanup_paths(thumb_path)
        if AUTO_DELETE_AFTER_UPLOAD:
            cleanup_paths(file_path)


async def upload_file_with_progress(client: Client, chat_id: int, file_path: str, caption: str, thumb_path: Optional[str] = None):
    try:
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            await client.send_message(chat_id, "❌ Upload failed: File is empty or missing.")
            cleanup_paths(thumb_path, file_path)
            return

        if os.path.getsize(file_path) > MAX_FILE_SIZE:
            status = await client.send_message(chat_id, "🔄 File is larger than Telegram limit. Splitting...")
            split_files = split_video_by_size(file_path, MAX_FILE_SIZE, os.path.dirname(file_path))
            with contextlib.suppress(Exception):
                await status.delete()
            if len(split_files) == 1 and split_files[0] == file_path:
                await client.send_message(chat_id, "❌ Splitting failed, so the file was not uploaded.")
                cleanup_paths(thumb_path, file_path)
                return

            for i, part_file in enumerate(split_files, start=1):
                part_thumb = generate_thumbnail(part_file)
                part_caption = f"Part {i} of {len(split_files)}\n{caption}"
                await upload_single_file(client, chat_id, part_file, part_caption, part_thumb)
                await asyncio.sleep(2)
            cleanup_paths(thumb_path)
            return

        await upload_single_file(client, chat_id, file_path, caption, thumb_path)
    except Exception as exc:
        logger.exception("Upload failed for %s: %s", file_path, exc)
        await client.send_message(chat_id, f"❌ Upload failed: `{str(exc)[:600]}`", parse_mode=enums.ParseMode.MARKDOWN)
        cleanup_paths(thumb_path, file_path)


# ===== RECORDING =====
async def execute_ffmpeg_recording(chat_id: int, job_id: str, m3u8_url: str, duration: int, temp_ts_path: str, video_track: Optional[Dict[str, Any]], audio_maps: List[str], stderr_path: str) -> Dict[str, Any]:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "warning",
        "-user_agent",
        HTTP_USER_AGENT,
        "-rw_timeout",
        "30000000",
        "-http_persistent",
        "0",
        "-reconnect",
        "1",
        "-reconnect_streamed",
        "1",
        "-reconnect_at_eof",
        "1",
        "-reconnect_on_network_error",
        "1",
        "-reconnect_on_http_error",
        "4xx,5xx",
        "-reconnect_delay_max",
        "10",
        "-fflags",
        "+genpts+discardcorrupt",
        "-i",
        m3u8_url,
    ]

    if video_track:
        cmd.extend(["-map", f"0:{video_track['index']}", "-c:v", "copy"])
    else:
        cmd.append("-vn")

    if audio_maps:
        for idx in audio_maps:
            cmd.extend(["-map", f"0:{idx}"])
        cmd.extend(["-c:a", "copy"])
    else:
        cmd.append("-an")

    cmd.extend([
        "-max_muxing_queue_size",
        "4096",
        "-t",
        str(duration),
        "-f",
        "mpegts",
        temp_ts_path,
    ])

    logger.info("Starting FFmpeg job %s for chat %s", job_id, chat_id)
    stderr_handle = open(stderr_path, "w", encoding="utf-8")
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=stderr_handle)
        running_jobs[job_id] = proc
        while True:
            state = pending_states.get(chat_id, {}).get(job_id)
            if not state:
                break
            if state.get("cancelled"):
                with contextlib.suppress(ProcessLookupError):
                    proc.terminate()
                break
            if proc.returncode is not None:
                break
            await asyncio.sleep(2)
        await proc.wait()
    finally:
        stderr_handle.close()
        running_jobs.pop(job_id, None)

    stderr_text = ""
    if os.path.exists(stderr_path):
        with contextlib.suppress(Exception):
            stderr_text = Path(stderr_path).read_text(encoding="utf-8", errors="ignore")

    state = pending_states.get(chat_id, {}).get(job_id, {})
    cancelled = bool(state.get("cancelled"))
    exists = os.path.exists(temp_ts_path)
    size = os.path.getsize(temp_ts_path) if exists else 0
    recorded_duration = get_duration(temp_ts_path) if exists and size > 0 else 0
    min_duration = minimum_acceptable_duration(duration)

    ok = (
        not cancelled
        and proc.returncode == 0
        and exists
        and size > 1024 * 1024
        and recorded_duration >= min_duration
    )

    if exists and recorded_duration < min_duration:
        logger.warning(
            "Rejecting incomplete recording for job %s: got %ss, expected about %ss",
            job_id,
            recorded_duration,
            duration,
        )

    return {
        "ok": ok,
        "cancelled": cancelled,
        "returncode": proc.returncode,
        "stderr": stderr_text[-2000:],
        "recorded_duration": recorded_duration,
        "min_duration": min_duration,
        "size": size,
    }


async def worker() -> None:
    while True:
        job = await job_queue.get()
        msg = None
        work_dir = None
        temp_ts_path = None
        final_output_path = None
        thumb_path = None
        try:
            chat_id = job["chat_id"]
            job_id = job["job_id"]
            msg = await app.get_messages(chat_id, message_ids=job["msg_id"])
            paths = build_job_paths(chat_id, job_id, job["filename"])
            work_dir = paths["work_dir"]
            temp_ts_path = paths["temp_ts_path"]
            final_output_path = paths["final_path"]
            stderr_path = paths["stderr_path"]
            state = pending_states.get(chat_id, {}).get(job_id)
            if not state or state.get("cancelled"):
                continue

            state["started"] = True
            start_dt = datetime.now()
            await msg.edit_text("✅ Job started! Recording in progress...")

            result = await execute_ffmpeg_recording(
                chat_id=chat_id,
                job_id=job_id,
                m3u8_url=job["url"],
                duration=job["duration"],
                temp_ts_path=temp_ts_path,
                video_track=job["video"],
                audio_maps=job["audios"],
                stderr_path=stderr_path,
            )

            if result["cancelled"]:
                await msg.edit_text("❌ Recording stopped. Partial file has been deleted.")
                continue

            if not result["ok"]:
                await msg.edit_text(
                    "❌ Job failed.\n\n"
                    "Possible causes:\n"
                    "- Stream URL expired or blocked\n"
                    "- Network interrupted the stream\n"
                    "- Recording ended too early, so the bot rejected the incomplete file\n\n"
                    f"Recorded: `{result['recorded_duration']}s`\n"
                    f"Needed at least: `{result['min_duration']}s`\n\n"
                    f"Log:\n`{(result['stderr'] or 'No FFmpeg log available.')[-800:]}`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
                continue

            await msg.edit_text("🔧 Post-processing recording...")
            if not convert_ts_to_mp4(temp_ts_path, final_output_path):
                await msg.edit_text("❌ Recording finished, but MP4 conversion failed. File was deleted.")
                continue

            converted_duration, width, height = get_video_attributes(final_output_path)
            minimum_after_convert = minimum_acceptable_duration(job["duration"])
            if converted_duration < minimum_after_convert:
                await msg.edit_text(
                    "❌ Recording was shorter than expected after conversion, so it was deleted instead of uploading.\n\n"
                    f"Recorded: `{converted_duration}s`\nNeeded at least: `{minimum_after_convert}s`",
                    parse_mode=enums.ParseMode.MARKDOWN,
                )
                continue

            end_dt = datetime.now()
            duration_str = str(timedelta(seconds=int(converted_duration or job["duration"])))
            video = job.get("video")
            w = width or (video["width"] if video else 0)
            h = height or (video["height"] if video else 0)
            resolution_text = f" ({w}x{h})" if w and h else ""
            filename_base = os.path.basename(final_output_path)
            caption = (
                f"**📺 Stream Recording:**{resolution_text}\n\n"
                f"**📁 File Name:** `{filename_base}`\n"
                f"**🗓️ Date:** `{start_dt.strftime('%d %B %Y')}`\n"
                f"**⏰ Recorded:** `{start_dt.strftime('%I:%M %p')}` - `{end_dt.strftime('%I:%M %p')}`\n"
                f"**⏱ Duration:** `{duration_str}`\n\n"
                f"**{BRANDING}**"
            )
            thumb_path = generate_thumbnail(final_output_path)
            await msg.edit_text("📤 Uploading finished recording...")
            await upload_file_with_progress(app, chat_id, final_output_path, caption, thumb_path)
            with contextlib.suppress(Exception):
                await msg.edit_text("✅ Upload complete. Local file deleted.")
        except Exception as exc:
            logger.exception("Worker failed on job %s: %s", job.get("job_id"), exc)
            if msg:
                with contextlib.suppress(Exception):
                    await msg.edit_text("❌ Job failed due to an internal error.")
        finally:
            job_id = job.get("job_id")
            chat_id = job.get("chat_id")
            if chat_id in pending_states and job_id in pending_states[chat_id]:
                pending_states[chat_id][job_id]["completed"] = True
                pending_states[chat_id].pop(job_id, None)
                if not pending_states[chat_id]:
                    pending_states.pop(chat_id, None)
            cleanup_paths(thumb_path, temp_ts_path, final_output_path, work_dir)
            job_queue.task_done()


# ===== COMMANDS =====
@app.on_message(filters.command("start") & (filters.private | filters.group))
async def start_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    await message.reply_text(
        "👋 Bot Online!\n\n"
        "**Commands:**\n"
        "`/record <duration> <m3u8_url> [file_name] [--audio-only] [--video-only]`\n"
        "Example: `/record 5m https://example.com/live.m3u8 my_show.mp4`\n"
        "`/cancel` - Cancel your active job\n"
        "`/status` - Check bot status\n"
        "`/list` - Show any leftover local files\n"
        "`/delete <file_name>` - Delete a leftover local file\n"
        "`/reupload <file_name>` - Reupload a leftover local file\n\n"
        "Uploaded recordings are auto-deleted from the server after upload.",
        parse_mode=enums.ParseMode.MARKDOWN,
    )


@app.on_message(filters.command("record") & (filters.private | filters.group))
async def record_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    if not message.from_user:
        return

    if user_active_job_count(message.from_user.id) >= MAX_JOBS_PER_USER and not is_admin(message.from_user.id):
        await message.reply_text(f"❌ You already have {MAX_JOBS_PER_USER} active job(s). Wait for one to finish first.")
        return

    parts = message.text.split()
    if len(parts) < 3:
        await message.reply_text(
            "Usage: `/record <duration> <m3u8_url> [file_name] [--audio-only] [--video-only]`\n\n"
            "Example: `/record 5m https://example.com/live.m3u8 my_show.mp4`",
            parse_mode=enums.ParseMode.MARKDOWN,
        )
        return

    duration = parse_duration_to_seconds(parts[1])
    if not duration or duration <= 0:
        await message.reply_text("❌ Invalid duration. Use formats like `60`, `30s`, `5m`, `2h`.", parse_mode=enums.ParseMode.MARKDOWN)
        return

    m3u8_url = parts[2].strip()
    if not re.match(r"^https?://", m3u8_url, re.IGNORECASE):
        await message.reply_text("❌ Please send a valid http/https stream URL.")
        return

    audio_only = "--audio-only" in parts
    video_only = "--video-only" in parts
    if audio_only and video_only:
        await message.reply_text("❌ Use only one mode: `--audio-only` or `--video-only`.", parse_mode=enums.ParseMode.MARKDOWN)
        return

    filename_parts = [p for p in parts[3:] if not p.startswith("--")]
    default_name = f"record_{message.chat.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4"
    filename_mp4 = sanitize_filename(" ".join(filename_parts), default_name) if filename_parts else default_name

    job_id = uuid4().hex[:12]
    msg = await message.reply_text("🎬 Recording job initialized.\n🔍 Fetching stream info...")

    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v",
            "error",
            "-show_streams",
            "-print_format",
            "json",
            "-user_agent",
            HTTP_USER_AGENT,
            "-rw_timeout",
            "15000000",
            m3u8_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(proc.communicate(), timeout=25)
        if proc.returncode != 0:
            raise RuntimeError(err.decode(errors="ignore") or "ffprobe failed")
        info = json.loads(out.decode(errors="ignore") or "{}")
    except asyncio.TimeoutError:
        await msg.edit_text("❌ Stream probe timed out. The server did not respond in time.")
        return
    except Exception as exc:
        await msg.edit_text(f"❌ Could not read stream info:\n`{str(exc)[:800]}`", parse_mode=enums.ParseMode.MARKDOWN)
        return

    videos: List[Dict[str, Any]] = []
    audios: List[Dict[str, Any]] = []
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "video" and stream.get("width") and stream.get("height"):
            videos.append({"index": stream["index"], "width": stream["width"], "height": stream["height"]})
        elif stream.get("codec_type") == "audio":
            lang = (stream.get("tags") or {}).get("language") or "Unknown"
            audios.append({"index": stream["index"], "language": lang})

    pending_states.setdefault(message.chat.id, {})[job_id] = {
        "msg_id": msg.id,
        "cancelled": False,
        "completed": False,
        "url": m3u8_url,
        "duration": duration,
        "filename": filename_mp4,
        "videos": videos,
        "audios": audios,
        "selected_video": None,
        "selected_audios": [],
        "audio_only": audio_only,
        "video_only": video_only,
        "starter_user_id": message.from_user.id,
    }

    if audio_only:
        if not audios:
            await msg.edit_text("❌ No audio tracks found for `--audio-only`.", parse_mode=enums.ParseMode.MARKDOWN)
            pending_states[message.chat.id].pop(job_id, None)
            return
        state = pending_states[message.chat.id][job_id]
        state["selected_video"] = None
        state["selected_audios"] = [str(a["index"]) for a in audios]
        await queue_recording_job(message.chat.id, job_id)
        audio_names = ", ".join(a["language"] for a in audios)
        await msg.edit_text(
            f"✅ Audio-only job added to queue!\nAudios: {audio_names}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{message.chat.id}|{job_id}")]]
            ),
        )
        return

    if video_only:
        if not videos:
            await msg.edit_text("❌ No video tracks found for `--video-only`.", parse_mode=enums.ParseMode.MARKDOWN)
            pending_states[message.chat.id].pop(job_id, None)
            return
        state = pending_states[message.chat.id][job_id]
        state["selected_video"] = videos[0]
        state["selected_audios"] = []
        await queue_recording_job(message.chat.id, job_id)
        await msg.edit_text(
            f"✅ Video-only job added to queue!\nVideo: {videos[0]['width']}x{videos[0]['height']}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{message.chat.id}|{job_id}")]]
            ),
        )
        return

    if not videos:
        await msg.edit_text("❌ No video tracks found in this stream.")
        pending_states[message.chat.id].pop(job_id, None)
        return

    buttons = [
        [
            InlineKeyboardButton(
                f"🎥 {v['width']}x{v['height']} (v{v['index']})",
                callback_data=f"vidsel|{message.chat.id}|{job_id}|{v['index']}",
            )
        ]
        for v in videos[:20]
    ]
    buttons.append([InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{message.chat.id}|{job_id}")])
    await msg.edit_text(f"Select video track for Job #{job_id}:", reply_markup=InlineKeyboardMarkup(buttons))


async def queue_recording_job(chat_id: int, job_id: str):
    state = pending_states.get(chat_id, {}).get(job_id)
    if not state:
        return
    job_details = {
        "chat_id": chat_id,
        "job_id": job_id,
        "url": state["url"],
        "duration": state["duration"],
        "filename": state["filename"],
        "video": state["selected_video"],
        "audios": state["selected_audios"],
        "msg_id": state["msg_id"],
    }
    await job_queue.put(job_details)


@app.on_callback_query(filters.regex(r"^vidsel\|"))
async def callback_video(client, callback_query):
    state, chat_id, job_id = await get_state_from_callback(callback_query)
    if not state:
        return
    parts = callback_query.data.split("|")
    if len(parts) != 4:
        await callback_query.answer("Invalid selection.", show_alert=True)
        return
    idx_str = parts[3]
    state["selected_video"] = next((v for v in state["videos"] if str(v["index"]) == idx_str), None)
    if not state["selected_video"]:
        await callback_query.answer("Invalid video selection.", show_alert=True)
        return

    if state["audios"] and not state["selected_audios"]:
        state["selected_audios"] = [str(state["audios"][0]["index"])]

    buttons = []
    if not state["audios"]:
        buttons.append([InlineKeyboardButton("✅ Start Recording", callback_data=f"done|{chat_id}|{job_id}")])
        buttons.append([InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")])
        await callback_query.message.edit_text(
            f"Selected Video: {state['selected_video']['width']}x{state['selected_video']['height']}\n\nNo audio tracks found. Proceed with video only?",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        await callback_query.answer("Video selected.")
        return

    for a in state["audios"][:20]:
        chosen = str(a["index"]) in state["selected_audios"]
        emoji = "✅" if chosen else "🔊"
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {a.get('language') or 'Unknown'} (a{a['index']})",
                callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}",
            )
        ])
    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])
    buttons.append([InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")])
    await callback_query.message.edit_text(
        f"Selected Video: {state['selected_video']['width']}x{state['selected_video']['height']}\n"
        f"Select audio tracks for Job #{job_id}. Tap ✅ Done when finished.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    await callback_query.answer("Video selected.")


@app.on_callback_query(filters.regex(r"^audsel\|"))
async def callback_audio(client, callback_query):
    state, chat_id, job_id = await get_state_from_callback(callback_query)
    if not state:
        return
    parts = callback_query.data.split("|")
    if len(parts) != 4:
        await callback_query.answer("Invalid selection.", show_alert=True)
        return
    idx_str = parts[3]
    if idx_str in state["selected_audios"]:
        state["selected_audios"].remove(idx_str)
    else:
        state["selected_audios"].append(idx_str)

    buttons = []
    for a in state["audios"][:20]:
        chosen = str(a["index"]) in state["selected_audios"]
        emoji = "✅" if chosen else "🔊"
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {a.get('language') or 'Unknown'} (a{a['index']})",
                callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}",
            )
        ])
    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])
    buttons.append([InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")])

    await callback_query.message.edit_text(
        f"Selected Video: {state['selected_video']['width']}x{state['selected_video']['height']}\n"
        f"Select audio tracks for Job #{job_id}. Tap ✅ Done when finished.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    await callback_query.answer("Audio selection updated.")


@app.on_callback_query(filters.regex(r"^done\|"))
async def callback_done(client, callback_query):
    state, chat_id, job_id = await get_state_from_callback(callback_query)
    if not state:
        return
    if state.get("queued"):
        await callback_query.answer("This job is already queued.", show_alert=True)
        return

    video = state.get("selected_video")
    audio_maps = state.get("selected_audios", [])
    state["queued"] = True
    await queue_recording_job(chat_id, job_id)

    audio_names = [a.get("language") or "Unknown" for a in state["audios"] if str(a["index"]) in audio_maps]
    if not audio_names:
        audio_names = ["None"]
    video_info = f"Video: {video['width']}x{video['height']}" if video else "Video: None"

    await callback_query.message.edit_text(
        f"✅ Recording Job #{job_id} added to queue!\n"
        f"{video_info}\n"
        f"Audios: {', '.join(audio_names)}\n"
        f"Mode: Direct Stream Copy\n\n"
        "Your recording will start soon.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
        ),
    )
    await callback_query.answer("Queued.")


@app.on_callback_query(filters.regex(r"^cancel\|"))
async def callback_cancel(client, callback_query):
    state, chat_id, job_id = await get_state_from_callback(callback_query)
    if not state:
        return

    state["cancelled"] = True
    await callback_query.answer("Cancelling...", show_alert=False)

    proc = running_jobs.get(job_id)
    if proc:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=8)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            with contextlib.suppress(Exception):
                await proc.wait()
        await callback_query.message.edit_text("❌ Recording stopped. Partial file will be deleted.")
        return

    removed = False
    async with queue_lock:
        items = []
        while not job_queue.empty():
            item = await job_queue.get()
            if item.get("job_id") == job_id:
                removed = True
                job_queue.task_done()
                continue
            items.append(item)
            job_queue.task_done()
        for item in items:
            await job_queue.put(item)

    pending_states.get(chat_id, {}).pop(job_id, None)
    if not pending_states.get(chat_id):
        pending_states.pop(chat_id, None)

    if removed:
        await callback_query.message.edit_text("❌ Recording job was removed from queue.")
    else:
        await callback_query.message.edit_text("❌ Job cancelled.")


@app.on_message(filters.command("cancel") & (filters.private | filters.group))
async def cancel_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    if not message.from_user:
        return

    buttons = []
    user_id = message.from_user.id
    for chat_id, jobs in pending_states.items():
        for job_id, state in jobs.items():
            if state.get("starter_user_id") != user_id and not is_admin(user_id):
                continue
            label = "Running" if job_id in running_jobs else "Queued"
            buttons.append([
                InlineKeyboardButton(
                    f"🎬 Cancel {label} #{job_id[:8]}",
                    callback_data=f"cancel|{chat_id}|{job_id}",
                )
            ])

    if not buttons:
        await message.reply_text("❌ No active jobs to cancel.")
        return
    await message.reply_text("Select a job to cancel:", reply_markup=InlineKeyboardMarkup(buttons[:30]))


@app.on_message(filters.command("status") & (filters.private | filters.group))
async def status_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    queue_size = job_queue.qsize()
    running_size = len(running_jobs)
    if queue_size == 0 and running_size == 0:
        await message.reply_text("✅ Bot is idle. No jobs are currently running or queued.")
        return

    text = "📊 **Bot Status**\n\n"
    text += f"🎬 **Running Jobs:** {running_size}\n"
    text += f"🕰️ **Jobs in Queue:** {queue_size}\n\n"
    if running_jobs:
        text += "**Running Job IDs:**\n"
        for job_id in running_jobs.keys():
            text += f"`{job_id}`\n"
    await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("approve") & filters.private)
async def approve_handler(client, message):
    if not await ensure_owner(message):
        return
    try:
        user_id = int(message.command[1])
        rc = get_redis_client()
        if rc:
            rc.sadd("approved_users", str(user_id))
        APPROVED_USERS.add(user_id)
        await message.reply_text(f"✅ User `{user_id}` has been approved.", parse_mode=enums.ParseMode.MARKDOWN)
    except (IndexError, ValueError):
        await message.reply_text("Usage: `/approve <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("unapprove") & filters.private)
async def unapprove_handler(client, message):
    if not await ensure_owner(message):
        return
    try:
        user_id = int(message.command[1])
        if user_id in OWNER_IDS:
            await message.reply_text("🚫 You cannot unapprove an owner.")
            return
        rc = get_redis_client()
        if rc:
            rc.srem("approved_users", str(user_id))
        APPROVED_USERS.discard(user_id)
        await message.reply_text(f"❌ User `{user_id}` has been unapproved.", parse_mode=enums.ParseMode.MARKDOWN)
    except (IndexError, ValueError):
        await message.reply_text("Usage: `/unapprove <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command(["ban", "unban", "addadmin", "removeadmin", "disapprove"]) & filters.private)
async def admin_handler(client, message):
    if not message.from_user or not is_admin(message.from_user.id):
        return
    rc = get_redis_client()
    if not rc:
        await message.reply_text("❌ Redis is not configured, so admin storage is unavailable.")
        return
    try:
        cmd = message.command[0].lower()
        uid = str(int(message.command[1]))
        if cmd == "ban":
            rc.sadd("banned_users", uid)
        elif cmd == "unban":
            rc.srem("banned_users", uid)
        elif cmd == "addadmin":
            rc.sadd("bot_admins", uid)
        elif cmd == "removeadmin":
            rc.srem("bot_admins", uid)
        elif cmd == "disapprove":
            rc.srem("approved_users", uid)
            APPROVED_USERS.discard(int(uid))
        await message.reply_text(f"✅ Action `{cmd}` successful for `{uid}`.", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception:
        await message.reply_text("Usage: `/command <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("list") & (filters.private | filters.group))
async def list_files_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    files = []
    for root, _, filenames in os.walk(OUTPUT_DIR):
        for file_name in filenames:
            if file_name.lower().endswith(".mp4"):
                full_path = os.path.join(root, file_name)
                files.append((full_path, os.path.getmtime(full_path)))
    if not files:
        await message.reply_text("✅ No stored recordings found. Uploaded files are auto-deleted.")
        return
    files.sort(key=lambda x: x[1], reverse=True)
    text = "**📂 Leftover Local Files:**\n\n"
    for full_path, _ in files[:20]:
        text += f"`{os.path.relpath(full_path, OUTPUT_DIR)}`\n"
    await message.reply_text(text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("reupload") & (filters.private | filters.group))
async def reupload_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    file_name = " ".join(message.command[1:]).strip()
    if not file_name:
        await message.reply_text("Usage: `/reupload <file_name>`", parse_mode=enums.ParseMode.MARKDOWN)
        return
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if not os.path.exists(file_path):
        await message.reply_text("❌ File not found on server.")
        return

    status = await message.reply_text(f"🔄 Processing `{file_name}` for upload...", parse_mode=enums.ParseMode.MARKDOWN)
    fix_video_metadata(file_path)
    duration_secs, width, height = get_video_attributes(file_path)
    caption = (
        f"**📺 Stream Recording (Re-upload):**\n\n"
        f"**📁 File Name:** `{os.path.basename(file_path)}`\n"
        f"**⏱ Duration:** `{str(timedelta(seconds=duration_secs)) if duration_secs else 'Unknown'}`\n"
        f"**📏 Res:** `{width}x{height}`\n\n"
        f"**{BRANDING}**"
    )
    thumb_path = generate_thumbnail(file_path)
    with contextlib.suppress(Exception):
        await status.delete()
    await upload_file_with_progress(client, message.chat.id, file_path, caption, thumb_path)


@app.on_message(filters.command("delete") & (filters.private | filters.group))
async def delete_file_handler(client, message):
    if not await ensure_user_allowed(message):
        return
    file_name = " ".join(message.command[1:]).strip()
    if not file_name:
        await message.reply_text("Usage: `/delete <file_name>`", parse_mode=enums.ParseMode.MARKDOWN)
        return
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(file_path):
        cleanup_paths(file_path)
        await message.reply_text(f"✅ File `{file_name}` has been deleted.", parse_mode=enums.ParseMode.MARKDOWN)
    else:
        await message.reply_text(f"❌ File `{file_name}` not found.", parse_mode=enums.ParseMode.MARKDOWN)



# ===== APP LIFECYCLE =====
async def main() -> None:
    load_approved_users()
    await app.start()
    logger.info("Bot started")

    for _ in range(max(1, NUM_WORKERS)):
        worker_tasks.append(asyncio.create_task(worker()))

    for owner_id in OWNER_IDS:
        with contextlib.suppress(Exception):
            await app.send_message(owner_id, "Pyrogram bot is running.")

    await idle()

    for task in worker_tasks:
        task.cancel()
    for task in worker_tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await task

    await app.stop()
    logger.info("Bot stopped")


if __name__ == "__main__":
    if not REDIS_URL:
        logger.warning("REDIS_URL is not set. Persistence will be limited to runtime memory.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrupted and shutting down.")
