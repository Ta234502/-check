import os
import json
import shlex
import asyncio
import subprocess
import re
import signal
import time
import hashlib
import shutil
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional, Tuple

from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image
import redis

# ===== CONFIG =====
API_ID = int(os.environ.get("API_ID", "12345678"))
API_HASH = os.environ.get("API_HASH", "humeshasukhirhoadcdegaidurijdrj284")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "Mera Bot Token Mst Dhundho Apna Dalo")
REDIS_URL = os.environ.get("REDIS_URL")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "pyrogram_recordings")
LOG_FILE = os.environ.get("BOT_LOG_FILE", "bot_activity.log")
FSUB_CHANNEL = os.environ.get("FSUB_CHANNEL", "").strip()
BRANDING = os.environ.get("BRANDING", "")

OWNER_IDS = [
    int(x.strip())
    for x in os.environ.get("OWNER_IDS", "").split(",")
    if x.strip().isdigit()
]

MAX_FILE_SIZE = int(float(os.environ.get("MAX_FILE_SIZE_GB", "1.9")) * 1024 * 1024 * 1024)
MAX_JOBS_PER_USER = int(os.environ.get("MAX_JOBS_PER_USER", "2"))
MAX_RETRY_ATTEMPTS = int(os.environ.get("MAX_RETRY_ATTEMPTS", "3"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "3"))
UPLOAD_EDIT_INTERVAL = int(os.environ.get("UPLOAD_EDIT_INTERVAL", "6"))
PROGRESS_EDIT_INTERVAL = int(os.environ.get("PROGRESS_EDIT_INTERVAL", "8"))
MIN_SUCCESS_RATIO = float(os.environ.get("MIN_SUCCESS_RATIO", "0.90"))
MIN_SUCCESS_GRACE_SECONDS = int(os.environ.get("MIN_SUCCESS_GRACE_SECONDS", "5"))

DEFAULT_USER_AGENT = os.environ.get(
    "HTTP_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== CLIENT =====
app = Client(
    "my_pyrogram_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    max_concurrent_transmissions=6,
)

# ===== GLOBAL STATE =====
pending_states: Dict[int, Dict[str, dict]] = {}
job_queue: asyncio.Queue = asyncio.Queue()
running_jobs: Dict[str, asyncio.subprocess.Process] = {}
active_uploads: Dict[str, dict] = {}
file_token_map: Dict[str, str] = {}
BOT_LOOP = None
FILE_TOKEN_TTL = int(os.environ.get("FILE_TOKEN_TTL", "604800"))

redis_client = None
APPROVED_USERS = set()
ADMIN_USERS = set()
BANNED_USERS = set()
FILE_OWNER_MAP: Dict[str, int] = {}


# ===== HELPERS =====

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write_log(line: str) -> None:
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{now_str()}] {line}\n")
    except Exception:
        pass


async def owner_log(line: str, notify: bool = False) -> None:
    write_log(line)
    if not notify:
        return
    for owner_id in OWNER_IDS:
        try:
            await app.send_message(owner_id, f"`{line}`", parse_mode=enums.ParseMode.MARKDOWN)
        except Exception:
            pass


def get_redis():
    global redis_client
    if redis_client is None and REDIS_URL:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        except Exception as e:
            write_log(f"Redis init failed: {e}")
            redis_client = None
    return redis_client


def refresh_user_sets() -> None:
    global APPROVED_USERS, ADMIN_USERS, BANNED_USERS
    APPROVED_USERS = set(OWNER_IDS)
    ADMIN_USERS = set(OWNER_IDS)
    BANNED_USERS = set()

    r = get_redis()
    if not r:
        return

    try:
        r.sadd("approved_users", *[str(x) for x in OWNER_IDS])
        r.sadd("admin_users", *[str(x) for x in OWNER_IDS])
        APPROVED_USERS |= {int(x) for x in r.smembers("approved_users") if str(x).isdigit()}
        ADMIN_USERS |= {int(x) for x in r.smembers("admin_users") if str(x).isdigit()}
        BANNED_USERS |= {int(x) for x in r.smembers("banned_users") if str(x).isdigit()}
        APPROVED_USERS -= BANNED_USERS
        ADMIN_USERS -= BANNED_USERS

        try:
            raw_owner_map = r.hgetall("file_owner_map") or {}
            FILE_OWNER_MAP.clear()
            for k, v in raw_owner_map.items():
                if str(v).isdigit():
                    FILE_OWNER_MAP[k] = int(v)
        except Exception as e:
            write_log(f"Redis file owner refresh failed: {e}")
    except Exception as e:
        write_log(f"Redis refresh failed: {e}")


async def redis_set_add(name: str, user_id: int) -> None:
    r = get_redis()
    if not r:
        return
    try:
        r.sadd(name, str(user_id))
    except Exception as e:
        write_log(f"Redis sadd failed for {name}:{user_id}: {e}")


async def redis_set_remove(name: str, user_id: int) -> None:
    r = get_redis()
    if not r:
        return
    try:
        r.srem(name, str(user_id))
    except Exception as e:
        write_log(f"Redis srem failed for {name}:{user_id}: {e}")


def record_file_owner(file_name: str, user_id: int) -> None:
    if not file_name:
        return
    FILE_OWNER_MAP[file_name] = int(user_id)
    r = get_redis()
    if not r:
        return
    try:
        r.hset("file_owner_map", file_name, str(user_id))
    except Exception as e:
        write_log(f"Redis hset failed for file owner {file_name}:{user_id}: {e}")


def get_file_owner(file_name: str) -> Optional[int]:
    if file_name in FILE_OWNER_MAP:
        return FILE_OWNER_MAP[file_name]
    r = get_redis()
    if not r:
        return None
    try:
        owner = r.hget("file_owner_map", file_name)
        if owner and str(owner).isdigit():
            owner_id = int(owner)
            FILE_OWNER_MAP[file_name] = owner_id
            return owner_id
    except Exception as e:
        write_log(f"Redis hget failed for file owner {file_name}: {e}")
    return None


def can_access_file(user_id: int, file_name: str) -> bool:
    if is_admin_user(user_id):
        return True
    owner_id = get_file_owner(file_name)
    if owner_id is None:
        return False
    return owner_id == user_id


async def safe_edit(message, text: str, reply_markup=None, parse_mode=None):
    try:
        return await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except MessageNotModified:
        return None
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            return await message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            return None
    except Exception:
        return None


async def safe_reply(message, text: str, reply_markup=None, parse_mode=None):
    try:
        return await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        try:
            return await message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
        except Exception:
            return None
    except Exception:
        return None


async def safe_answer(callback_query, text: str, show_alert: bool = False):
    try:
        await callback_query.answer(text, show_alert=show_alert)
    except Exception:
        pass


async def get_or_create_progress_message(chat_id: int, msg_id: Optional[int], default_text: str):
    if msg_id:
        try:
            return await app.get_messages(chat_id, message_ids=msg_id)
        except Exception:
            pass
    return await app.send_message(chat_id, default_text)


async def check_force_sub(user_id: int) -> bool:
    if not FSUB_CHANNEL:
        return True
    if user_id in OWNER_IDS:
        return True
    try:
        member = await app.get_chat_member(FSUB_CHANNEL, user_id)
        status = getattr(member, "status", None)
        return status in {
            enums.ChatMemberStatus.MEMBER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.OWNER,
        }
    except Exception:
        return False


async def force_sub_markup():
    if not FSUB_CHANNEL:
        return None
    channel_url = f"https://t.me/{FSUB_CHANNEL.lstrip('@')}"
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Join Channel", url=channel_url)]]
    )


def validate_runtime() -> None:
    bad_values = {
        "12345678",
        "humeshasukhirhoadcdegaidurijdrj284",
        "Mera Bot Token Mst Dhundho Apna Dalo",
        "",
    }
    missing = []
    if str(API_ID) in bad_values:
        missing.append("API_ID")
    if API_HASH in bad_values:
        missing.append("API_HASH")
    if BOT_TOKEN in bad_values:
        missing.append("BOT_TOKEN")
    if missing:
        raise RuntimeError(f"Missing/placeholder config: {', '.join(missing)}")

    required_bins = ["ffmpeg", "ffprobe"]
    missing_bins = [name for name in required_bins if shutil.which(name) is None]
    if missing_bins:
        raise RuntimeError(f"Missing required binaries: {', '.join(missing_bins)}")


def resolve_file_name_from_token(token: str) -> Optional[str]:
    file_name = file_token_map.get(token)
    if file_name:
        return file_name

    r = get_redis()
    if r:
        try:
            file_name = r.get(f"file_token:{token}")
            if file_name:
                file_token_map[token] = file_name
                return file_name
        except Exception as e:
            write_log(f"Redis token lookup failed for {token}: {e}")

    try:
        for file_name in os.listdir(OUTPUT_DIR):
            if not os.path.isfile(os.path.join(OUTPUT_DIR, file_name)) or file_name.startswith('.'):
                continue
            if hashlib.md5(file_name.encode("utf-8", errors="ignore")).hexdigest()[:12] == token:
                file_token_map[token] = file_name
                return file_name
    except Exception as e:
        write_log(f"Token file scan failed for {token}: {e}")

    return None


def cleanup_job_state(chat_id: int, job_id: str) -> None:
    chat_jobs = pending_states.get(chat_id)
    if not chat_jobs:
        return
    chat_jobs.pop(job_id, None)
    if not chat_jobs:
        pending_states.pop(chat_id, None)


def is_owner_user(user_id: int) -> bool:
    return user_id in OWNER_IDS


def is_admin_user(user_id: int) -> bool:
    return user_id in ADMIN_USERS or user_id in OWNER_IDS


def is_approved_id(user_id: int) -> bool:
    return user_id in APPROVED_USERS or is_admin_user(user_id)


def is_banned_user(user_id: int) -> bool:
    return user_id in BANNED_USERS


def user_running_jobs(user_id: int) -> int:
    count = 0
    for chat_jobs in pending_states.values():
        for state in chat_jobs.values():
            if state.get("starter_user_id") == user_id and not state.get("completed") and not state.get("cancelled"):
                count += 1
    return count


def can_manage_job(user_id: int, state: dict) -> bool:
    if is_admin_user(user_id):
        return True
    return state.get("starter_user_id") == user_id


async def access_gate_message(message, owner_only: bool = False, admin_only: bool = False) -> bool:
    user_id = message.from_user.id if message.from_user else 0

    if is_banned_user(user_id):
        await safe_reply(message, "🚫 You are banned from using this bot.")
        return False

    if owner_only and not is_owner_user(user_id):
        await safe_reply(message, "🚫 Only bot owner can use this command.")
        return False

    if admin_only and not is_admin_user(user_id):
        await safe_reply(message, "🚫 Only bot admin can use this command.")
        return False

    if not owner_only and not admin_only and not is_approved_id(user_id):
        await safe_reply(message, "🚫 You are not approved to use this bot.")
        return False

    if not owner_only and not admin_only and not await check_force_sub(user_id):
        await safe_reply(
            message,
            f"🚫 Join {FSUB_CHANNEL} first, then try again.",
            reply_markup=await force_sub_markup(),
        )
        return False

    return True


def owner_only(func):
    @wraps(func)
    async def wrapper(client, message):
        if not await access_gate_message(message, owner_only=True):
            return
        return await func(client, message)
    return wrapper



def admin_only(func):
    @wraps(func)
    async def wrapper(client, message):
        if not await access_gate_message(message, admin_only=True):
            return
        return await func(client, message)
    return wrapper



def approved_only(func):
    @wraps(func)
    async def wrapper(client, message):
        if not await access_gate_message(message):
            return
        return await func(client, message)
    return wrapper


async def enforce_callback_access(callback_query, job_state: Optional[dict] = None, allow_admin: bool = True) -> bool:
    user_id = callback_query.from_user.id if callback_query.from_user else 0
    if is_banned_user(user_id):
        await safe_answer(callback_query, "🚫 You are banned.", show_alert=True)
        return False

    if not is_approved_id(user_id) and not is_owner_user(user_id):
        await safe_answer(callback_query, "🚫 You are not approved.", show_alert=True)
        return False

    if not await check_force_sub(user_id) and not is_admin_user(user_id):
        await safe_answer(callback_query, f"Join {FSUB_CHANNEL} first.", show_alert=True)
        return False

    if job_state is not None:
        starter_id = job_state.get("starter_user_id")
        if allow_admin and is_admin_user(user_id):
            return True
        if starter_id and starter_id != user_id:
            await safe_answer(callback_query, "🚫 This button belongs to another user.", show_alert=True)
            return False
    return True


# ===== MEDIA / FFPROBE / FFMPEG =====

def parse_duration_to_seconds(duration_str: str) -> int:
    s = duration_str.strip().lower()
    if re.fullmatch(r"\d+", s):
        return int(s)
    if re.fullmatch(r"\d+[smh]", s):
        unit = s[-1]
        val = int(s[:-1])
        return val * {"s": 1, "m": 60, "h": 3600}[unit]
    if re.fullmatch(r"\d{1,2}:\d{1,2}:\d{1,2}", s):
        h, m, sec = map(int, s.split(":"))
        return h * 3600 + m * 60 + sec
    raise ValueError("Invalid duration format")



def sanitize_filename(name: str, default_ext: str = ".mkv") -> str:
    name = name.strip().replace("\n", " ").replace("\r", " ")
    name = os.path.basename(name)
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    if not name:
        name = f"record_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    root, ext = os.path.splitext(name)
    if ext.lower() not in {".mkv", ".mp4"}:
        ext = default_ext
    return f"{root}{ext}"



def safe_output_path(file_name: str) -> str:
    safe_name = sanitize_filename(file_name)
    base, ext = os.path.splitext(safe_name)
    candidate = os.path.join(OUTPUT_DIR, safe_name)
    counter = 1

    active_targets = {
        state.get("filename")
        for chat_jobs in pending_states.values()
        for state in chat_jobs.values()
        if state.get("filename") and not state.get("completed") and not state.get("cancelled")
    }

    while candidate in active_targets or os.path.exists(candidate):
        candidate = os.path.join(OUTPUT_DIR, f"{base}_{counter}{ext}")
        counter += 1
    return candidate



def build_headers_blob(headers: Dict[str, str]) -> str:
    return "".join(f"{k}: {v}\r\n" for k, v in headers.items() if v is not None)



def build_input_args(url: str, headers: Optional[Dict[str, str]] = None) -> List[str]:
    args = [
        "-rw_timeout", "60000000",
        "-thread_queue_size", "4096",
        "-user_agent", DEFAULT_USER_AGENT,
    ]
    headers = headers or {}
    referer = headers.get("Referer") or headers.get("referer")
    if referer:
        args.extend(["-referer", referer])
    header_blob = build_headers_blob(headers)
    if header_blob:
        args.extend(["-headers", header_blob])
    args.extend(["-i", url])
    return args



def get_video_attributes(file_path: str) -> Tuple[int, int, int]:
    try:
        proc = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,duration",
                "-show_entries", "format=duration",
                "-of", "json", file_path,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        output = proc.stdout.decode().strip()
        if not output:
            return 0, 0, 0

        data = json.loads(output)
        width = height = 0
        duration = 0.0

        if data.get("streams"):
            stream = data["streams"][0]
            width = int(stream.get("width") or 0)
            height = int(stream.get("height") or 0)
            try:
                duration = float(stream.get("duration") or 0)
            except Exception:
                duration = 0.0

        if duration <= 0 and data.get("format", {}).get("duration"):
            try:
                duration = float(data["format"]["duration"])
            except Exception:
                duration = 0.0

        return max(int(duration), 0), width, height
    except Exception as e:
        write_log(f"ffprobe attrs failed for {file_path}: {e}")
        return 0, 0, 0



def get_duration(file_path: str) -> int:
    d, _, _ = get_video_attributes(file_path)
    return d



def repair_media_file(file_path: str) -> bool:
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return False
    ext = os.path.splitext(file_path)[1].lower() or ".mkv"
    temp_path = f"{file_path}.repair{ext}"
    try:
        cmd = [
            "ffmpeg", "-y", "-i", file_path,
            "-map", "0", "-c", "copy",
            "-dn", "-sn", "-ignore_unknown",
        ]
        if ext == ".mp4":
            cmd.extend(["-movflags", "+faststart"])
        cmd.append(temp_path)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            os.replace(temp_path, file_path)
            return True
    except Exception as e:
        write_log(f"repair_media_file failed for {file_path}: {e}")
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass
    return False



def generate_thumbnail(video_path: str) -> Optional[str]:
    thumb_path = f"{video_path}.jpg"
    try:
        if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            return None
        duration = get_duration(video_path)
        ss_time = "00:00:05" if duration >= 5 else "00:00:01"
        subprocess.run(
            [
                "ffmpeg", "-y", "-ss", ss_time, "-i", video_path,
                "-frames:v", "1", "-q:v", "2", thumb_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        if os.path.exists(thumb_path):
            Image.open(thumb_path).convert("RGB").save(thumb_path, "JPEG")
            return thumb_path
    except Exception as e:
        write_log(f"thumbnail failed for {video_path}: {e}")
    return None



def split_video_by_size(input_file: str, max_size: int, output_dir: str) -> List[str]:
    try:
        total_duration = get_duration(input_file)
        total_size = os.path.getsize(input_file)
        if total_duration <= 0 or total_size <= max_size:
            return [input_file]

        num_parts = int(total_size / max_size) + 1
        part_duration = max(total_duration // num_parts, 1)
        base_name, ext = os.path.splitext(os.path.basename(input_file))
        split_files = []
        start_time = 0

        for i in range(num_parts):
            output_file = os.path.join(output_dir, f"{base_name}_part{i+1}{ext}")
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time),
                "-i", input_file,
                "-c", "copy",
                "-map", "0",
                "-dn", "-ignore_unknown",
            ]
            if i != num_parts - 1:
                cmd.extend(["-t", str(part_duration)])
            cmd.append(output_file)
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            split_files.append(output_file)
            start_time += part_duration

        if os.path.exists(input_file):
            os.remove(input_file)
        return split_files
    except Exception as e:
        write_log(f"split_video_by_size failed for {input_file}: {e}")
        return [input_file]



def format_seconds(seconds: int) -> str:
    return str(timedelta(seconds=max(int(seconds), 0)))



def make_progress_bar(percentage: float, length: int = 10) -> str:
    percentage = max(0.0, min(100.0, percentage))
    filled = int((percentage / 100.0) * length)
    return f"[{'■' * filled}{'□' * (length - filled)}]"



def file_token_for(file_name: str) -> str:
    token = hashlib.md5(file_name.encode("utf-8", errors="ignore")).hexdigest()[:12]
    file_token_map[token] = file_name
    r = get_redis()
    if r:
        try:
            r.setex(f"file_token:{token}", FILE_TOKEN_TTL, file_name)
        except Exception as e:
            write_log(f"Redis token set failed for {file_name}: {e}")
    return token



def parse_record_command(message_text: str) -> dict:
    tokens = shlex.split(message_text)
    if len(tokens) < 3:
        raise ValueError("Usage: /record <duration> [bitrate] <m3u8_url> [file_name] [--referer URL] [--header 'Key: Value'] [--audio-only|--video-only]")

    tokens = tokens[1:]
    duration = parse_duration_to_seconds(tokens[0])
    idx = 1

    bitrate = None
    if idx < len(tokens) and re.fullmatch(r"\d+[kKmM]", tokens[idx]):
        bitrate = tokens[idx]
        idx += 1

    if idx >= len(tokens):
        raise ValueError("Missing stream URL.")
    url = tokens[idx]
    idx += 1

    file_name_tokens = []
    flags = {
        "audio_only": False,
        "video_only": False,
        "headers": {},
    }

    while idx < len(tokens):
        tok = tokens[idx]
        if tok == "--audio-only":
            flags["audio_only"] = True
            idx += 1
        elif tok == "--video-only":
            flags["video_only"] = True
            idx += 1
        elif tok == "--referer":
            if idx + 1 >= len(tokens):
                raise ValueError("--referer needs a value")
            flags["headers"]["Referer"] = tokens[idx + 1]
            idx += 2
        elif tok == "--header":
            if idx + 1 >= len(tokens):
                raise ValueError("--header needs a value like 'Origin: https://site.com'")
            raw = tokens[idx + 1]
            if ":" not in raw:
                raise ValueError("Header must be like 'Key: Value'")
            k, v = raw.split(":", 1)
            flags["headers"][k.strip()] = v.strip()
            idx += 2
        elif tok.startswith("--"):
            raise ValueError(f"Unknown flag: {tok}")
        else:
            file_name_tokens.append(tok)
            idx += 1

    file_name = " ".join(file_name_tokens).strip() if file_name_tokens else ""
    if not file_name:
        file_name = f"record_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mkv"
    file_name = sanitize_filename(file_name, default_ext=".mkv")

    return {
        "duration": duration,
        "bitrate": bitrate or "copy",
        "url": url,
        "file_name": file_name,
        "headers": flags["headers"],
        "audio_only": flags["audio_only"],
        "video_only": flags["video_only"],
    }


async def ffprobe_streams(url: str, headers: Optional[Dict[str, str]] = None) -> dict:
    cmd = ["ffprobe", "-v", "error", "-show_streams", "-print_format", "json"]
    cmd.extend(build_input_args(url, headers=headers or {}))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(err.decode("utf-8", errors="ignore")[-400:] or "ffprobe failed")
    return json.loads(out.decode("utf-8", errors="ignore") or "{}")



def normalized_quality_label(width: int, height: int) -> str:
    h = int(height or 0)
    if h >= 1000:
        q = "1080p"
    elif h >= 700:
        q = "720p"
    elif h >= 470:
        q = "480p"
    elif h >= 350:
        q = "360p"
    else:
        q = f"{h}p"
    return f"{q} ({width}x{height})"



def min_success_duration(requested: int) -> int:
    if requested <= 0:
        return 0
    ratio_target = int(requested * MIN_SUCCESS_RATIO)
    grace_target = requested - MIN_SUCCESS_GRACE_SECONDS
    target = max(ratio_target, grace_target)
    if requested <= 30:
        target = max(requested - 3, int(requested * 0.85))
    return max(1, min(requested, target))



def finalize_recording(temp_path: str, final_path: str) -> bool:
    try:
        final_ext = os.path.splitext(final_path)[1].lower()
        if final_ext not in {".mkv", ".mp4"}:
            final_path = os.path.splitext(final_path)[0] + ".mkv"
            final_ext = ".mkv"

        if final_ext == ".mkv":
            os.replace(temp_path, final_path)
            return os.path.exists(final_path) and os.path.getsize(final_path) > 0

        cmd = [
            "ffmpeg", "-y",
            "-i", temp_path,
            "-map", "0", "-c", "copy",
            "-dn", "-sn", "-ignore_unknown",
            "-movflags", "+faststart",
            final_path,
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=True)
        return os.path.exists(final_path) and os.path.getsize(final_path) > 0
    except Exception as e:
        write_log(f"finalize_recording failed for {final_path}: {e}")
        return False
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


async def upload_single_file(client, chat_id: int, file_path: str, caption: str, thumb_path: Optional[str] = None,
                             msg_id: Optional[int] = None, cleanup_after: bool = False,
                             owner_user_id: Optional[int] = None):
    msg = await get_or_create_progress_message(chat_id, msg_id, "🚀 Uploading...")
    upload_id = hashlib.md5(f"{chat_id}:{file_path}:{time.time()}".encode()).hexdigest()[:12]
    active_uploads[upload_id] = {
        "chat_id": chat_id,
        "file_path": file_path,
        "cancelled": False,
        "owner_user_id": owner_user_id,
    }

    duration, width, height = await asyncio.to_thread(get_video_attributes, file_path)
    has_video = width > 0 and height > 0
    if duration <= 0:
        duration = 1
    total_size = max(os.path.getsize(file_path), 1)
    thumb_to_send = thumb_path if has_video and thumb_path and os.path.exists(thumb_path) else None

    start_time = time.time()
    last_edit = 0.0

    async def update_progress(current: int, total: int):
        nonlocal last_edit
        upload_state = active_uploads.get(upload_id)
        if not upload_state:
            return
        if upload_state.get("cancelled"):
            try:
                client.stop_transmission()
            except Exception:
                pass
            return

        now = time.time()
        if current != total and now - last_edit < UPLOAD_EDIT_INTERVAL:
            return

        percentage = (current / max(total, 1)) * 100
        elapsed = max(now - start_time, 0.001)
        speed = current / elapsed
        eta = (max(total, 1) - current) / speed if speed > 0 else 0

        text = (
            "🚀 Uploading...\n"
            f"**{make_progress_bar(percentage)}** **{percentage:.2f}%**\n"
            f"📦 `{current/1024/1024:.2f} MB / {max(total,1)/1024/1024:.2f} MB`\n"
            f"⚡ Speed: `{speed/1024/1024:.2f} MB/s`\n"
            f"⏱️ ETA: `{format_seconds(int(eta))}`"
        )
        await safe_edit(
            msg,
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("❌ Cancel Upload", callback_data=f"cancelup|{chat_id}|{upload_id}")]]
            ),
        )
        last_edit = now

    try:
        progress_cb = lambda current, total: asyncio.run_coroutine_threadsafe(update_progress(current, total), BOT_LOOP)
        if has_video:
            result = await client.send_video(
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
            result = await client.send_document(
                chat_id=chat_id,
                document=file_path,
                caption=caption,
                thumb=thumb_to_send,
                progress=progress_cb,
            )
        if active_uploads.get(upload_id, {}).get("cancelled"):
            await safe_edit(msg, "❌ Upload cancelled.")
        else:
            try:
                await msg.delete()
            except Exception:
                pass
            return result
    except Exception as e:
        await safe_edit(msg, f"❌ Upload failed: {e}")
        write_log(f"upload failed for {file_path}: {e}")
    finally:
        active_uploads.pop(upload_id, None)
        if cleanup_after and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
        if thumb_path and os.path.exists(thumb_path):
            try:
                os.remove(thumb_path)
            except Exception:
                pass
    return None


async def upload_file_with_progress(client, chat_id: int, file_path: str, caption: str,
                                    thumb_path: Optional[str] = None, msg_id: Optional[int] = None,
                                    owner_user_id: Optional[int] = None, cleanup_after: bool = False):
    msg = await get_or_create_progress_message(chat_id, msg_id, "🚀 Preparing upload...")

    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        await safe_edit(msg, "❌ Upload failed: file is missing or empty.")
        return

    file_size = os.path.getsize(file_path)
    if file_size <= MAX_FILE_SIZE:
        await upload_single_file(
            client,
            chat_id,
            file_path,
            caption,
            thumb_path=thumb_path,
            msg_id=msg.id,
            owner_user_id=owner_user_id,
            cleanup_after=cleanup_after,
        )
        return

    await safe_edit(msg, "🔄 File is larger than Telegram limit. Splitting into parts...")
    split_files = await asyncio.to_thread(split_video_by_size, file_path, MAX_FILE_SIZE, OUTPUT_DIR)
    if len(split_files) <= 1:
        await safe_edit(msg, "❌ Splitting failed. Uploading original may fail.")
        await upload_single_file(
            client,
            chat_id,
            file_path,
            caption,
            thumb_path=thumb_path,
            msg_id=msg.id,
            owner_user_id=owner_user_id,
            cleanup_after=cleanup_after,
        )
        return

    for i, part_file in enumerate(split_files, start=1):
        part_thumb = await asyncio.to_thread(generate_thumbnail, part_file)
        part_caption = f"Part {i}/{len(split_files)}\n{caption}"
        await upload_single_file(
            client,
            chat_id,
            part_file,
            part_caption,
            thumb_path=part_thumb,
            msg_id=msg.id if i == 1 else None,
            owner_user_id=owner_user_id,
            cleanup_after=True,
        )
        await asyncio.sleep(2)

    if thumb_path and os.path.exists(thumb_path):
        try:
            os.remove(thumb_path)
        except Exception:
            pass


async def execute_ffmpeg_recording(job: dict) -> Tuple[bool, str]:
    chat_id = job["chat_id"]
    job_id = job["job_id"]
    requested_duration = int(job["duration"])
    progress_message = await app.get_messages(chat_id, message_ids=job["msg_id"])
    selected_video = job.get("video")
    selected_audios = job.get("audios") or []
    final_output_path = job["filename"]
    headers = job.get("headers") or {}

    stderr_tail = ""
    target_min_duration = min_success_duration(requested_duration)

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        temp_output = os.path.join(
            OUTPUT_DIR,
            f"{os.path.splitext(os.path.basename(final_output_path))[0]}.{job_id}.attempt{attempt}.mkv",
        )
        if os.path.exists(temp_output):
            try:
                os.remove(temp_output)
            except Exception:
                pass

        cmd = [
            "ffmpeg", "-y", "-hide_banner", "-nostdin", "-loglevel", "info",
            "-fflags", "+genpts+discardcorrupt",
            "-err_detect", "ignore_err",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_at_eof", "1",
            "-reconnect_delay_max", "10",
            "-max_muxing_queue_size", "4096",
        ]
        cmd.extend(build_input_args(job["url"], headers=headers))

        if selected_video:
            cmd.extend(["-map", f"0:{selected_video['index']}"])
            bitrate = str(job.get("bitrate") or "copy").lower()
            if bitrate != "copy":
                numeric_bitrate = str(job.get("bitrate"))
                buffer_size = numeric_bitrate
                m = re.fullmatch(r"(\d+)([kKmM])", numeric_bitrate)
                if m:
                    buffer_size = f"{int(m.group(1)) * 2}{m.group(2)}"
                cmd.extend([
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-pix_fmt", "yuv420p",
                    "-b:v", numeric_bitrate,
                    "-maxrate", numeric_bitrate,
                    "-bufsize", buffer_size,
                ])
            else:
                cmd.extend(["-c:v", "copy"])
        else:
            cmd.append("-vn")

        if selected_audios:
            for audio_idx in selected_audios:
                cmd.extend(["-map", f"0:{audio_idx}"])
            cmd.extend(["-c:a", "copy"])
        else:
            cmd.append("-an")

        if not selected_video and not selected_audios:
            return False, "No stream tracks selected."

        cmd.extend([
            "-dn", "-sn", "-ignore_unknown",
            "-muxdelay", "0", "-muxpreload", "0",
            "-t", str(requested_duration),
            temp_output,
        ])

        await safe_edit(
            progress_message,
            f"🎬 Recording started...\nAttempt `{attempt}/{MAX_RETRY_ATTEMPTS}`\nRequested duration: `{format_seconds(requested_duration)}`",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
            ),
            parse_mode=enums.ParseMode.MARKDOWN,
        )

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        running_jobs[job_id] = proc

        time_regex = re.compile(r"time=([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{2})")
        speed_regex = re.compile(r"speed=\s*([0-9\.x]+)")
        last_update = 0.0
        last_percentage_block = -1
        current_speed = "N/A"
        stderr_lines: List[str] = []

        while True:
            line = await proc.stderr.readline()
            if not line:
                break

            line_str = line.decode("utf-8", errors="ignore").strip()
            if line_str:
                stderr_lines.append(line_str)
                if len(stderr_lines) > 80:
                    stderr_lines = stderr_lines[-80:]

            speed_match = speed_regex.search(line_str)
            if speed_match:
                current_speed = speed_match.group(1)

            match = time_regex.search(line_str)
            if match:
                h, m, s, _ms = map(int, match.groups())
                elapsed = h * 3600 + m * 60 + s
                percentage = (elapsed / requested_duration) * 100 if requested_duration > 0 else 0
                block = int(percentage) // 10
                now = time.time()
                if block > last_percentage_block or now - last_update >= PROGRESS_EDIT_INTERVAL or elapsed >= requested_duration:
                    current_size_mb = 0.0
                    if os.path.exists(temp_output):
                        current_size_mb = os.path.getsize(temp_output) / (1024 * 1024)
                    elapsed_for_speed = max(now - last_update, 1)
                    text = (
                        "🎬 Recording in progress...\n"
                        f"Attempt `{attempt}/{MAX_RETRY_ATTEMPTS}`\n"
                        f"**{make_progress_bar(percentage)}** **{percentage:.2f}%**\n"
                        f"📦 `{current_size_mb:.2f} MB`\n"
                        f"⚡ Stream Speed: `{current_speed}`\n"
                        f"⏱️ Time: `{format_seconds(elapsed)}` / `{format_seconds(requested_duration)}`"
                    )
                    await safe_edit(
                        progress_message,
                        text,
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
                        ),
                        parse_mode=enums.ParseMode.MARKDOWN,
                    )
                    last_update = now
                    last_percentage_block = block

            if pending_states.get(chat_id, {}).get(job_id, {}).get("cancelled"):
                try:
                    proc.send_signal(signal.SIGINT)
                except Exception:
                    pass
                try:
                    await asyncio.wait_for(proc.wait(), timeout=8)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
                break

        await proc.wait()
        running_jobs.pop(job_id, None)
        stderr_tail = "\n".join(stderr_lines)[-2000:]

        if pending_states.get(chat_id, {}).get(job_id, {}).get("cancelled"):
            if os.path.exists(temp_output):
                try:
                    os.remove(temp_output)
                except Exception:
                    pass
            return False, "Recording cancelled by user."

        actual_duration = await asyncio.to_thread(get_duration, temp_output) if os.path.exists(temp_output) else 0
        file_size = os.path.getsize(temp_output) if os.path.exists(temp_output) else 0

        if proc.returncode == 0 and file_size > 500 * 1024 and actual_duration >= target_min_duration:
            if await asyncio.to_thread(finalize_recording, temp_output, final_output_path):
                return True, f"Recorded {actual_duration}s successfully."
            return False, "Recording finished but final container conversion failed."

        reason = (
            f"Attempt {attempt} incomplete. Requested {requested_duration}s, got {actual_duration}s, "
            f"exit={proc.returncode}, size={file_size} bytes."
        )
        await owner_log(f"Job {job_id} unstable: {reason}", notify=False)

        if os.path.exists(temp_output):
            try:
                os.remove(temp_output)
            except Exception:
                pass

        if attempt < MAX_RETRY_ATTEMPTS:
            await safe_edit(
                progress_message,
                "⚠️ Stream became unstable. Retrying automatically...\n"
                f"Attempt `{attempt}/{MAX_RETRY_ATTEMPTS}` failed because the recorded duration was too short.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
                ),
                parse_mode=enums.ParseMode.MARKDOWN,
            )
            await asyncio.sleep(3)

    return False, stderr_tail or "Recording failed after retries."


# ===== WORKER =====
async def worker(worker_id: int):
    while True:
        job = await job_queue.get()
        job_id = job["job_id"]
        chat_id = job["chat_id"]
        msg = None
        try:
            msg = await app.get_messages(chat_id, message_ids=job["msg_id"])
            state = pending_states.setdefault(chat_id, {}).setdefault(job_id, {})
            if state.get("cancelled"):
                if msg:
                    await safe_edit(msg, "❌ Recording cancelled before start.")
                await owner_log(f"Job {job_id} was cancelled before worker start", notify=False)
                continue
            state["started"] = True
            start_dt = datetime.now()

            await owner_log(
                f"Job {job_id} started by {job['starter_user_id']} on worker {worker_id} for {job['duration']}s -> {os.path.basename(job['filename'])}",
                notify=False,
            )

            success, details = await execute_ffmpeg_recording(job)
            state = pending_states.get(chat_id, {}).get(job_id, {})
            is_cancelled = state.get("cancelled", False)

            if is_cancelled:
                await safe_edit(msg, "❌ Recording cancelled.")
                await owner_log(f"Job {job_id} cancelled by {job['starter_user_id']}", notify=False)
                continue

            if not success:
                fail_text = (
                    f"❌ Job #{job_id[:8]} failed.\n\n"
                    f"The stream was unstable or ended too early, so the bot refused to upload a broken file.\n\n"
                    f"**Details:**\n```\n{details[-1200:]}\n```"
                )
                await safe_edit(msg, fail_text, parse_mode=enums.ParseMode.MARKDOWN)
                await owner_log(f"Job {job_id} failed: {details[:500]}", notify=False)
                continue

            final_output_path = job["filename"]
            record_file_owner(os.path.basename(final_output_path), job["starter_user_id"])
            await safe_edit(msg, "🔧 Post-processing recording for Telegram upload...")
            await asyncio.to_thread(repair_media_file, final_output_path)

            duration_secs, width, height = await asyncio.to_thread(get_video_attributes, final_output_path)
            thumb_path = await asyncio.to_thread(generate_thumbnail, final_output_path) if width and height else None
            job_end = datetime.now()
            resolution_text = f"{width}x{height}" if width and height else "Unknown"

            caption = (
                f"**📺 Stream Recording**\n\n"
                f"**📁 File Name:** `{os.path.basename(final_output_path)}`\n"
                f"**📏 Resolution:** `{resolution_text}`\n"
                f"**⏱ Duration:** `{format_seconds(duration_secs)}`\n"
                f"**🕒 Recorded:** `{start_dt.strftime('%I:%M %p')}` - `{job_end.strftime('%I:%M %p')}`\n"
                f"**🗓 Date:** `{start_dt.strftime('%d-%m-%Y')}`\n\n"
                f"**{BRANDING}**"
            )

            await upload_file_with_progress(
                app,
                chat_id,
                final_output_path,
                caption,
                thumb_path=thumb_path,
                msg_id=msg.id,
                owner_user_id=job["starter_user_id"],
                cleanup_after=False,
            )
            await owner_log(
                f"Job {job_id} uploaded successfully. duration={duration_secs}s file={os.path.basename(final_output_path)}",
                notify=False,
            )
        except Exception as e:
            write_log(f"Worker {worker_id} failed on job {job_id}: {e}")
            if msg:
                await safe_edit(msg, f"❌ Internal error while processing job #{job_id[:8]}: {e}")
        finally:
            state = pending_states.get(chat_id, {}).get(job_id)
            if state is not None:
                state["completed"] = True
            cleanup_job_state(chat_id, job_id)
            job_queue.task_done()


# ===== COMMANDS =====
@app.on_message(filters.command(["start", "help"]) & (filters.private | filters.group))
async def start_handler(client, message):
    user_id = message.from_user.id if message.from_user else 0
    if is_banned_user(user_id):
        return await safe_reply(message, "🚫 You are banned from using this bot.")

    text = (
        "👋 Bot is online.\n\n"
        "**Main Commands**\n"
        "`/record <duration> [bitrate] <m3u8_url> [file_name] [--referer URL] [--header \"Key: Value\"]`\n"
        "Example: `/record 2m https://site/stream.m3u8 Doraemon.mkv`\n"
        "Example: `/record 2m 1400k https://site/stream.m3u8 Doraemon.mp4 --referer https://site.com`\n"
        "Bitrate is optional. When provided, the bot re-encodes video to target that bitrate.\n\n"
        "`/cancel` - cancel your running/queued job\n"
        "`/status` - queue and running status\n"
        "`/list` - show recent files\n"
        "`/reupload <file_name>` - upload saved file again\n"
        "`/delete <file_name>` - delete saved file\n"
        "`/logs` - owner/admin logs\n\n"
        "**Admin Commands**\n"
        "`/approve <user_id>` | `/unapprove <user_id>`\n"
        "`/addadmin <user_id>` | `/removeadmin <user_id>`\n"
        "`/ban <user_id>` | `/unban <user_id>`\n"
    )

    if not is_approved_id(user_id):
        text += "\nYou are not approved yet. Ask an admin/owner to approve your Telegram user ID."
        return await safe_reply(message, text, parse_mode=enums.ParseMode.MARKDOWN)

    if not await check_force_sub(user_id) and not is_admin_user(user_id):
        return await safe_reply(
            message,
            text + f"\nJoin {FSUB_CHANNEL} first, then try again.",
            reply_markup=await force_sub_markup(),
            parse_mode=enums.ParseMode.MARKDOWN,
        )

    await safe_reply(message, text, parse_mode=enums.ParseMode.MARKDOWN)

@app.on_message(filters.command("record") & (filters.private | filters.group))
@approved_only
async def record_handler(client, message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if user_running_jobs(user_id) >= MAX_JOBS_PER_USER and not is_admin_user(user_id):
        return await safe_reply(
            message,
            f"🚫 User limit reached. You can run maximum {MAX_JOBS_PER_USER} active job(s) at once.",
        )

    try:
        parsed = parse_record_command(message.text)
    except Exception as e:
        return await safe_reply(message, f"❌ {e}", parse_mode=enums.ParseMode.MARKDOWN)

    if parsed["audio_only"] and parsed["video_only"]:
        return await safe_reply(message, "❌ Use only one of --audio-only or --video-only.")

    url = parsed["url"]
    if not url.startswith(("http://", "https://")):
        return await safe_reply(message, "❌ Stream URL must start with http:// or https://")

    output_path = safe_output_path(parsed["file_name"])
    job_id = os.urandom(8).hex()
    msg = await safe_reply(message, f"🎬 Recording Job #{job_id[:8]} created.\n🔍 Fetching stream info...")
    if msg is None:
        return

    try:
        info = await ffprobe_streams(url, headers=parsed["headers"])
    except Exception as e:
        return await safe_edit(msg, f"❌ Could not read stream info.\n`{str(e)[:800]}`", parse_mode=enums.ParseMode.MARKDOWN)

    videos = []
    audios = []
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "video" and stream.get("width") and stream.get("height"):
            videos.append(
                {
                    "index": int(stream["index"]),
                    "width": int(stream.get("width") or 0),
                    "height": int(stream.get("height") or 0),
                    "bitrate": stream.get("bit_rate"),
                }
            )
        elif stream.get("codec_type") == "audio":
            audios.append(
                {
                    "index": int(stream["index"]),
                    "language": stream.get("tags", {}).get("language") or "Unknown",
                }
            )

    videos.sort(key=lambda x: (x["height"], x["width"]), reverse=True)

    pending_states.setdefault(chat_id, {})[job_id] = {
        "msg_id": msg.id,
        "cancelled": False,
        "completed": False,
        "url": url,
        "duration": parsed["duration"],
        "filename": output_path,
        "videos": videos,
        "audios": audios,
        "selected_video": None,
        "selected_audios": [],
        "bitrate": parsed["bitrate"],
        "audio_only": parsed["audio_only"],
        "video_only": parsed["video_only"],
        "starter_user_id": user_id,
        "headers": parsed["headers"],
        "queued": False,
    }
    sel = pending_states[chat_id][job_id]

    if parsed["audio_only"]:
        if not audios:
            pending_states[chat_id].pop(job_id, None)
            return await safe_edit(msg, "❌ No audio tracks found for audio-only recording.")
        sel["selected_audios"] = [str(a["index"]) for a in audios]
        job_details = {
            "chat_id": chat_id,
            "job_id": job_id,
            "url": url,
            "duration": parsed["duration"],
            "filename": output_path,
            "video": None,
            "audios": sel["selected_audios"],
            "bitrate": parsed["bitrate"],
            "msg_id": msg.id,
            "starter_user_id": user_id,
            "headers": parsed["headers"],
        }
        await job_queue.put(job_details)
        sel["queued"] = True
        return await safe_edit(
            msg,
            f"✅ Audio-only job added to queue.\nAudios: {', '.join(a['language'] for a in audios)}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
            ),
        )

    if not videos:
        pending_states[chat_id].pop(job_id, None)
        return await safe_edit(msg, "❌ No video tracks found in this stream.")

    if parsed["video_only"]:
        sel["selected_video"] = videos[0]
        job_details = {
            "chat_id": chat_id,
            "job_id": job_id,
            "url": url,
            "duration": parsed["duration"],
            "filename": output_path,
            "video": sel["selected_video"],
            "audios": [],
            "bitrate": parsed["bitrate"],
            "msg_id": msg.id,
            "starter_user_id": user_id,
            "headers": parsed["headers"],
        }
        await job_queue.put(job_details)
        sel["queued"] = True
        return await safe_edit(
            msg,
            f"✅ Video-only job added to queue.\nVideo: {normalized_quality_label(sel['selected_video']['width'], sel['selected_video']['height'])}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
            ),
        )

    buttons = [
        [
            InlineKeyboardButton(
                f"🎥 {normalized_quality_label(v['width'], v['height'])}",
                callback_data=f"vidsel|{chat_id}|{job_id}|{v['index']}",
            )
        ]
        for v in videos[:12]
    ]
    await safe_edit(msg, f"Select video quality for Job #{job_id[:8]}:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex(r"^vidsel\|"))
async def callback_video(client, callback_query):
    _, chat_id_str, job_id, idx_str = callback_query.data.split("|")
    chat_id = int(chat_id_str)
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        await safe_answer(callback_query, "Session expired.", show_alert=True)
        return
    if not await enforce_callback_access(callback_query, job_state=sel):
        return

    selected_video = next((v for v in sel["videos"] if str(v["index"]) == idx_str), None)
    if not selected_video:
        await safe_answer(callback_query, "Invalid video selection.", show_alert=True)
        return

    sel["selected_video"] = selected_video
    if not sel["audios"]:
        buttons = [[InlineKeyboardButton("✅ Start Recording", callback_data=f"done|{chat_id}|{job_id}")]]
        await safe_edit(
            callback_query.message,
            f"Selected video: {normalized_quality_label(selected_video['width'], selected_video['height'])}\n\nNo audio tracks found. Start video-only recording?",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        await safe_answer(callback_query, "Video selected.")
        return

    if not sel["selected_audios"] and sel["audios"]:
        sel["selected_audios"] = [str(sel["audios"][0]["index"])]

    buttons = []
    for a in sel["audios"]:
        is_selected = str(a["index"]) in sel["selected_audios"]
        emoji = "✅" if is_selected else "🔊"
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {a['language']} (a{a['index']})",
                callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}",
            )
        ])
    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])

    await safe_edit(
        callback_query.message,
        f"Selected video: {normalized_quality_label(selected_video['width'], selected_video['height'])}\n"
        f"Select audio tracks, then tap ✅ Done.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    await safe_answer(callback_query, "Video selected.")


@app.on_callback_query(filters.regex(r"^audsel\|"))
async def callback_audio(client, callback_query):
    _, chat_id_str, job_id, idx_str = callback_query.data.split("|")
    chat_id = int(chat_id_str)
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        await safe_answer(callback_query, "Session expired.", show_alert=True)
        return
    if not await enforce_callback_access(callback_query, job_state=sel):
        return

    if idx_str in sel["selected_audios"]:
        sel["selected_audios"].remove(idx_str)
    else:
        sel["selected_audios"].append(idx_str)

    buttons = []
    for a in sel["audios"]:
        is_selected = str(a["index"]) in sel["selected_audios"]
        emoji = "✅" if is_selected else "🔊"
        buttons.append([
            InlineKeyboardButton(
                f"{emoji} {a['language']} (a{a['index']})",
                callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}",
            )
        ])
    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])

    await safe_edit(
        callback_query.message,
        f"Selected video: {normalized_quality_label(sel['selected_video']['width'], sel['selected_video']['height'])}\n"
        f"Select audio tracks, then tap ✅ Done.",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    await safe_answer(callback_query, "Audio selection updated.")


@app.on_callback_query(filters.regex(r"^done\|"))
async def callback_done(client, callback_query):
    _, chat_id_str, job_id = callback_query.data.split("|")
    chat_id = int(chat_id_str)
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        await safe_answer(callback_query, "Session expired.", show_alert=True)
        return
    if not await enforce_callback_access(callback_query, job_state=sel):
        return

    video = sel.get("selected_video")
    audio_maps = sel.get("selected_audios") or []
    job_details = {
        "chat_id": chat_id,
        "job_id": job_id,
        "url": sel["url"],
        "duration": sel["duration"],
        "filename": sel["filename"],
        "video": video,
        "audios": audio_maps,
        "bitrate": sel["bitrate"],
        "msg_id": sel["msg_id"],
        "starter_user_id": sel["starter_user_id"],
        "headers": sel.get("headers") or {},
    }
    await job_queue.put(job_details)
    sel["queued"] = True

    audio_names = [a["language"] for a in sel["audios"] if str(a["index"]) in audio_maps] or ["None"]
    video_info = normalized_quality_label(video["width"], video["height"]) if video else "None"
    await safe_edit(
        callback_query.message,
        f"✅ Job #{job_id[:8]} added to queue.\n"
        f"Video: {video_info}\n"
        f"Audios: {', '.join(audio_names)}\n"
        f"Duration: {format_seconds(sel['duration'])}",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]]
        ),
    )
    await safe_answer(callback_query, "Added to queue.")


@app.on_callback_query(filters.regex(r"^cancel\|"))
async def callback_cancel(client, callback_query):
    _, chat_id_str, job_id = callback_query.data.split("|")
    chat_id = int(chat_id_str)
    state = pending_states.get(chat_id, {}).get(job_id)
    if not state:
        await safe_answer(callback_query, "Job not found.", show_alert=True)
        return await safe_edit(callback_query.message, "❌ Job no longer exists.")

    if not await enforce_callback_access(callback_query, job_state=state):
        return

    state["cancelled"] = True
    await safe_answer(callback_query, "Cancelling job...")

    proc = running_jobs.get(job_id)
    if proc:
        try:
            proc.send_signal(signal.SIGTERM)
        except Exception:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=8)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        running_jobs.pop(job_id, None)

    file_path = state.get("filename")
    if file_path and os.path.exists(file_path):
        try:
            os.remove(file_path)
        except Exception:
            pass
    cleanup_job_state(chat_id, job_id)
    await safe_edit(callback_query.message, "❌ Recording cancelled.")


@app.on_callback_query(filters.regex(r"^cancelup\|"))
async def callback_cancel_upload(client, callback_query):
    _, _chat_id_str, upload_id = callback_query.data.split("|")
    upload_state = active_uploads.get(upload_id)
    if not upload_state:
        return await safe_answer(callback_query, "Upload not found or already finished.", show_alert=True)

    owner_user_id = upload_state.get("owner_user_id")
    user_id = callback_query.from_user.id if callback_query.from_user else 0
    if owner_user_id and user_id != owner_user_id and not is_admin_user(user_id):
        return await safe_answer(callback_query, "🚫 This upload belongs to another user.", show_alert=True)

    upload_state["cancelled"] = True
    await safe_answer(callback_query, "Upload cancel requested.")
    await safe_edit(callback_query.message, "❌ Upload is being cancelled...")


@app.on_message(filters.command("cancel") & (filters.private | filters.group))
@approved_only
async def cancel_handler(client, message):
    user_id = message.from_user.id
    buttons = []

    for state_chat_id, chat_jobs in pending_states.items():
        for job_id, state in chat_jobs.items():
            if state.get("completed") or state.get("cancelled"):
                continue
            if not can_manage_job(user_id, state):
                continue
            prefix = "🎬 Running" if job_id in running_jobs else "🕰️ Queued"
            buttons.append([
                InlineKeyboardButton(
                    f"{prefix} #{job_id[:8]}",
                    callback_data=f"cancel|{state_chat_id}|{job_id}",
                )
            ])

    if not buttons:
        return await safe_reply(message, "❌ No active jobs you can cancel.")

    await safe_reply(message, "Select a job to cancel:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_message(filters.command("status") & (filters.private | filters.group))
@approved_only
async def status_handler(client, message):
    running_count = len(running_jobs)
    queue_count = job_queue.qsize()
    text = (
        "📊 **Bot Status**\n\n"
        f"🎬 Running Jobs: **{running_count}**\n"
        f"🕰️ Queued Jobs: **{queue_count}**\n"
        f"👥 Approved Users: **{len(APPROVED_USERS)}**\n"
        f"🛡️ Admins: **{len(ADMIN_USERS)}**\n"
        f"🚫 Banned: **{len(BANNED_USERS)}**\n"
        f"⚙️ Per User Active Limit: **{MAX_JOBS_PER_USER}**"
    )
    if running_count:
        text += "\n\n**Running Job IDs**\n"
        for job_id in running_jobs.keys():
            text += f"`{job_id[:8]}`\n"
    await safe_reply(message, text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command(["approve"]) & filters.private)
@admin_only
async def approve_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/approve <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    if user_id in BANNED_USERS:
        BANNED_USERS.discard(user_id)
        await redis_set_remove("banned_users", user_id)

    APPROVED_USERS.add(user_id)
    await redis_set_add("approved_users", user_id)
    await owner_log(f"User approved: {user_id}", notify=False)
    await safe_reply(message, f"✅ User `{user_id}` approved.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command(["unapprove", "disapprove"]) & filters.private)
@admin_only
async def unapprove_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/disapprove <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    if user_id in OWNER_IDS:
        return await safe_reply(message, "🚫 You cannot disapprove an owner.")

    APPROVED_USERS.discard(user_id)
    ADMIN_USERS.discard(user_id)
    await redis_set_remove("approved_users", user_id)
    await redis_set_remove("admin_users", user_id)
    await owner_log(f"User disapproved: {user_id}", notify=False)
    await safe_reply(message, f"❌ User `{user_id}` disapproved.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command(["addadmin", "admin"]) & filters.private)
@owner_only
async def add_admin_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/addadmin <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    APPROVED_USERS.add(user_id)
    ADMIN_USERS.add(user_id)
    await redis_set_add("approved_users", user_id)
    await redis_set_add("admin_users", user_id)
    await owner_log(f"Admin added: {user_id}", notify=False)
    await safe_reply(message, f"✅ Admin `{user_id}` added.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command(["removeadmin", "unadmin"]) & filters.private)
@owner_only
async def remove_admin_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/removeadmin <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    if user_id in OWNER_IDS:
        return await safe_reply(message, "🚫 You cannot remove owner from admins.")

    ADMIN_USERS.discard(user_id)
    await redis_set_remove("admin_users", user_id)
    await owner_log(f"Admin removed: {user_id}", notify=False)
    await safe_reply(message, f"❌ Admin `{user_id}` removed.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("ban") & filters.private)
@admin_only
async def ban_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/ban <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    if user_id in OWNER_IDS:
        return await safe_reply(message, "🚫 You cannot ban an owner.")

    BANNED_USERS.add(user_id)
    APPROVED_USERS.discard(user_id)
    ADMIN_USERS.discard(user_id)
    await redis_set_add("banned_users", user_id)
    await redis_set_remove("approved_users", user_id)
    await redis_set_remove("admin_users", user_id)
    await owner_log(f"User banned: {user_id}", notify=False)
    await safe_reply(message, f"🚫 User `{user_id}` banned.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("unban") & filters.private)
@admin_only
async def unban_handler(client, message):
    try:
        user_id = int(message.text.split(maxsplit=1)[1])
    except Exception:
        return await safe_reply(message, "Usage: `/unban <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)

    BANNED_USERS.discard(user_id)
    APPROVED_USERS.add(user_id)
    await redis_set_remove("banned_users", user_id)
    await redis_set_add("approved_users", user_id)
    await owner_log(f"User unbanned: {user_id}", notify=False)
    await safe_reply(message, f"✅ User `{user_id}` unbanned and approved.", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("logs") & filters.private)
@admin_only
async def logs_handler(client, message):
    if not os.path.exists(LOG_FILE):
        return await safe_reply(message, "❌ No logs found yet.")
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()[-50:]
        text = "```\n" + "".join(lines)[-3500:] + "\n```"
        await safe_reply(message, text, parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        await safe_reply(message, f"❌ Could not read logs: {e}")


@app.on_message(filters.command("list") & (filters.private | filters.group))
@approved_only
async def list_files_handler(client, message):
    user_id = message.from_user.id
    files = [
        f for f in os.listdir(OUTPUT_DIR)
        if os.path.isfile(os.path.join(OUTPUT_DIR, f)) and not f.startswith(".") and can_access_file(user_id, f)
    ]
    if not files:
        return await safe_reply(message, "❌ No recorded files found for your account.")

    files.sort(key=lambda x: os.path.getmtime(os.path.join(OUTPUT_DIR, x)), reverse=True)
    text = "**📂 Recorded Files (Last 10)**\n\n"
    buttons = []
    for file_name in files[:10]:
        text += f"`{file_name}`\n"
        token = file_token_for(file_name)
        buttons.append([
            InlineKeyboardButton(f"🔄 Reupload {file_name[:24]}", callback_data=f"reupload|{token}")
        ])
    await safe_reply(message, text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.MARKDOWN)


@app.on_callback_query(filters.regex(r"^reupload\|"))
async def callback_reupload(client, callback_query):
    user_id = callback_query.from_user.id if callback_query.from_user else 0
    if is_banned_user(user_id) or not is_approved_id(user_id):
        return await safe_answer(callback_query, "🚫 You are not allowed.", show_alert=True)
    if not await check_force_sub(user_id) and not is_admin_user(user_id):
        return await safe_answer(callback_query, f"Join {FSUB_CHANNEL} first.", show_alert=True)

    _, token = callback_query.data.split("|")
    file_name = resolve_file_name_from_token(token)
    if not file_name:
        return await safe_answer(callback_query, "File token expired.", show_alert=True)

    file_path = os.path.join(OUTPUT_DIR, file_name)
    if not os.path.exists(file_path):
        return await safe_edit(callback_query.message, "❌ File not found on server.")
    if not can_access_file(user_id, file_name):
        return await safe_answer(callback_query, "🚫 This file belongs to another user.", show_alert=True)

    await safe_answer(callback_query, "Re-upload started.")
    await safe_edit(callback_query.message, f"🔄 Repairing and re-uploading `{file_name}`...", parse_mode=enums.ParseMode.MARKDOWN)

    await asyncio.to_thread(repair_media_file, file_path)
    duration_secs, width, height = await asyncio.to_thread(get_video_attributes, file_path)
    caption = (
        f"**📺 Stream Recording (Re-upload)**\n\n"
        f"**📁 File Name:** `{file_name}`\n"
        f"**⏱ Duration:** `{format_seconds(duration_secs)}`\n"
        f"**📏 Resolution:** `{width}x{height}`\n\n"
        f"**{BRANDING}**"
    )
    thumb_path = await asyncio.to_thread(generate_thumbnail, file_path) if width and height else None
    await upload_file_with_progress(
        client,
        callback_query.message.chat.id,
        file_path,
        caption,
        thumb_path=thumb_path,
        msg_id=callback_query.message.id,
        owner_user_id=user_id,
        cleanup_after=False,
    )


@app.on_message(filters.command("reupload") & (filters.private | filters.group))
@approved_only
async def reupload_handler(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await safe_reply(message, "Usage: `/reupload <file_name>`", parse_mode=enums.ParseMode.MARKDOWN)
    file_name = sanitize_filename(parts[1])
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if not os.path.exists(file_path):
        return await safe_reply(message, "❌ File not found.")
    if not can_access_file(message.from_user.id, file_name):
        return await safe_reply(message, "🚫 This file belongs to another user.")

    await asyncio.to_thread(repair_media_file, file_path)
    duration_secs, width, height = await asyncio.to_thread(get_video_attributes, file_path)
    thumb_path = await asyncio.to_thread(generate_thumbnail, file_path) if width and height else None
    caption = (
        f"**📺 Stream Recording (Re-upload)**\n\n"
        f"**📁 File Name:** `{file_name}`\n"
        f"**⏱ Duration:** `{format_seconds(duration_secs)}`\n"
        f"**📏 Resolution:** `{width}x{height}`\n\n"
        f"**{BRANDING}**"
    )
    await upload_file_with_progress(
        client,
        message.chat.id,
        file_path,
        caption,
        thumb_path=thumb_path,
        owner_user_id=message.from_user.id,
        cleanup_after=False,
    )


@app.on_message(filters.command("delete") & (filters.private | filters.group))
@approved_only
async def delete_file_handler(client, message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await safe_reply(message, "Usage: `/delete <file_name>`", parse_mode=enums.ParseMode.MARKDOWN)

    file_name = sanitize_filename(parts[1])
    file_path = os.path.join(OUTPUT_DIR, file_name)
    if not os.path.exists(file_path):
        return await safe_reply(message, f"❌ File `{file_name}` not found.", parse_mode=enums.ParseMode.MARKDOWN)
    if not can_access_file(message.from_user.id, file_name):
        return await safe_reply(message, "🚫 This file belongs to another user.")

    try:
        os.remove(file_path)
        FILE_OWNER_MAP.pop(file_name, None)
        r = get_redis()
        if r:
            try:
                r.hdel("file_owner_map", file_name)
            except Exception as e:
                write_log(f"Redis hdel failed for file owner {file_name}: {e}")
        await safe_reply(message, f"✅ File `{file_name}` deleted.", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        await safe_reply(message, f"❌ Could not delete file: {e}")


# ===== STARTUP =====
async def start_bot():
    global BOT_LOOP
    BOT_LOOP = asyncio.get_running_loop()
    print("Booting bot...")
    print("API_ID present:", bool(os.environ.get("API_ID")))
    print("API_HASH present:", bool(os.environ.get("API_HASH")))
    print("BOT_TOKEN present:", bool(os.environ.get("BOT_TOKEN")))
    print("OWNER_IDS:", os.environ.get("OWNER_IDS", ""))
    print("FSUB_CHANNEL:", os.environ.get("FSUB_CHANNEL", ""))

    validate_runtime()
    refresh_user_sets()

    await app.start()
    me = await app.get_me()
    print(f"Bot started successfully as @{getattr(me, 'username', 'unknown')}")
    write_log("Bot started")

    for owner_id in OWNER_IDS:
        try:
            await app.send_message(owner_id, "✅ Recording bot is running on Railway.")
        except Exception:
            pass

    workers = [asyncio.create_task(worker(i + 1)) for i in range(NUM_WORKERS)]
    try:
        await asyncio.Event().wait()
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        try:
            await app.stop()
        except Exception:
            pass


if __name__ == "__main__":
    if not REDIS_URL:
        write_log("Warning: REDIS_URL is not set. Persistence will be limited.")
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        write_log("Bot stopped by KeyboardInterrupt")
    except Exception as e:
        write_log(f"Fatal startup error: {e}")
        raise
