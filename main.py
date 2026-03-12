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
BOT_TOKEN = "8698277473:AAEkWMwudRlP3LvfU66zIT5hIMydmBjuTC0"
REDIS_URL = os.environ.get('REDIS_URL')

OUTPUT_DIR = "pyrogram_recordings"
OWNER_IDS = [6407533831]  # List of owner IDs
APPROVED_USERS = []
BRANDING = "🔗 Join Channel:- @DorutoChan"

MAX_FILE_SIZE = 1.9 * 1024 * 1024 * 1024  # 1.9 GB in bytes

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Client Setup
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

# Redis Client
redis_client = None


# ===== Helpers =====

def get_video_attributes(file_path):
    """
    Gets accurate duration, width, and height for Telegram.
    """
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height,duration",
             "-show_entries", "format=duration",
             "-of", "json", file_path],
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

        # Try to get from stream first
        if 'streams' in data and data['streams']:
            stream = data['streams'][0]
            width = int(stream.get('width', 0))
            height = int(stream.get('height', 0))
            try:
                duration = float(stream.get('duration', 0))
            except:
                pass

        # Fallback to format duration if stream duration is missing
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
    """Backward compatibility wrapper"""
    d, _, _ = get_video_attributes(file_path)
    return d


def convert_ts_to_mp4(ts_path, mp4_path):
    """
    FIX: Converts TS recording to MP4 without re-encoding (-c copy).
    This fixes 'Duration Unknown' and 'No Thumbnail' issues.
    """
    try:
        print(f"🔧 Converting TS to MP4: {ts_path} -> {mp4_path}")

        # -bsf:a aac_adtstoasc is CRITICAL for converting mpeg-ts audio to mp4
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
            print(f"✅ Conversion successful: {mp4_path}")
            return True
        else:
            print("⚠️ Conversion produced empty file.")
            return False

    except Exception as e:
        print(f"⚠️ Conversion failed: {e}")
        return False


def fix_video_metadata(file_path):
    """
    Used for Re-upload command to fix existing MP4s.
    """
    temp_path = f"{file_path}.temp.mp4"
    try:
        print(f"🔧 Fixing metadata for {file_path}...")
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

        if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
            os.replace(temp_path, file_path)
            return True
        else:
            if os.path.exists(temp_path): os.remove(temp_path)
            return False
    except Exception as e:
        print(f"⚠️ Metadata fix failed: {e}")
        if os.path.exists(temp_path): os.remove(temp_path)
        return False


def generate_thumbnail(video_path):
    thumb_path = f"{video_path}.jpg"
    try:
        if not os.path.exists(video_path) or os.path.getsize(video_path) == 0:
            return None

        duration = get_duration(video_path)

        ss_time = "00:00:05"
        if duration < 5:
            ss_time = "00:00:01"

        subprocess.run(
            ["ffmpeg", "-y", "-ss", ss_time, "-i", video_path,
             "-frames:v", "1", "-q:v", "2", thumb_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True
        )

        if os.path.exists(thumb_path):
            Image.open(thumb_path).convert("RGB").save(thumb_path, "JPEG")
            return thumb_path

    except subprocess.CalledProcessError as e:
        print(f"Thumbnail generation failed with ffmpeg error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during thumbnail generation: {e}")

    return None


def split_video_by_size(input_file, max_size, output_dir):
    """
    Splits a video file into smaller chunks using FFmpeg based on a target size.
    """
    try:
        total_duration = get_duration(input_file)
        if not total_duration:
            print("Could not get video duration, unable to split.")
            return [input_file]

        total_size = os.path.getsize(input_file)
        if total_size <= max_size:
            return [input_file]

        num_parts = int(total_size / max_size) + 1
        part_duration = total_duration // num_parts

        base_name, ext = os.path.splitext(os.path.basename(input_file))

        split_files = []
        start_time = 0

        print(f"Splitting file {input_file} (duration: {total_duration}s) into {num_parts} parts.")

        for i in range(num_parts):
            output_file = os.path.join(output_dir, f"{base_name}_part{i + 1}{ext}")

            cmd = [
                "ffmpeg", "-y",
                "-ss", str(start_time),
                "-i", input_file,
                "-t", str(part_duration),
                "-c", "copy",
                "-map", "0", "-dn", "-ignore_unknown",
                output_file
            ]

            if i == num_parts - 1:
                cmd = [
                    "ffmpeg", "-y",
                    "-ss", str(start_time),
                    "-i", input_file,
                    "-c", "copy",
                    "-map", "0", "-dn", "-ignore_unknown",
                    output_file
                ]

            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            split_files.append(output_file)

            start_time += part_duration

        os.remove(input_file)
        print(f"Original large file {input_file} deleted after splitting.")

        return split_files

    except Exception as e:
        print(f"Error during video splitting: {e}")
        return [input_file]


async def upload_file_with_progress(client, chat_id, file_path, caption, thumb_path=None, msg_id=None, is_temp=False):
    msg = None
    try:
        if msg_id:
            try:
                msg = await client.get_messages(chat_id, message_ids=msg_id)
                await msg.edit_text("🚀 Preparing Upload...", reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Cancel", "cancel_upload")]]))
            except Exception:
                msg = await client.send_message(chat_id, "🚀 Preparing Upload...")
        else:
            msg = await client.send_message(chat_id, "🚀 Preparing Upload...")

        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            await msg.edit_text("❌ Upload failed: File is empty or does not exist.")
            return

        file_size = os.path.getsize(file_path)

        if file_size > MAX_FILE_SIZE:
            await msg.edit_text("🔄 File size is too large. Splitting into parts...")
            split_files = split_video_by_size(file_path, MAX_FILE_SIZE, OUTPUT_DIR)

            if len(split_files) > 1:
                await msg.edit_text(f"✅ Splitting complete. Uploading {len(split_files)} parts...")
                for i, part_file in enumerate(split_files):
                    part_caption = f"Part {i + 1} of {len(split_files)}\n{caption}"
                    part_thumb = generate_thumbnail(part_file)
                    await upload_single_file(client, chat_id, part_file, part_caption, part_thumb)

                    # Clean up parts immediately to save space
                    if os.path.exists(part_file):
                        os.remove(part_file)
                    if part_thumb and os.path.exists(part_thumb):
                        os.remove(part_thumb)
                    await asyncio.sleep(3)
            else:
                await msg.edit_text("❌ Splitting failed. Uploading original file (may fail).")
                await upload_single_file(client, chat_id, file_path, caption, thumb_path)
        else:
            await upload_single_file(client, chat_id, file_path, caption, thumb_path, msg_id, is_temp)

        if msg:
            try:
                await msg.delete()
            except Exception:
                pass
    except Exception as e:
        if msg:
            try:
                await msg.edit_text(f"❌ Upload failed: {e}")
            except:
                pass
        print(f"Upload failed: {e}")
    finally:
        for f in os.listdir(OUTPUT_DIR):
            if f.startswith(f"job_{chat_id}_"):
                os.remove(os.path.join(OUTPUT_DIR, f))


async def upload_single_file(client, chat_id, file_path, caption, thumb_path=None, msg_id=None, is_temp=False):
    """Handles the upload of a single file, with progress bar updates."""
    msg = None
    if msg_id:
        try:
            msg = await client.get_messages(chat_id, message_ids=msg_id)
        except Exception:
            msg = await client.send_message(chat_id, "🚀 Uploading...")
    else:
        msg = await client.send_message(chat_id, "🚀 Uploading...")

    thumb_to_send = thumb_path if thumb_path and os.path.exists(thumb_path) else None

    # Get Accurate Attributes to force 'Video' type message
    duration, width, height = get_video_attributes(file_path)
    if duration == 0: duration = 1

    start_time = time.time()
    last_edit_time = time.time()
    last_sent_bytes = 0

    last_10_percent_block = -1

    total_size = os.path.getsize(file_path) if os.path.exists(file_path) else 1

    async def update_progress(current, total):
        nonlocal last_edit_time, last_sent_bytes, last_10_percent_block

        if current < last_sent_bytes:
            current = last_sent_bytes

        now = time.time()
        percentage = (current / total) * 100
        current_10_percent_block = int(percentage) // 10

        # Update every 10 seconds or at 100%
        if current == total or (current_10_percent_block > last_10_percent_block and now - last_edit_time > 10):
            elapsed_time = now - start_time
            speed = current / elapsed_time if elapsed_time > 0 else 0
            eta = (total - current) / speed if speed > 0 else 0

            progress_bar_length = 10
            filled_blocks = int(percentage / 100 * progress_bar_length)
            empty_blocks = progress_bar_length - filled_blocks
            progress_bar = f"[{'■' * filled_blocks}{'□' * empty_blocks}]"

            text = (
                f"🚀 Uploading...\n"
                f"**{progress_bar}** **{percentage:.2f}%**\n"
                f"📦 `{current / 1024 / 1024:.2f} MB / {total / 1024 / 1024:.2f} MB`\n"
                f"⚡ Speed: `{speed / 1024:.2f} KB/s`\n"
                f"⏱️ ETA: `{str(timedelta(seconds=int(eta)))}`\n"
            )
            try:
                await msg.edit_text(text)
                last_edit_time = now
                last_sent_bytes = current
                last_10_percent_block = current_10_percent_block
            except (FloodWait, MessageNotModified):
                pass
            except Exception:
                pass

    try:
        # IMPORTANT: Passing width, height, and supports_streaming tells Telegram this is a VIDEO
        await client.send_video(
            chat_id=chat_id,
            video=file_path,
            caption=caption,
            thumb=thumb_to_send,
            duration=duration,
            width=width,
            height=height,
            supports_streaming=True,
            progress=lambda current, total: asyncio.run_coroutine_threadsafe(
                update_progress(current, total), main_loop
            )
        )
        if msg:
            await msg.delete()
    except Exception as e:
        print(f"Pyrogram upload failed: {e}")
        if msg:
            try:
                await msg.edit_text(f"❌ Upload failed: {e}")
            except:
                pass
    finally:
        if is_temp and os.path.exists(file_path):
            os.remove(file_path)
        if thumb_path and os.path.exists(thumb_path):
            os.remove(thumb_path)


async def execute_ffmpeg_recording(chat_id, job_id, m3u8_url, duration, final_output_path, video_track, audio_maps):
    # We record to .ts first to prevent corruption if stream cuts
    temp_ts_path = final_output_path + ".ts"

    try:
        # ===== FIX: Correct Argument Ordering =====
        # Input options must be BEFORE -i
        # Output options (like -max_muxing_queue_size) must be AFTER -i
        
        cmd = [
    "ffmpeg", "-y",
    "-hide_banner", "-loglevel", "error",
    "-rw_timeout", "20000000",
    "-analyzeduration", "20000000", "-probesize", "20000000",
    "-user_agent", "Mozilla/5.0",
    "-headers", "Referer: https://www.sonyliv.com\r\nOrigin: https://www.sonyliv.com\r\n",
    "-reconnect", "1", "-reconnect_streamed", "1",
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
            for audio_idx in audio_maps:
                cmd.extend(["-map", f"0:{audio_idx}", "-c:a", "copy"])
        else:
            cmd.extend(["-an"])

        if not video_track and not audio_maps:
            return False

        # Output options placed here (AFTER input)
        cmd.extend([
            "-max_muxing_queue_size", "9999", # Correct placement
            "-dn", "-sn", "-ignore_unknown",
            "-map_metadata", "-1",
            "-t", str(duration + 10), # Extra buffer in ffmpeg, but Python controls kill
            "-f", "mpegts",
            temp_ts_path
        ])

        print(f"FFmpeg command: {' '.join(cmd)}")

        proc = await asyncio.create_subprocess_exec(*cmd,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE
                                                    )
        running_jobs[job_id] = proc

        progress_message_id = pending_states[chat_id][job_id]['msg_id']
        progress_message = await app.get_messages(chat_id, message_ids=progress_message_id)
        
        start_time_real = time.time()
        stop_time_real = start_time_real + duration
        
        last_update_time = time.time()
        last_10_percent_block = -1

        ffmpeg_stderr = b''
        
        # Monitor Loop
        while True:
            # FORCE STOP LOGIC: If time is up, kill the process
            now = time.time()
            if now >= stop_time_real:
                print(f"⏰ Duration limit reached ({duration}s). Stopping FFmpeg...")
                try:
                    proc.terminate()
                except:
                    pass
                break

            try:
                # Add timeout to readline to detect stuck processes
                line = await asyncio.wait_for(proc.stderr.readline(), timeout=5.0) 
            except asyncio.TimeoutError:
                # Check if process died
                if proc.returncode is not None:
                    break
                # If just silence, continue loop to check wall clock time
                continue

            if not line:
                break

            ffmpeg_stderr += line
            # line_str = line.decode('utf-8', errors='ignore')

            # UPDATE PROGRESS BASED ON WALL CLOCK TIME (More Reliable for Streams)
            elapsed_seconds = now - start_time_real
            percentage = (elapsed_seconds / duration) * 100
            if percentage > 100: percentage = 99.9

            current_10_percent_block = int(percentage) // 10

            if current_10_percent_block > last_10_percent_block or (now - last_update_time > 15):
                progress_bar_length = 10
                filled_blocks = int(percentage / 100 * progress_bar_length)
                empty_blocks = progress_bar_length - filled_blocks
                progress_bar = f"[{'■' * filled_blocks}{'□' * empty_blocks}]"

                current_size_mb = 0
                if os.path.exists(temp_ts_path):
                    current_size_mb = os.path.getsize(temp_ts_path) / (1024 * 1024)

                text = (
                    f"🎬 Recording in progress (Direct Copy)...\n"
                    f"**{progress_bar}** **{percentage:.2f}%**\n"
                    f"📦 `{current_size_mb:.2f} MB`\n"
                    f"⏱️ **Time:** `{str(timedelta(seconds=int(elapsed_seconds)))}` / `{str(timedelta(seconds=duration))}`"
                )
                try:
                    await progress_message.edit_text(
                        text,
                        reply_markup=InlineKeyboardMarkup(
                            [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]])
                    )
                    last_update_time = now
                    last_10_percent_block = current_10_percent_block
                except (FloodWait, MessageNotModified):
                    pass
                except Exception:
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

        is_cancelled = pending_states.get(chat_id, {}).get(job_id, {}).get('cancelled', False)
        if is_cancelled:
            print(f"Recording cancelled. Deleting partial file.")
            if os.path.exists(temp_ts_path):
                os.remove(temp_ts_path)
            return False

        # FIX: Check file size. If > 2MB, treat as success even if error code
        if os.path.exists(temp_ts_path) and os.path.getsize(temp_ts_path) > 2 * 1024 * 1024:
            return True

        if proc.returncode != 0:
            error_msg = ffmpeg_stderr.decode('utf-8', errors='ignore')
            print(f"FFmpeg process failed. Code {proc.returncode}")
            pending_states[chat_id][job_id]['ffmpeg_error'] = error_msg
            return False

        return False
    except Exception as e:
        print(f"Error in FFmpeg recording: {e}")
        if job_id in running_jobs:
            del running_jobs[job_id]
        return False


async def worker():
    while True:
        job = await job_queue.get()
        print(f"Processing job {job['job_id']} from queue.")

        temp_ts_path = job['filename'] + ".ts"
        final_output_path = job['filename']

        try:
            msg = await app.get_messages(job['chat_id'], message_ids=job['msg_id'])
            job_start_time = datetime.now()

            await msg.edit_text("✅ Job started! Recording in progress...")

            success = await execute_ffmpeg_recording(
                job['chat_id'], job['job_id'], job['url'],
                job['duration'], final_output_path, job['video'],
                job['audios']
            )

            is_cancelled = pending_states.get(job['chat_id'], {}).get(job['job_id'], {}).get('cancelled', False)

            if not success and not is_cancelled:
                ffmpeg_error_msg = pending_states.get(job['chat_id'], {}).get(job['job_id'], {}).get(
                    'ffmpeg_error', 'No specific error message.')

                user_friendly_error = (
                    f"❌ Job #{job['job_id']} failed.\n\n"
                    f"**Possible causes:**\n"
                    f"- Stream URL is invalid, expired, or blocked.\n"
                    f"- Network error interrupted the recording.\n"
                    f"- Stream format is not supported.\n\n"
                    f"**Log:**\n`{ffmpeg_error_msg[-300:]}`"
                )
                try:
                    await msg.edit_text(user_friendly_error, parse_mode=enums.ParseMode.MARKDOWN)
                except:
                    pass

            if success and not is_cancelled:
                await msg.edit_text("🔧 Post-processing: Converting TS to MP4 (Container Fix)...")

                # CONVERT TS TO MP4 (Critical for 2hr recordings, but simply copying streams)
                if os.path.exists(temp_ts_path):
                    conversion_success = convert_ts_to_mp4(temp_ts_path, final_output_path)
                    if conversion_success:
                        os.remove(temp_ts_path)
                    else:
                        # Fallback if conversion fails (rare)
                        await msg.edit_text("⚠️ Conversion failed. Uploading raw file...")
                        shutil.move(temp_ts_path, final_output_path)

                duration_secs, width, height = get_video_attributes(final_output_path)
                if duration_secs == 0: duration_secs = job['duration']  # Estimate if metadata fail

                duration_str = str(timedelta(seconds=int(duration_secs)))
                filename_base = os.path.basename(final_output_path)

                video = job.get('video')
                w = width if width else (video['width'] if video else 0)
                h = height if height else (video['height'] if video else 0)
                resolution_text = f" ({w}x{h}p)" if w else ""

                job_end_time = datetime.now()
                start_time_str = job_start_time.strftime('%I:%M %p')
                end_time_str = job_end_time.strftime('%I:%M %p')

                caption = (
                    f"**📺 Stream Recording:**{resolution_text}\n\n"
                    f"**📁 File Name:** `{filename_base}`\n"
                    f"**🗓️ Date:** `{job_start_time.strftime('%d %B %Y')}`\n"
                    f"**⏰ Recorded:** `{start_time_str}` - `{end_time_str}`\n"
                    f"**⏱ Duration:** `{duration_str}`\n\n"
                    f"**{BRANDING}**"
                )

                thumb_path = generate_thumbnail(final_output_path)
                await upload_file_with_progress(app, job['chat_id'], final_output_path, caption, thumb_path,
                                                msg_id=msg.id, is_temp=False)
        except Exception as e:
            print(f"Worker failed to process job {job['job_id']}: {e}")
            try:
                await msg.edit_text(f"❌ Job #{job['job_id']} failed due to an internal error.")
            except Exception:
                pass
        finally:
            job_queue.task_done()
            if os.path.exists(temp_ts_path):  # Cleanup just in case
                os.remove(temp_ts_path)
            print(f"Finished processing job {job['job_id']}.")


# ===== User Management =====
def load_approved_users():
    global APPROVED_USERS, redis_client
    if not redis_client and REDIS_URL:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)

    try:
        # Add all owner IDs to Redis set to ensure they are always approved
        if redis_client:
            redis_client.sadd("approved_users", *[str(uid) for uid in OWNER_IDS])
            # Get all approved users from Redis and update the global list
            approved_user_ids = redis_client.smembers("approved_users")
            APPROVED_USERS = [int(uid) for uid in approved_user_ids]
        else:
            APPROVED_USERS = list(OWNER_IDS)
    except Exception as e:
        print(f"Error connecting to Redis or loading users: {e}. Falling back to OWNER_IDS.")
        APPROVED_USERS = list(OWNER_IDS)


def is_owner(func):
    async def wrapper(client, message):
        if message.from_user.id not in OWNER_IDS:
            await message.reply_text("🚫 Sorry, this command is only for the bot owner(s).")
            return
        return await func(client, message)

    return wrapper


def is_approved_user(func):
    async def wrapper(client, message):
        if message.from_user.id not in APPROVED_USERS:
            await message.reply_text("🚫 Sorry, you are not approved to use this bot. Please contact the owner.")
            return
        return await func(client, message)

    return wrapper


# ===== Commands =====
@app.on_message(filters.command("start") & (filters.private | filters.group))
@is_approved_user
async def start_handler(client, message):
    await message.reply_text("👋 Bot Online!\n\n"
                             "**Commands:**\n"
                             "  `/record <duration> <m3u8_url> [file_name]` - Start recording a stream.\n"
                             "  `Example: /record 5m http://url.com/playlist.m3u8 my_show.mp4`\n"
                             "  `/cancel` - Cancel an active job.\n"
                             "  `/status` - Check bot status.\n"
                             "  `/list` - Show all recorded files.\n"
                             "  `/delete <file_name>` - Delete a recorded file.\n"
                             "  `/reupload <file_name>` - Reupload an existing file.\n\n"
                             "**Owner Commands:**\n"
                             "  `/approve <user_id>` - Approve a new user.\n"
                             "  `/unapprove <user_id>` - Unapprove a user.\n",
                             parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("record") &
                (filters.private | filters.group))
@is_approved_user
async def record_handler(client, message):
    parts = message.text.split()
    # Updated Argument parsing to REMOVE bitrate
    if len(parts) < 3:
        return await message.reply_text("Usage: `/record <duration> <m3u8_url> [file_name]`\n\n"
                                        "Example: `/record 5m http://example.com/live.m3u8 my_show.mp4`",
                                        parse_mode=enums.ParseMode.MARKDOWN)

    chat_id = message.chat.id
    user_id = message.from_user.id
    duration_str = parts[1]
    
    if duration_str.endswith("m"):
        duration = int(duration_str[:-1]) * 60
    elif duration_str.endswith("h"):
        duration = int(duration_str[:-1]) * 3600
    else:
        try:
            duration = int(duration_str)
        except ValueError:
            return await message.reply_text("Invalid duration format. Use numbers with optional m/h.")

    m3u8_url = parts[2]
    audio_only = "--audio-only" in parts
    video_only = "--video-only" in parts

    # Handle Filename (Everything after URL)
    filename_parts = [p for p in parts[3:] if not p.startswith('--')]
    if filename_parts:
        filename_mp4 = " ".join(filename_parts)
        if not filename_mp4.endswith(".mp4"):
            filename_mp4 += ".mp4"
    else:
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_mp4 = f"record_{chat_id}_{now_str}.mp4"

    final_output_path = os.path.join(OUTPUT_DIR, filename_mp4)
    job_id = os.urandom(8).hex()

    msg = await message.reply_text(f"🎬 Recording Job #{job_id} initialized!\n🔍 Fetching stream info (This may take a moment)...")

    try:
        # Added timeout to ffprobe to prevent hanging on dead links
proc = await asyncio.create_subprocess_exec(
    "ffprobe", "-v", "error",
    "-headers", "Referer: https://www.sonyliv.com/\r\nOrigin: https://www.sonyliv.com\r\nUser-Agent: Mozilla/5.0\r\n",
    "-show_streams", "-print_format", "json",
    "-rw_timeout", "10000000", m3u8_url,
    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
)
out, err = await asyncio.wait_for(proc.communicate(), timeout=20.0)
        
        if proc.returncode != 0:
            raise Exception(f"FFprobe error: {err.decode()}")
            
        info = json.loads(out.decode())
    except asyncio.TimeoutError:
        return await msg.edit_text("❌ Error: Stream URL timed out. The server is not responding.")
    except Exception as e:
        return await msg.edit_text(f"❌ Could not get stream info: {e}")

    videos, audios = [], []
    for stream in info.get("streams", []):
        if stream.get("codec_type") == "video" and stream.get("width") and stream.get("height"):
            videos.append({"index": stream["index"], "width": stream["width"], "height": stream["height"]})
        elif stream.get("codec_type") == "audio":
            audios.append({"index": stream["index"], "language": stream.get("tags", {}).get("language")})

    pending_states.setdefault(chat_id, {})[job_id] = {
        'msg_id': msg.id,
        'cancelled': False,
        'url': m3u8_url,
        'duration': duration,
        'filename': final_output_path,
        'videos': videos,
        'audios': audios,
        'selected_video': None,
        'selected_audios': [],
        'audio_only': audio_only,
        'video_only': video_only,
        'starter_user_id': user_id
    }

    if audio_only:
        if not audios:
            return await msg.edit_text("❌ No audio tracks found for `--audio-only`!")
        sel = pending_states[chat_id][job_id]
        sel['selected_video'] = None
        sel['selected_audios'] = [str(a['index']) for a in audios]
        job_details = {
            'chat_id': chat_id, 'job_id': job_id, 'url': sel["url"],
            'duration': sel["duration"], 'filename': sel["filename"],
            'video': sel['selected_video'], 'audios': sel['selected_audios'],
            'msg_id': sel['msg_id']
        }
        await job_queue.put(job_details)
        await msg.edit_text(f"✅ Audio-only job added to queue!\n"
                            f"Audios: {', '.join([a.get('language', 'Unknown') or 'Unknown' for a in audios])}",
                            reply_markup=InlineKeyboardMarkup(
                                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]])
                            )
    elif video_only:
        if not videos:
            return await msg.edit_text("❌ No video tracks found for `--video-only`!")
        sel = pending_states[chat_id][job_id]
        sel['selected_video'] = videos[0]
        sel['selected_audios'] = []
        job_details = {
            'chat_id': chat_id, 'job_id': job_id, 'url': sel["url"],
            'duration': sel["duration"], 'filename': sel["filename"],
            'video': sel['selected_video'], 'audios': [],
            'msg_id': sel['msg_id']
        }
        await job_queue.put(job_details)
        await msg.edit_text(f"✅ Video-only job added to queue!\n"
                            f"Video: {sel['selected_video']['width']}x{sel['selected_video']['height']}",
                            reply_markup=InlineKeyboardMarkup(
                                [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]])
                            )
    else:
        if not videos:
            return await msg.edit_text("❌ No video tracks found!")
        buttons = [[InlineKeyboardButton(f"🎥 {v['width']}x{v['height']} (v{v['index']})",
                                         callback_data=f"vidsel|{chat_id}|{job_id}|{v['index']}")] for v in videos]
        await msg.edit_text(f"Select Video Track for Job #{job_id}:", reply_markup=InlineKeyboardMarkup(buttons))


@app.on_callback_query(filters.regex("^vidsel"))
async def callback_video(client, callback_query):
    if callback_query.from_user.id not in APPROVED_USERS:
        return await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)
    _, chat_id_str, job_id_str, idx_str = callback_query.data.split("|")
    chat_id, job_id = int(chat_id_str), job_id_str
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        return await callback_query.answer("Session expired!", show_alert=True)
    sel["selected_video"] = next((v for v in sel["videos"] if str(v["index"]) == idx_str), None)
    if not sel["selected_video"]:
        return await callback_query.answer("Invalid selection!", show_alert=True)

    buttons = []
    if not sel["audios"]:
        buttons.append([InlineKeyboardButton("✅ Start Recording", callback_data=f"done|{chat_id}|{job_id}")])
        text = f"Selected Video: {sel['selected_video']['width']}x{sel['selected_video']['height']}\n\nNo audio tracks found. Proceed with video only?"
        await callback_query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    if not sel["selected_audios"]:
        if sel["audios"]:
            sel["selected_audios"] = [str(sel["audios"][0]["index"])]

    for a in sel["audios"]:
        is_selected = str(a['index']) in sel["selected_audios"]
        emoji = "✅ " if is_selected else "🔊 "
        buttons.append([InlineKeyboardButton(f"{emoji}{a.get('language', 'Unknown') or 'Unknown'} (a{a['index']})",
                                             callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}")] )

    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])

    await callback_query.message.edit_text(
        f"Selected Video: {sel['selected_video']['width']}x{sel['selected_video']['height']}\n"
        f"Select audio tracks for Job #{job_id} (multi-select). Tap ✅ Done when finished.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


@app.on_callback_query(filters.regex("^audsel"))
async def callback_audio(client, callback_query):
    if callback_query.from_user.id not in APPROVED_USERS:
        return await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)
    _, chat_id_str, job_id_str, idx_str = callback_query.data.split("|")
    chat_id, job_id = int(chat_id_str), job_id_str
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        return await callback_query.answer("Session expired!", show_alert=True)

    if idx_str in sel["selected_audios"]:
        sel["selected_audios"].remove(idx_str)
    else:
        sel["selected_audios"].append(idx_str)

    buttons = []
    for a in sel["audios"]:
        is_selected = str(a['index']) in sel["selected_audios"]
        emoji = "✅ " if is_selected else "🔊 "
        buttons.append([InlineKeyboardButton(f"{emoji}{a.get('language', 'Unknown') or 'Unknown'} (a{a['index']})",
                                             callback_data=f"audsel|{chat_id}|{job_id}|{a['index']}")] )

    buttons.append([InlineKeyboardButton("✅ Done", callback_data=f"done|{chat_id}|{job_id}")])

    await callback_query.message.edit_text(
        f"Selected Video: {sel['selected_video']['width']}x{sel['selected_video']['height']}\n"
        f"Select audio tracks for Job #{job_id} (multi-select). Tap ✅ Done when finished.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

    await callback_query.answer("Audio selection updated.", show_alert=False)


@app.on_callback_query(filters.regex("^done"))
async def callback_done(client, callback_query):
    if callback_query.from_user.id not in APPROVED_USERS:
        return await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)
    _, chat_id_str, job_id_str = callback_query.data.split("|")
    chat_id, job_id = int(chat_id_str), job_id_str
    sel = pending_states.get(chat_id, {}).get(job_id)
    if not sel:
        return await callback_query.answer("Session expired!", show_alert=True)

    video = sel.get("selected_video")
    audio_maps = sel["selected_audios"]

    job_details = {
        'chat_id': chat_id, 'job_id': job_id, 'url': sel["url"],
        'duration': sel["duration"], 'filename': sel["filename"],
        'video': video, 'audios': audio_maps, 'msg_id': sel['msg_id']
    }

    await job_queue.put(job_details)

    audio_names = [a.get('language', 'Unknown') or 'Unknown' for a in sel['audios'] if
                   str(a['index']) in sel['selected_audios']]
    if not audio_names:
        audio_names = ['None']

    video_info = f"Video: {video['width']}x{video['height']}" if video else "Video: None"

    await callback_query.message.edit_text(
        f"✅ Recording Job #{job_id} added to queue!\n"
        f"{video_info}\n"
        f"Audios: {', '.join(audio_names)}\n"
        f"Mode: Direct Stream Copy\n\n"
        "Your recording will start soon. Please wait.",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("🎬 Cancel", callback_data=f"cancel|{chat_id}|{job_id}")]])
    )


@app.on_callback_query(filters.regex("^cancel"))
async def callback_cancel(client, callback_query):
    if callback_query.from_user.id not in APPROVED_USERS:
        return await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)
    _, chat_id_str, job_id_str = callback_query.data.split("|")
    chat_id, job_id = int(chat_id_str), job_id_str

    job_state = pending_states.get(chat_id, {}).get(job_id)
    if not job_state:
        await callback_query.answer("Job not found!", show_alert=True)
        return await callback_query.message.edit_text(
            "❌ Error: The job no longer exists or has already been completed/cancelled.")

    starter_user_id = job_state.get('starter_user_id')
    if starter_user_id and callback_query.from_user.id != starter_user_id:
        await callback_query.answer("🚫 You can only cancel a recording you started.", show_alert=True)
        return

    await callback_query.answer("Recording is being cancelled...", show_alert=False)
    await callback_query.message.edit_text("❌ Recording job is being cancelled...")

    job_state['cancelled'] = True

    if job_id in running_jobs:
        proc = running_jobs[job_id]
        print(f"Sending SIGTERM to FFmpeg process for graceful shutdown...")
        try:
            proc.terminate()
        except:
            pass
        
        # Wait a bit then kill if needed
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            print(f"FFmpeg process did not exit gracefully within 10 seconds, sending SIGKILL...")
            try:
                proc.kill()
            except:
                pass
            await proc.wait()

        if job_id in running_jobs:
            del running_jobs[job_id]

        # Cleanup TS files if running
        file_path = job_state['filename']
        if os.path.exists(file_path): os.remove(file_path)
        if os.path.exists(file_path + ".ts"): os.remove(file_path + ".ts")

        await callback_query.message.edit_text("❌ Recording stopped! Partial file has been deleted.")

    else:
        # Remove from queue logic (simplified)
        found_in_queue = False
        queue_items = []
        while not job_queue.empty():
            item = await job_queue.get()
            if item['job_id'] == job_id:
                found_in_queue = True
            else:
                queue_items.append(item)
        for item in queue_items:
            await job_queue.put(item)
        if found_in_queue:
            await callback_query.message.edit_text("❌ Recording job was cancelled from the queue. No file was created.")
        else:
            await callback_query.message.edit_text(
                "❌ Error: Could not find job to cancel. It may have already been completed.")


@app.on_message(filters.command("cancel") & (filters.private | filters.group))
@is_approved_user
async def cancel_handler(client, message):
    if not running_jobs and job_queue.empty():
        return await message.reply_text("❌ No active jobs to cancel.")
    buttons = []
    for job_id in running_jobs.keys():
        buttons.append([InlineKeyboardButton(f"🎬 Cancel Running Job #{job_id[:8]}...",
                                             callback_data=f"cancel|{message.chat.id}|{job_id}")])
    queue_items = list(job_queue._queue)
    for item in queue_items:
        job_id = item['job_id']
        buttons.append([InlineKeyboardButton(f"🕰️ Cancel Queued Job #{job_id[:8]}...",
                                             callback_data=f"cancel|{message.chat.id}|{job_id}")])
    if buttons:
        await message.reply_text("Select a job to cancel:", reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await message.reply_text("❌ No active jobs to cancel.")


@app.on_message(filters.command("status") & (filters.private | filters.group))
@is_approved_user
async def status_handler(client, message):
    queue_size = job_queue.qsize()
    running_size = len(running_jobs)
    if queue_size == 0 and running_size == 0:
        await message.reply_text("✅ Bot is idle. No jobs are currently running or in queue.")
    else:
        status_text = "📊 **Bot Status**\n\n"
        status_text += f"🎬 **Running Jobs:** {running_size}\n"
        status_text += f"🕰️ **Jobs in Queue:** {queue_size}\n\n"
        if running_size > 0:
            status_text += "---**Running Jobs Details**---\n"
            for job_id in running_jobs.keys():
                status_text += f"**Job ID:** `{job_id[:8]}`\n"
        await message.reply_text(status_text, parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("approve") & filters.private)
@is_owner
async def approve_handler(client, message):
    global APPROVED_USERS, redis_client
    try:
        user_id = int(message.text.split()[1])
        # Add user to Redis set
        if redis_client:
            redis_client.sadd("approved_users", str(user_id))
            # Refresh the local approved users list
            APPROVED_USERS = [int(uid) for uid in redis_client.smembers("approved_users")]
        else:
            APPROVED_USERS.append(user_id)

        await message.reply_text(f"✅ User `{user_id}` has been approved.", parse_mode=enums.ParseMode.MARKDOWN)
    except (IndexError, ValueError):
        await message.reply_text("Usage: `/approve <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply_text(f"❌ Error approving user: {e}", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("unapprove") & filters.private)
@is_owner
async def unapprove_handler(client, message):
    global APPROVED_USERS, redis_client
    try:
        user_id = int(message.text.split()[1])
        if user_id in OWNER_IDS:
            return await message.reply_text("🚫 You cannot unapprove an owner.", parse_mode=enums.ParseMode.MARKDOWN)

        if redis_client:
            redis_client.srem("approved_users", str(user_id))
            APPROVED_USERS = [int(uid) for uid in redis_client.smembers("approved_users")]
        else:
            if user_id in APPROVED_USERS:
                APPROVED_USERS.remove(user_id)

        await message.reply_text(f"❌ User `{user_id}` has been unapproved.", parse_mode=enums.ParseMode.MARKDOWN)
    except (IndexError, ValueError):
        await message.reply_text("Usage: `/unapprove <user_id>`", parse_mode=enums.ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply_text(f"❌ Error unapproving user: {e}", parse_mode=enums.ParseMode.MARKDOWN)


@app.on_message(filters.command("list") & (filters.private | filters.group))
@is_approved_user
async def list_files_handler(client, message):
    # Show files sorted by modified time (newest first)
    files = [f for f in os.listdir(OUTPUT_DIR) if
             os.path.isfile(os.path.join(OUTPUT_DIR, f)) and not f.startswith(".") and f.endswith(".mp4")]
    if not files:
        return await message.reply_text("❌ No recorded files found in storage.")

    files.sort(key=lambda x: os.path.getmtime(os.path.join(OUTPUT_DIR, x)), reverse=True)

    text = "**📂 Recorded Files (Last 10):**\n\n"
    buttons = []

    for file_name in files[:10]:
        text += f"`{file_name}`\n"
        buttons.append([InlineKeyboardButton(f"🔄 Reupload `{file_name}`", callback_data=f"reupload|{file_name}")])

    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode=enums.ParseMode.MARKDOWN)


@app.on_callback_query(filters.regex("^reupload"))
async def callback_reupload(client, callback_query):
    if callback_query.from_user.id not in APPROVED_USERS:
        return await callback_query.answer("🚫 You are not approved to use this bot.", show_alert=True)

    _, file_name = callback_query.data.split("|")
    file_path = os.path.join(OUTPUT_DIR, file_name)

    if not os.path.exists(file_path):
        await callback_query.answer("❌ File not found.", show_alert=True)
        return await callback_query.message.edit_text("❌ File not found on server.")

    await callback_query.answer("Re-upload started.", show_alert=True)
    await callback_query.message.edit_text(f"🔄 Processing `{file_name}` for upload (Repairing metadata)...")

    # Fix metadata before manual reupload (Fixes 'Unknown' duration and no thumb issue)
    fix_video_metadata(file_path)

    duration_secs, width, height = get_video_attributes(file_path)
    duration_str = str(timedelta(seconds=duration_secs)) if duration_secs else "Unknown"

    caption = (
        f"**📺 Stream Recording (Re-upload):**\n\n"
        f"**📁 File Name:** `{file_name}`\n"
        f"**⏱ Duration:** `{duration_str}`\n"
        f"**📏 Res:** `{width}x{height}p`\n\n"
        f"**{BRANDING}**"
    )

    thumb_path = generate_thumbnail(file_path)
    await upload_file_with_progress(client, callback_query.message.chat.id, file_path, caption, thumb_path,
                                    msg_id=callback_query.message.id)


@app.on_message(filters.command("delete") & (filters.private | filters.group))
@is_approved_user
async def delete_file_handler(client, message):
    try:
        file_name = " ".join(message.command[1:])
        file_path = os.path.join(OUTPUT_DIR, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            await message.reply_text(f"✅ File `{file_name}` has been deleted.", parse_mode=enums.ParseMode.MARKDOWN)
        else:
            await message.reply_text(f"❌ File `{file_name}` not found.", parse_mode=enums.ParseMode.MARKDOWN)
    except IndexError:
        await message.reply_text("Usage: `/delete <file_name>`", parse_mode=enums.ParseMode.MARKDOWN)


# ===== Bot Start Function =====
async def start_bot():
    load_approved_users()
    num_workers = 5  # Multiple workers for parallel processing
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await app.start()
    print("✅ Pyrogram Bot started.")
    for owner_id in OWNER_IDS:
        try:
            await app.send_message(owner_id, "Pyrogram Bot is running!")
        except:
            pass
    await asyncio.gather(*workers)


if __name__ == "__main__":
    if not REDIS_URL:
        print("⚠️ Warning: REDIS_URL is not set. Persistence will be disabled.")

    try:
        app.run(start_bot())
    except KeyboardInterrupt:
        print("Bot is shutting down.")
