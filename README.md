# M3U8 Recording Bot

Telegram bot for recording HLS/M3U8 streams with:

- video track selection
- multi-audio selection
- queue system
- reconnect-aware FFmpeg recording
- incomplete-recording rejection
- upload progress
- automatic local file deletion after upload

## Commands

- `/start`
- `/record <duration> <m3u8_url> [file_name] [--audio-only] [--video-only]`
- `/cancel`
- `/status`
- `/list`
- `/delete <file_name>`
- `/reupload <file_name>`

Examples:

- `/record 5m https://example.com/live.m3u8 movie.mp4`
- `/record 2h https://example.com/live.m3u8 match.mp4`
- `/record 20m https://example.com/live.m3u8 song.mp4 --audio-only`

## Environment Variables

- `API_ID`
- `API_HASH`
- `BOT_TOKEN`
- `OWNER_IDS`
- `REDIS_URL` (optional)
- `FSUB_CHANNEL` (optional)
- `BRANDING` (optional)
- `MAX_FILE_SIZE_GB` (optional, default `1.9`)
- `MAX_JOBS_PER_USER` (optional, default `2`)
- `NUM_WORKERS` (optional, default `2`)
- `AUTO_DELETE_AFTER_UPLOAD` (optional, default `true`)

## Notes

- Uploaded recordings are deleted locally after upload.
- Incomplete recordings are rejected instead of uploading a half file.
- Dockerfile and Nixpacks config both install FFmpeg.
