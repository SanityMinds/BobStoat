# Bob AI for Stoat

Bob is an asynchronous AI client for [Stoat](https://stoat.chat/) that runs from a user session. It supports conversational replies, persistent channel history, configurable personalities, image understanding and generation, text-to-speech, administrative controls, and optional proactive messaging.

This repository uses `bot.py` as the application entry point and `supervisor.py` as the recommended process manager.

## Important notice

This project signs in with a Stoat **user session token**, not a conventional bot token.
Self botting is against TOS so use at your own risk.

## Features

- Asynchronous Stoat gateway connection through `stoat.py`
- Replies in servers, one-to-one DMs, and group DMs
- Mention, reply, keyword, random, and conversational follow-up triggers
- Per-channel queues and server-wide concurrency limits
- SQLite conversation history, channel settings, access rules, and schedules
- NanoGPT-backed chat, vision, usage reporting, and text-to-speech
- Dezgo image generation and image editing
- Built-in and custom personalities
- Text, TTS, and hybrid response modes
- Automatic follow-up DMs to added friends, server messages, and friend requests when enabled
- Administrator-only authorization, blocking, channel-send, and DM commands
- Rotating application, error, and supervisor logs
- Cross-platform supervisor with restart backoff and duplicate-instance locking

## Project files

| File | Purpose |
| --- | --- |
| `bot.py` | Main Stoat application |
| `supervisor.py` | Starts, monitors, restarts, and stops `bot.py` |
| `.env` | bot configurations |
| `requirements.txt` | Python dependencies |
| `conversation_history.db` | SQL database created on start-up |
| `bot.log` | created automatically |
| `errors.log` | created automatically |
| `supervisor.log` | created automatically |

## Requirements

- Python 3.10 or newer
- A valid Stoat user session token
- A NanoGPT API key
- A Dezgo API key for image generation and editing
- `ffmpeg` available on `PATH` when TTS conversion is enabled
- Network access to Stoat and the configured AI service endpoints

The Python dependencies are:

- `stoat.py==1.2.1`
- `aiohttp==3.14.1`
- `psutil==7.2.2`

Their transitive dependencies are installed automatically by pip.

## Installation

### Linux and macOS

```bash
git clone https://github.com/SanityMinds/BobStoat
cd BobStoat
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Install `ffmpeg` using the operating system package manager if TTS is enabled. For example, on Debian or Ubuntu:

```bash
sudo apt update
sudo apt install ffmpeg
```

### Windows PowerShell

```powershell
git clone https://github.com/SanityMinds/BobStoat
Set-Location BobStoat
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Install `ffmpeg`, add it to `PATH`, and confirm it is available:

```powershell
ffmpeg -version
```

## Configuration

Edit `.env` before launching the bot. At minimum, set:

```env
STOAT_USER_TOKEN=
NANO_GPT_API_KEY=
DEZGO_API_KEY=
ADMIN_USER=
```

`ADMIN_USER` accepts either an exact `username#discriminator` or a Stoat user ULID. `ADMIN_USER_ID` is retained as a fallback compatibility option. Do not set both to different accounts.

Environment variables already present in the process take precedence over values in `.env`. This matters when running through systemd, a hosting panel, Docker, or an exported shell variable. If the value in `.env` appears to be ignored, check the service environment.

### Credentials and access

| Setting | Description |
| --- | --- |
| `STOAT_USER_TOKEN` | Private Stoat user session token |
| `NANO_GPT_API_KEY` | Private NanoGPT API key |
| `DEZGO_API_KEY` | Private Dezgo API key |
| `ADMIN_USER` | Exact administrator tag or ULID |
| `ADMIN_USER_ID` | Legacy administrator ULID fallback |
| `BLOCKLIST_USER_IDS` | Comma-separated users or bots Bob must ignore |
| `SERVER_BLACKLIST_IDS` | Comma-separated servers Bob must ignore |
| `BYPASS_USER_IDS` | Comma-separated users eligible for configured bypass behavior |

Sensitive placeholders are blank in `.env.example`. Keep them blank in public copies.

### Models and API endpoints

`DEFAULT_CHAT_MODEL`, `INSANE_CHAT_MODEL`, `IMAGE_VISION_MODEL`, `TTS_MODEL`, and `TTS_VOICE` select the models and voice used by each feature. `CHAT_THINKING_SUFFIX`, `CHAT_REASONING_EFFORT`, and `CHAT_REASONING_MAX_TOKENS` control reasoning-model requests.

The endpoint settings are `NANO_GPT_BASE_URL`, `NANO_GPT_SUBSCRIPTION_USAGE_URL`, `NANO_GPT_BALANCE_URL`, `NANO_GPT_TTS_URL`, and `DEZGO_BASE_URL`. Change them only when using compatible services.

`IMAGE_VISION_SYSTEM_PROMPT`, `SERVER_OWNER_JOIN_DM_TEMPLATE`, and `SERVER_HIGHEST_ROLE_JOIN_DM_TEMPLATE` control generated context and automatic join messages.

### Request limits and history

The following settings control API reliability and prompt size:

- `API_TIMEOUT_SECONDS`, `API_CONNECT_TIMEOUT_SECONDS`, and `API_MAX_ATTEMPTS`
- `CHAT_API_TIMEOUT_SECONDS` and `CHAT_API_MAX_ATTEMPTS`
- `RETRYABLE_HTTP_STATUSES`
- `DEFAULT_CHAT_MAX_TOKENS`, `SHORT_CHAT_MAX_TOKENS`, and `LONG_CHAT_MAX_TOKENS`
- `IMAGE_VISION_MAX_TOKENS`
- `MAX_HISTORY_USER_MESSAGES`, `MAX_HISTORY_TOTAL_CHARS`, and `MAX_HISTORY_ENTRY_CHARS`
- `MAX_STORED_HISTORY_MESSAGES`
- `MAX_SYSTEM_MESSAGE_CHARS`, `MAX_INCOMING_MESSAGE_CHARS`, and `MAX_TRIGGERED_MESSAGE_CHARS`
- `MAX_COMBINED_TRIGGERED_MESSAGE_CHARS` and `MAX_RECENT_CONTEXT_LINE_CHARS`
- `MAX_VISION_USER_MESSAGE_CHARS`, `MAX_IMAGE_DESCRIPTION_CHARS`, and `MAX_OUTGOING_IMAGE_PROMPT_CHARS`
- `MAX_MEMORY_RECALL_ITEMS`, `MAX_MEMORY_RECALL_CHARS`, `MAX_MEMORY_FACT_CHARS`, and `MAX_MEMORY_ITEMS_PER_CHANNEL`
- `MAX_IMAGE_URLS_FOR_VISION` and `MAX_TRACKED_CHANNELS`

Reducing these values lowers request cost and memory use. Increasing them can increase latency and API usage.

### Reply triggers, queues, and rate limits

`RESPOND_TO_SERVER_KEYWORDS` enables `SERVER_TRIGGER_KEYWORDS`. Keywords are comma-separated and matched in eligible server and group-DM messages. `UNPROMPTED_SERVER_REPLY_CHANCE` is a value from `0` to `1`; set it to `0` to disable random replies.

Queue and concurrency settings include:

- `MAX_CONCURRENT_HANDLES`
- `MAX_SERVER_CONCURRENT_REPLIES`
- `MAX_CHANNEL_QUEUE_SIZE`
- `ACTIVE_CHANNEL_QUEUE_SIZE`
- `MAX_OUTBOUND_MESSAGE_CHARS`
- `CHANNEL_MESSAGE_GAP_SECONDS`
- `PING_BATCH_WINDOW_SECONDS`

Cooldown and burst settings include:

- `USER_TRIGGER_COOLDOWN_SECONDS`
- `CHANNEL_REPLY_COOLDOWN_SECONDS`
- `SERVER_REPLY_COOLDOWN_SECONDS`
- `GLOBAL_REPLY_COOLDOWN_SECONDS`
- `SERVER_REPLY_BURST_WINDOW_SECONDS` and `SERVER_REPLY_BURST_LIMIT`
- `GLOBAL_REPLY_BURST_WINDOW_SECONDS` and `GLOBAL_REPLY_BURST_LIMIT`
- `MULTI_SERVER_REPLY_WINDOW_SECONDS` and `MULTI_SERVER_REPLY_LIMIT`
- `DUPLICATE_MESSAGE_COOLDOWN_SECONDS` and `DUPLICATE_MESSAGE_SIMILARITY`
- `ENABLE_COOLDOWN_BYPASS`

Follow-up and activity settings include:

- `ENABLE_SERVER_FOLLOWUPS`
- `CONVERSATION_FOLLOWUP_WINDOW_SECONDS`
- `MAX_FOLLOWUP_MESSAGES`
- `REPLY_REVISION_WINDOW_SECONDS`
- `DIRECT_MESSAGE_REPLY_DELAY_SECONDS`
- `CHANNEL_ACTIVITY_WINDOW_SECONDS`
- `CHANNEL_ACTIVITY_SILENCE_MIN_MESSAGES`
- `CHANNEL_ACTIVITY_SILENCE_MAX_MESSAGES`
- `CHANNEL_ACTIVITY_THRESHOLD_REFRESH_SECONDS`

One-to-one DMs use a quiet-period debounce. Servers and group DMs use queues, activity detection, and a post-generation revision window that can merge additional messages before the final reply is sent.

### Typing behavior

`TYPING_MIN_DELAY_SECONDS`, `TYPING_MAX_DELAY_SECONDS`, `TYPING_FULL_LENGTH_CHARS`, `TYPING_JITTER_SECONDS`, and `TYPING_REFRESH_SECONDS` control visible typing duration and refresh behavior.

`TYPING_BASE_DELAY_SECONDS` and `TYPING_CHARS_PER_SECOND` remain in the configuration template for compatibility but are not read by the current release.

### TTS, images, and reactions

TTS settings:

- `TTS_ENABLED`
- `TTS_MODEL`, `TTS_VOICE`, `TTS_SPEED`, and `TTS_RESPONSE_FORMAT`
- `TTS_MAX_CHARACTERS` and `TTS_SAFE_CHARACTER_LIMIT`
- `TTS_COMMAND_COOLDOWN_SECONDS`

Image settings:

- `DEZGO_IMAGE_WIDTH`, `DEZGO_IMAGE_HEIGHT`, and `DEZGO_IMAGE_STEPS`
- `DEZGO_EDIT_IMAGE_STEPS`, `DEZGO_EDIT_IMAGE_SAMPLER`, and `DEZGO_EDIT_IMAGE_UPSCALE`
- `DEZGO_EDIT_IMAGE_GUIDANCE` and `DEZGO_EDIT_IMAGE_GUIDANCE_STRENGTH`
- `DEZGO_EDIT_TIMEOUT_SECONDS` and `DEZGO_EDIT_IMAGE_NEGATIVE_PROMPT`

`IMAGE_ENABLED` 

Hybrid and reaction settings are `HYBRID_RANDOM_IMAGE_CHANCE`, `HYBRID_RANDOM_TTS_CHANCE`, `AI_REACTION_CHANCE`, `AI_REACTION_CHECK_EVERY_MESSAGES`, `AI_REACTION_MAX_MESSAGE_CHARS`, `AI_REACTION_MAX_TOKENS`, and `ENABLE_AUTOMATIC_REACTIONS`.

### Automatic outbound activity


- `ENABLE_RANDOM_SERVER_MESSAGES`
- `ENABLE_RANDOM_FRIEND_DMS`
- `ENABLE_RANDOM_SERVER_FRIEND_REQUESTS`
- `AUTOMATIC_OUTBOUND_INTERVAL_MINUTES`
- `AUTOMATIC_OUTBOUND_STAGGER_SECONDS`
- `AUTOMATIC_DM_RECIPIENT_COOLDOWN_MINUTES`
- `RANDOM_SERVER_FRIEND_REQUEST_INTERVAL_MINUTES`
- `RANDOM_SERVER_FRIEND_REQUESTS_PER_RUN`
- `AUTOMATIC_FRIEND_REQUEST_RECIPIENT_COOLDOWN_HOURS`
- `FRIEND_REQUEST_LIMIT_ROTATION_COOLDOWN_HOURS`
- `ENABLE_DM_CHECKUPS`
- `CHECKUP_INTERVAL_HOURS`
- `ENABLE_JOIN_DMS`
- `SERVER_JOIN_DM_OWNER_RETRY_DELAYS_SECONDS`


### Commands and administrator behavior

`COMMAND_COOLDOWN_SECONDS` applies to public commands other than TTS. `PERSONALITY_CHANGE_CHANNEL_COOLDOWN_SECONDS` applies to personality-changing commands. Administrator commands have no command cooldown.

`ENABLE_ADMIN_OUTBOUND_COMMANDS` controls `!.!send` and `!.!dm`.

### Storage, logs, status, and supervisor

| Setting | Description |
| --- | --- |
| `DATABASE_PATH` | SQLite database path |
| `ERROR_LOG_FILE` | Structured error log path |
| `ERROR_LOG_VALUE_LIMIT` | Maximum logged metadata value length |
| `ERROR_LOG_BODY_LIMIT` | Maximum logged response-body length |
| `STATUS_ROTATION` | Pipe-separated `status:hours` rotation entries |
| `SUPERVISOR_RESTART_DELAY_SECONDS` | Initial restart delay |
| `SUPERVISOR_MAX_RESTART_DELAY_SECONDS` | Maximum exponential backoff delay |
| `SUPERVISOR_STABLE_RUN_SECONDS` | Runtime required to reset restart backoff |
| `SUPERVISOR_SHUTDOWN_TIMEOUT_SECONDS` | Graceful shutdown timeout before force-kill |

Relative paths are resolved from the project directory.

## Running the bot

The recommended command is:

```bash
python supervisor.py
```

The supervisor:

- launches `bot.py` with the current Python interpreter;
- prevents a second supervisor from acquiring the same `supervisor.lock`;
- combines the child's stdout and stderr into `supervisor.log`;
- restarts failed processes using exponential backoff;
- forwards graceful shutdown signals to the bot process group; and
- force-stops the group if graceful shutdown exceeds the configured timeout.

Run once without restart behavior:

```bash
python supervisor.py --once
```

Run another entry point:

```bash
python supervisor.py --bot another_bot.py
```

Run without the supervisor for debugging:

```bash
python -u bot.py
```

Stop the supervisor with `Ctrl+C` or send it `SIGTERM`. Do not start `bot.py` separately while the supervisor is active.

## Linux systemd example

Create `/etc/systemd/system/bob-stoat.service` and adjust the paths and user:

```ini
[Unit]
Description=Bob AI for Stoat
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bob
WorkingDirectory=/opt/bob-stoat
ExecStart=/opt/bob-stoat/.venv/bin/python /opt/bob-stoat/supervisor.py
Restart=no
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

The supervisor already manages bot restarts, so the example leaves systemd restart behavior disabled.

Enable and inspect the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now bob-stoat
sudo systemctl status bob-stoat
sudo journalctl -u bob-stoat -f
```

## Commands

All commands begin with `!.!`.

### Public commands

| Command | Description |
| --- | --- |
| `!.!help` | Show the public command list |
| `!.!reset` | Clear channel conversation history and restore the default personality |
| `!.!short` | Use shorter responses |
| `!.!long` | Allow longer responses |
| `!.!normal` | Reset history and select the default chat model |
| `!.!insane` | Reset history and select the alternate long-form model |
| `!.!channel` | Show current channel model, mode, personality, and response type |
| `!.!personality [name]` | Reset history and select a built-in personality |
| `!.!personalities` | List built-in personalities |
| `!.!custom [description]` | Reset history and set a custom channel personality |
| `!.!type [text\|tts\|hybrid]` | Select channel response output; TTS is available only when enabled |
| `!.!tts [text]` | Generate a voice attachment |
| `!.!generate [prompt]` | Generate an image |
| `!.!edit [prompt]` | Edit an attached PNG or JPEG |
| `!.!system` | Show host, network, API usage, and balance information |

### Administrator commands

Administrator commands are intentionally omitted from `!.!help` and require the account configured by `ADMIN_USER` or `ADMIN_USER_ID`.

| Command | Description |
| --- | --- |
| `!.!authorize [user ID]` | Authorize a user or bot and remove it from the local blocklist |
| `!.!block [user ID]` | Make Bob ignore a user or bot; this does not ban them from a server |
| `!.!send [channel ID] [instruction]` | Generate and send a message to a channel |
| `!.!dm [username#discriminator] [instruction]` | Resolve an exact added-friend username tag and send a generated DM |

`!.!send` and `!.!dm` require `ENABLE_ADMIN_OUTBOUND_COMMANDS=true`. The DM target must already be an added friend. Follow-up command variants are not supported; every invocation must include its destination.

