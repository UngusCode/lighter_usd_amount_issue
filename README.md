# Lighter Zero USD Amount Detector

Small script that listens to a websocket trade feed and logs trades where `usd_amount` is zero while `price` and `size` are non-zero.

It always subscribes to these markets: `2050`, `2049`, `2053`, `1`.

Fixed websocket settings:

- `wss://mainnet.zklighter.elliot.ai/stream`
- Origin `https://mainnet.zklighter.elliot.ai`
- `readonly=true`

## Requirements

- Python 3.11+
- `uv`

## Setup

```bash
uv venv --python 3.11
uv pip install websockets
```

## Usage

```bash
source .venv/bin/activate
python detect_zero_usd_amount.py
```

Optional flags:

- `--channel-format` `slash` or `colon`
- `--max-messages` stop after N trade messages
- `--duration-secs` stop after N seconds
- `--file` scan a JSONL capture file instead of live websocket
