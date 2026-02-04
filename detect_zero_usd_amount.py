#!/usr/bin/env python3
import argparse
import asyncio
import json
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List

MARKETS = [2050, 2049, 2053, 1]
WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
WS_ORIGIN = "https://mainnet.zklighter.elliot.ai"
WS_READONLY = True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Listen to the live websocket feed and log trades with usd_amount=0 while price and size are non-zero. "
            "Fixed markets: 2050, 2049, 2053, 1."
        )
    )
    parser.add_argument(
        "--channel-format",
        choices=["slash", "colon"],
        default="slash",
        help="Channel format: slash or colon",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N update/trade messages (0 = no limit)",
    )
    parser.add_argument(
        "--duration-secs",
        type=int,
        default=60,
        help="Max duration to run (seconds)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Optional JSONL file to scan instead of live websocket",
    )
    return parser.parse_args()


def apply_readonly_param(url: str, readonly: bool) -> str:
    if not readonly or "readonly=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}readonly=true"


def parse_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def build_subscriptions(markets: List[int], channel_format: str) -> List[str]:
    if channel_format == "colon":
        return [f"trade:{market}" for market in markets]
    return [f"trade/{market}" for market in markets]


def iter_trades_from_message(message: str):
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return
    if payload.get("type") != "update/trade":
        return
    for trade in payload.get("trades", []):
        yield trade


def log_trade(trade: Dict[str, Any]):
    print(
        "zero_usd",
        f"market_id={trade.get('market_id')}",
        f"tx_hash={trade.get('tx_hash')}",
        f"size={trade.get('size')}",
        f"price={trade.get('price')}",
    )


def scan_file(path: str):
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"missing {file_path}")
    hits = 0
    lines = 0
    for line in file_path.read_text().splitlines():
        if not line.strip():
            continue
        lines += 1
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg_text = rec.get("message")
        if not msg_text:
            continue
        for trade in iter_trades_from_message(msg_text):
            usd_amount = parse_decimal(trade.get("usd_amount"))
            if usd_amount is None or usd_amount != 0:
                continue
            size_val = parse_decimal(trade.get("size"))
            price_val = parse_decimal(trade.get("price"))
            if size_val in (None, 0) or price_val in (None, 0):
                continue
            hits += 1
            log_trade(trade)
    print(f"scanned_lines={lines} hits={hits}")


async def run_live(args: argparse.Namespace):
    try:
        import websockets  # type: ignore
    except Exception:
        raise SystemExit("missing websockets module. Install with: python3 -m pip install websockets")

    url = apply_readonly_param(WS_URL, WS_READONLY)
    origin = WS_ORIGIN
    user_agent = None
    channel_format = args.channel_format

    channel_format = (channel_format or "slash").lower()
    markets = MARKETS

    headers = []
    if origin:
        headers.append(("Origin", origin))
    if user_agent:
        headers.append(("User-Agent", user_agent))

    subs = build_subscriptions(markets, channel_format)
    print(f"subscribing to {len(subs)} markets")

    start = time.monotonic()
    messages = 0
    hits = 0

    connect_kwargs = {"ping_interval": None}
    if headers:
        # websockets >= 12 uses additional_headers, older uses extra_headers
        connect_kwargs["additional_headers"] = headers
    try:
        ws_ctx = websockets.connect(url, **connect_kwargs)
    except TypeError:
        connect_kwargs.pop("additional_headers", None)
        connect_kwargs["extra_headers"] = headers
        ws_ctx = websockets.connect(url, **connect_kwargs)
    async with ws_ctx as ws:
        for channel in subs:
            await ws.send(json.dumps({"type": "subscribe", "channel": channel}))

        while True:
            if args.max_messages and messages >= args.max_messages:
                break
            if args.duration_secs and time.monotonic() - start > args.duration_secs:
                break
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue

            if isinstance(message, bytes):
                try:
                    message = message.decode("utf-8")
                except Exception:
                    continue

            if not isinstance(message, str):
                continue

            if '"type"' in message and '"ping"' in message:
                try:
                    if json.loads(message).get("type") == "ping":
                        await ws.send('{"type":"pong"}')
                        continue
                except json.JSONDecodeError:
                    pass

            found_trade = False
            for trade in iter_trades_from_message(message):
                found_trade = True
                usd_amount = parse_decimal(trade.get("usd_amount"))
                if usd_amount is None or usd_amount != 0:
                    continue
                size_val = parse_decimal(trade.get("size"))
                price_val = parse_decimal(trade.get("price"))
                if size_val in (None, 0) or price_val in (None, 0):
                    continue
                hits += 1
                log_trade(trade)
            if found_trade:
                messages += 1

    print(f"trade_messages={messages} hits={hits}")


def main():
    args = parse_args()
    if args.file:
        scan_file(args.file)
        return
    asyncio.run(run_live(args))


if __name__ == "__main__":
    main()
