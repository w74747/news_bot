"""
news_bot.py — Production-Ready Telegram Crypto Channel Automation Bot
Modules:
  1. Morning Market Report (daily @ 08:00)
  2. Real-Time Breaking News Aggregator (every 5 min)
  3. Whale Movements Watcher (every 10 min)
"""

import asyncio
import logging
import os
import hashlib
import time
import json
import re
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import feedparser
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Bot
from telegram.error import TelegramError

# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("news_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("news_bot")


# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────
BOT_TOKEN: str = os.environ.get("PUBLIC_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
CHANNEL_ID: str = os.environ.get("PUBLIC_CHANNEL_ID", "@YourChannelUsername")
CMC_API_KEY: Optional[str] = os.environ.get("COINMARKETCAP_API_KEY")  # optional

# DeepSeek API — used to translate/summarize headlines into Arabic with
# a short expert-style market impact note.
DEEPSEEK_API_KEY: Optional[str] = os.environ.get("DEEPSEEK_API_KEY")
DEEPSEEK_API_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

# News RSS feeds
RSS_FEEDS = [
    "https://cointelegraph.com/rss",
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://feeds.feedburner.com/CoinDesk",
    "https://cryptopanic.com/news/rss/",
]

# Whale Alert free public endpoint
WHALE_ALERT_API = "https://api.whale-alert.io/v1/transactions"
WHALE_ALERT_KEY: Optional[str] = os.environ.get("WHALE_ALERT_API_KEY")
WHALE_MIN_VALUE_USD = 10_000_000  # $10M threshold

# In-memory dedup store
processed_news_ids: set[str] = set()
hourly_news_buffer: list[dict] = []  # Buffer for hourly news aggregation
last_whale_cursor: int = int(time.time()) - 600  # last 10 min on first run


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def _fmt_number(n: float, decimals: int = 2) -> str:
    """Format large numbers with K/M/B suffix."""
    if n >= 1_000_000_000_000:
        return f"{n / 1_000_000_000_000:.{decimals}f}T"
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.{decimals}f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.{decimals}f}M"
    if n >= 1_000:
        return f"{n / 1_000:.{decimals}f}K"
    return f"{n:.{decimals}f}"


def _change_emoji(change: float) -> str:
    return "🟢" if change >= 0 else "🔴"


async def _safe_send(bot: Bot, text: str) -> bool:
    """Send a message with retry logic."""
    for attempt in range(1, 4):
        try:
            await bot.send_message(
                chat_id=CHANNEL_ID,
                text=text,
                parse_mode="HTML",
            )
            return True
        except TelegramError as e:
            logger.warning(f"Telegram send attempt {attempt} failed: {e}")
            if attempt < 3:
                await asyncio.sleep(5 * attempt)
    logger.error("All Telegram send attempts failed.")
    return False


# ─────────────────────────────────────────────
# MODULE 1: MORNING MARKET REPORT
# ─────────────────────────────────────────────
async def fetch_coingecko_prices(session: aiohttp.ClientSession) -> dict:
    """Fetch BTC, ETH, SOL prices + market cap from CoinGecko (free, no key)."""
    url = (
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum,solana"
        "&vs_currencies=usd"
        "&include_24hr_change=true"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        logger.error(f"CoinGecko price fetch error: {e}")
        return {}


async def fetch_global_market(session: aiohttp.ClientSession) -> dict:
    """Fetch total market cap & volume from CoinGecko."""
    url = "https://api.coingecko.com/api/v3/global"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("data", {})
    except Exception as e:
        logger.error(f"CoinGecko global fetch error: {e}")
        return {}


async def fetch_fear_and_greed(session: aiohttp.ClientSession) -> dict:
    """Fetch Fear & Greed Index from alternative.me."""
    url = "https://api.alternative.me/fng/?limit=1"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data.get("data", [{}])[0]
    except Exception as e:
        logger.error(f"Fear & Greed fetch error: {e}")
        return {}


def _fng_arabic(classification: str) -> str:
    """Translate F&G classification to Arabic."""
    mapping = {
        "Extreme Fear": "خوف شديد",
        "Fear": "خوف",
        "Neutral": "محايد",
        "Greed": "جشع",
        "Extreme Greed": "جشع شديد",
    }
    return mapping.get(classification, classification)


async def build_morning_report(bot: Bot) -> None:
    """Compose and send the daily morning report."""
    logger.info("Building morning report…")
    async with aiohttp.ClientSession() as session:
        prices, global_data, fng = await asyncio.gather(
            fetch_coingecko_prices(session),
            fetch_global_market(session),
            fetch_fear_and_greed(session),
        )

    # ── Prices ──────────────────────────────────
    btc = prices.get("bitcoin", {})
    eth = prices.get("ethereum", {})
    sol = prices.get("solana", {})

    btc_price = btc.get("usd", 0)
    btc_change = btc.get("usd_24h_change", 0.0)
    eth_price = eth.get("usd", 0)
    eth_change = eth.get("usd_24h_change", 0.0)
    sol_price = sol.get("usd", 0)
    sol_change = sol.get("usd_24h_change", 0.0)

    # ── Global ───────────────────────────────────
    total_mc = global_data.get("total_market_cap", {}).get("usd", 0)
    total_vol = global_data.get("total_volume", {}).get("usd", 0)

    # ── F&G ─────────────────────────────────────
    fng_value = fng.get("value", "N/A")
    fng_class = fng.get("value_classification", "N/A")
    fng_arabic = _fng_arabic(fng_class)

    # Dynamic emoji based on F&G classification
    if "Greed" in fng_class:
        fng_emoji = "🔥"
    elif "Fear" in fng_class:
        fng_emoji = "⚠️"
    else:
        fng_emoji = "🎯"

    # Dynamic indicator emoji for section header
    fng_indicator = fng_emoji

    # ── Economic calendar placeholder ────────────
    # Free, reliable economic calendar APIs with Arabic content are rare.
    # We embed a placeholder with a note; operators can integrate a paid feed.
    economic_summary = (
        "⚠️ تحقق من الموقع الرسمي لـ Investing.com للاطلاع على المفكرة الاقتصادية اليوم"
    )

    report = f"""🌅 <b>التقرير الصباحي لحالة السوق والسيولة</b>
{datetime.now(timezone.utc).strftime("%A, %d %B %Y — %H:%M UTC")}

<b>أسعار العملات القيادية (خلال 24 ساعة):</b>
👑 البيتكوين (BTC): <code>${btc_price:,.2f}</code> {_change_emoji(btc_change)} {btc_change:+.2f}%
💎 الإيثيريوم (ETH): <code>${eth_price:,.2f}</code> {_change_emoji(eth_change)} {eth_change:+.2f}%
⚡ السولانا (SOL): <code>${sol_price:,.2f}</code> {_change_emoji(sol_change)} {sol_change:+.2f}%

<b>إحصائيات السيولة الإجمالية:</b>
القيمة السوقية الكلية: <code>${_fmt_number(total_mc)}</code>
حجم التداول اليومي: <code>${_fmt_number(total_vol)}</code>

<b>مؤشر الخوف والجشع (Fear &amp; Greed Index):</b>
القيمة الحالية: <code>{fng_value}</code> — {fng_arabic} {fng_emoji}

<b>مفكرة الأحداث الاقتصادية المرتقبة اليوم:</b>
{economic_summary}

#تقرير_صباحي #بيتكوين #كريبتو"""

    await _safe_send(bot, report)
    logger.info("Morning report sent.")


# ─────────────────────────────────────────────
# AI CONTEXT SHAPING (Anthropic API)
# ─────────────────────────────────────────────
def _strip_html(text: str) -> str:
    """Remove HTML tags from RSS summary fields."""
    return re.sub(r"<[^>]+>", "", text or "").strip()


async def shape_news_with_ai(
    session: aiohttp.ClientSession, title: str, summary: str
) -> Optional[dict]:
    """
    Send the raw (usually English) headline + summary to DeepSeek and get back
    a structured JSON object with:
      - arabic_title: short Arabic translation of the headline
      - arabic_summary: 1-2 sentence Arabic summary (very concise)
      - expert_impact: 1-2 sentence Arabic expert-style market impact note

    Returns None on any failure so the caller can fall back to the raw text.
    """
    if not DEEPSEEK_API_KEY:
        logger.warning("DEEPSEEK_API_KEY not set — skipping AI shaping.")
        return None

    clean_summary = _strip_html(summary)[:600]

    system_prompt = (
        "أنت محلل أسواق مالية ومحرر أخبار كريبتو محترف. "
        "ستستلم عنوان خبر وملخصاً بالإنجليزية، ومهمتك إخراج JSON فقط بدون أي شرح "
        "أو علامات Markdown أو ```json، يحتوي على ثلاثة حقول بالعربية فقط:\n"
        '{"arabic_title": "ترجمة دقيقة ومختصرة للعنوان بالعربية الفصحى",'
        ' "arabic_summary": "ملخص جداً مختصر من جملة إلى جملتين كحد أقصى",'
        ' "expert_impact": "تعليق تأثير سوقي مختصر جداً (جملة واحدة إلى جملتين)'
        ' بعقلية خبير ومحلل اقتصادي/كريبتو محترف، يربط الخبر بسلوك السيولة أو'
        ' البيتكوين أو الأسواق عند الصلة"}\n'
        "لا تستخدم أي نص خارج JSON. لا تكرر الإنجليزية. كن دقيقاً وموضوعياً."
    )

    user_content = f"العنوان: {title}\nالملخص: {clean_summary}"

    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.7,
        "max_tokens": 400,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    }

    try:
        async with session.post(
            DEEPSEEK_API_URL,
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"DeepSeek API HTTP {resp.status}: {body[:200]}")
                return None
            data = await resp.json()

        # Extract text from DeepSeek response
        if "choices" not in data or not data["choices"]:
            logger.warning(f"DeepSeek API returned no choices: {data}")
            return None

        message_content = data["choices"][0].get("message", {}).get("content", "").strip()
        if not message_content:
            logger.warning("DeepSeek API returned empty content")
            return None

        # Defensive cleanup in case the model wraps output in code fences
        raw_text = re.sub(r"^```json|```$", "", message_content).strip()

        parsed = json.loads(raw_text)
        if not all(k in parsed for k in ("arabic_title", "arabic_summary", "expert_impact")):
            logger.warning(f"AI shaping returned incomplete JSON: {parsed}")
            return None
        return parsed

    except (json.JSONDecodeError, asyncio.TimeoutError) as e:
        logger.warning(f"AI shaping parse/timeout error: {e}")
        return None
    except Exception as e:
        logger.error(f"AI shaping unexpected error: {e}")
        return None


# ─────────────────────────────────────────────
# MODULE 2: BREAKING NEWS AGGREGATOR
# ─────────────────────────────────────────────
def _make_news_id(entry) -> str:
    """Stable unique ID for a feed entry."""
    raw = (entry.get("id") or entry.get("link") or entry.get("title") or "")
    return hashlib.md5(raw.encode()).hexdigest()


async def fetch_rss_entries(session: aiohttp.ClientSession, feed_url: str) -> list[dict]:
    """Download and parse a single RSS feed asynchronously."""
    try:
        async with session.get(
            feed_url, timeout=aiohttp.ClientTimeout(total=20)
        ) as resp:
            raw = await resp.text()
        parsed = feedparser.parse(raw)
        return parsed.entries or []
    except Exception as e:
        logger.warning(f"RSS fetch failed [{feed_url}]: {e}")
        return []


async def poll_breaking_news(bot: Bot) -> None:
    """Poll all RSS feeds every hour and collect new articles for hourly digest."""
    logger.info("Polling news feeds…")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rss_entries(session, url) for url in RSS_FEEDS]
        results = await asyncio.gather(*tasks)

        for entries in results:
            # Oldest-first so channel reads chronologically
            for entry in reversed(entries[:20]):
                news_id = _make_news_id(entry)
                if news_id in processed_news_ids:
                    continue

                processed_news_ids.add(news_id)
                title: str = entry.get("title", "").strip()
                link: str = entry.get("link", "").strip()
                summary: str = entry.get("summary", "").strip()

                if not title or not link:
                    continue

                shaped = await shape_news_with_ai(session, title, summary)

                if shaped:
                    arabic_title = shaped["arabic_title"].strip()
                    expert_impact = shaped["expert_impact"].strip()
                else:
                    # Fallback
                    arabic_title = title
                    expert_impact = "متابعة السوق ضرورية."

                # Add to hourly buffer instead of sending immediately
                hourly_news_buffer.append({
                    "title": arabic_title,
                    "impact": expert_impact,
                    "link": link,
                })

    logger.info(f"News poll complete. {len(hourly_news_buffer)} items in buffer.")

    # Cap memory usage — keep only the most recent 5000 IDs
    if len(processed_news_ids) > 5000:
        trimmed = list(processed_news_ids)[-2500:]
        processed_news_ids.clear()
        processed_news_ids.update(trimmed)


async def send_hourly_news_digest(bot: Bot) -> None:
    """Send hourly news digest with all collected articles."""
    global hourly_news_buffer

    if not hourly_news_buffer:
        logger.info("No new articles for hourly digest.")
        return

    logger.info(f"Sending hourly news digest with {len(hourly_news_buffer)} articles…")

    # Build message with clean format
    message_parts = [
        "🚨 <b>تقرير الأخبار العاجلة — آخر ساعة</b>\n",
    ]

    for idx, article in enumerate(hourly_news_buffer, 1):
        message_parts.append(f"\n📌 <b>{article['title']}</b>")
        message_parts.append(f"\n{article['impact']}")
        message_parts.append(f"\n🔗 <a href='{article['link']}'>اقرأ المزيد</a>")

        # Add separator between articles (except last one)
        if idx < len(hourly_news_buffer):
            message_parts.append("\n" + "─" * 40)

    message_parts.append("\n\n#أخبار_عاجلة #كريبتو #تحليل_سوق")

    message = "".join(message_parts)

    # Send and clear buffer
    sent = await _safe_send(bot, message)
    if sent:
        logger.info(f"Hourly digest sent with {len(hourly_news_buffer)} articles.")
        hourly_news_buffer.clear()
    else:
        logger.warning("Failed to send hourly digest — keeping buffer for next cycle.")


# ─────────────────────────────────────────────
# MODULE 3: WHALE MOVEMENTS WATCHER
# ─────────────────────────────────────────────
async def fetch_whale_transactions(session: aiohttp.ClientSession) -> list[dict]:
    """
    Pull recent large transactions from Whale Alert free tier.
    Falls back to a public blockchain monitor endpoint if no API key is set.
    """
    global last_whale_cursor

    now = int(time.time())
    transactions = []

    if WHALE_ALERT_KEY:
        # Official Whale Alert API (free tier: 10 req/min, last 3600s)
        params = {
            "api_key": WHALE_ALERT_KEY,
            "min_value": WHALE_MIN_VALUE_USD,
            "start": last_whale_cursor,
            "limit": 100,
        }
        try:
            async with session.get(
                WHALE_ALERT_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    transactions = data.get("transactions", [])
                else:
                    logger.warning(f"Whale Alert HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Whale Alert fetch error: {e}")
    else:
        # ── Fallback: CryptoQuant public large-tx feed (no auth required) ──
        # We query the CoinGecko on-chain large movers as a proxy.
        # For production, register at whale-alert.io for a free key.
        logger.info("No WHALE_ALERT_API_KEY set — skipping whale watch this cycle.")

    last_whale_cursor = now
    return transactions


def _direction_arabic(tx: dict) -> tuple[str, str]:
    """
    Infer transfer direction and return (alert_header, indicator_text).

    Direction matrix:
      to exchange      -> 🚨 [تدفق سلبي - دخول منصة] 📉   | ضغط بيعي محتمل 🔴
      from exchange    -> 🐳 [تجميع خارجي - سحب سيولة] 📈  | ضغط شرائي 🟢
      wallet-to-wallet -> 📦 [تدوير داخلي صامت] 🔄          | راقب التحركات ⚠️
    """
    from_owner = (tx.get("from", {}) or {}).get("owner_type", "unknown")
    to_owner = (tx.get("to", {}) or {}).get("owner_type", "unknown")

    if to_owner == "exchange":
        header = "🚨 [تدفق سلبي - دخول منصة] 📉"
        indicator = "تنبيه: ضغط بيعي محتمل 🔴"
    elif from_owner == "exchange":
        header = "🐳 [تجميع خارجي - سحب سيولة] 📈"
        indicator = "تنبيه: ضغط شرائي / تدوير خارج التداول 🟢"
    else:
        header = "📦 [تدوير داخلي صامت] 🔄"
        indicator = "تحرك بين محافظ خاصة — راقب التحركات ⚠️"

    return header, indicator


async def poll_whale_movements(bot: Bot) -> None:
    """Check for large on-chain transactions and alert the channel."""
    logger.info("Polling whale movements…")
    async with aiohttp.ClientSession() as session:
        txs = await fetch_whale_transactions(session)

    for tx in txs:
        symbol: str = (tx.get("symbol") or "???").upper()
        amount: float = tx.get("amount", 0)
        amount_usd: float = tx.get("amount_usd", 0)
        to_info: dict = tx.get("to", {}) or {}
        from_info: dict = tx.get("from", {}) or {}
        exchange: str = (
            to_info.get("owner")
            or from_info.get("owner")
            or "محفظة خاصة"
        )
        alert_header, indicator = _direction_arabic(tx)

        message = (
            f"{alert_header}\n\n"
            f"<b>العملة:</b> {symbol}\n"
            f"<b>الكمية:</b> {_fmt_number(amount, 2)} {symbol} "
            f"(≈ <code>${_fmt_number(amount_usd)}</code>)\n"
            f"<b>الجهة:</b> {exchange}\n\n"
            f"{indicator}\n\n"
            f"#حيتان #whale_alert"
        )
        await _safe_send(bot, message)
        await asyncio.sleep(3)

    logger.info(f"Whale poll complete. {len(txs)} transactions processed.")


# ─────────────────────────────────────────────
# SCHEDULER SETUP
# ─────────────────────────────────────────────
def setup_scheduler(bot: Bot) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Module 1: Daily morning report at 08:00 UTC
    scheduler.add_job(
        build_morning_report,
        trigger="cron",
        hour=8,
        minute=0,
        id="morning_report",
        kwargs={"bot": bot},
        max_instances=1,
        coalesce=True,
    )

    # Module 2: News poll every hour (collect articles)
    scheduler.add_job(
        poll_breaking_news,
        trigger="cron",
        minute=0,  # Top of every hour
        id="news_poll",
        kwargs={"bot": bot},
        max_instances=1,
        coalesce=True,
    )

    # Module 2b: Send hourly digest 1 minute after poll
    scheduler.add_job(
        send_hourly_news_digest,
        trigger="cron",
        minute=1,  # 1 minute after each hour
        id="news_digest",
        kwargs={"bot": bot},
        max_instances=1,
        coalesce=True,
    )

    # Module 3: Whale watcher every 4 hours
    scheduler.add_job(
        poll_whale_movements,
        trigger="interval",
        hours=4,
        id="whale_watch",
        kwargs={"bot": bot},
        max_instances=1,
        coalesce=True,
    )

    return scheduler


# ─────────────────────────────────────────────
# STARTUP SELF-TEST
# ─────────────────────────────────────────────
async def run_startup_tests(bot: Bot) -> None:
    """Send a startup ping to the channel to confirm connectivity."""
    logger.info("Running startup self-test…")
    try:
        me = await bot.get_me()
        logger.info(f"Authenticated as: @{me.username} (id={me.id})")
    except TelegramError as e:
        logger.critical(f"Bot authentication failed: {e}")
        raise SystemExit(1)

    startup_msg = (
        "🤖 <b>البوت يعمل الآن</b>\n\n"
        "✅ جميع الوحدات نشطة:\n"
        "• 🌅 التقرير الصباحي — يومياً الساعة 08:00 UTC\n"
        "• 📰 أخبار عاجلة — كل 5 دقائق\n"
        "• 🐋 مراقبة الحيتان — كل 10 دقائق\n\n"
        f"🕐 وقت البدء: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    await _safe_send(bot, startup_msg)
    logger.info("Startup ping sent.")


# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────
async def main() -> None:
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.critical(
            "PUBLIC_BOT_TOKEN is not set. "
            "Export it as an environment variable before running."
        )
        raise SystemExit(1)

    bot = Bot(token=BOT_TOKEN)

    await run_startup_tests(bot)

    scheduler = setup_scheduler(bot)
    scheduler.start()
    logger.info("Scheduler started. Bot is running — press Ctrl+C to stop.")

    # Run an immediate first-pass of news and whale polls on startup
    await poll_breaking_news(bot)
    await send_hourly_news_digest(bot)
    await poll_whale_movements(bot)

    # Keep the event loop alive indefinitely
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    finally:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped. Goodbye.")


if __name__ == "__main__":
    asyncio.run(main())
