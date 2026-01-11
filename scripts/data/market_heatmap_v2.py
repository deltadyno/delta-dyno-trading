#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Market Events Heatmap Loader

This script calls OpenAI's GPT API to fetch upcoming market-moving events and
persists them into the database. It automatically retries with a stricter prompt
if the initial parse finds 0 valid events.

Features:
- Calls GPT-5 via Responses API
- Parses multiple date formats
- Classifies events by category
- Inserts into dd_macro_event_heatmap table
- Prunes old records

Requirements:
    pip install openai

Environment:
    Set OPENAI_API_KEY environment variable or use AWS SSM parameter 'openai_key'
"""

import os
import re
from datetime import datetime, date

from openai import OpenAI

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader
from deltadyno.utils.helpers import get_ssm_parameter
from deltadyno.utils.logger import setup_logger


# =============================================================================
# Configuration
# =============================================================================

# Determine base directory (AWS EC2 vs local)
if os.path.exists('/home/ec2-user/deltadynocode'):
    BASE_DIR = "/home/ec2-user/deltadynocode/"
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logs_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(logs_dir, exist_ok=True)

# Load configuration
if os.path.exists(os.path.join(BASE_DIR, "config.ini")):
    config_path = os.path.join(BASE_DIR, "config.ini")
else:
    config_path = 'config/config.ini'

file_config = ConfigLoader(config_file=config_path)

# Initialize database configuration loader
db_config_loader = DatabaseConfigLoader(
    profile_id=None,
    db_host=file_config.db_host,
    db_user=file_config.db_user,
    db_password=file_config.db_password,
    db_name=file_config.db_name,
    tables=["user_profile"],
    refresh_interval=0  # Not needed for this script
)

# Setup logger
logger = setup_logger(
    db_config_loader,
    log_to_file=True,
    file_name=os.path.join(logs_dir, "marketHeatmap.log")
)
logger.info("Logger initialized.")


# =============================================================================
# OpenAI Configuration
# =============================================================================

logger.info("Fetching OpenAI API key...")
try:
    api_key = get_ssm_parameter("openai_key")
except Exception as e:
    # Fall back to environment variable
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            f"OPENAI_API_KEY not set. Export it or configure AWS SSM parameter 'openai_key'. Error: {e}"
        )

logger.info("API key ready.")

# Initialize OpenAI client
client = OpenAI(api_key=api_key)

today_str = datetime.now().strftime("%Y-%m-%d")


# =============================================================================
# Prompts
# =============================================================================

# Primary prompt
PROMPT = f"""
Act as a professional macro trader managing a multi-asset portfolio.

Today is {today_str}. Provide a chronological, compact, and strictly formatted list of upcoming market-moving events, starting from today and covering the next few weeks.

Only include:
- U.S. macro data (e.g., CPI, Jobs, PPI, Retail Sales)
- Fed events (e.g., FOMC meetings, Powell speeches, Fed minutes)
- Major corporate earnings (only Tech, Financials, Energy)
- Geopolitical catalysts (elections, wars, peace deals)
- U.S. Presidential deadlines or executive actions (tariffs, regulations, policy decisions)

⚠️ Do not include summaries, explanations, apologies, limitations, questions, or any extra text.
⚠️ If exact dates are uncertain, provide your best estimate and append " (Est.)".
✅ Format each line exactly as:
[YYYY-MM-DD] — Short Event Description

Output ONLY the list (one event per line). No headings, no extra lines.
"""

# Strict fallback prompt (if first parse fails)
FALLBACK_PROMPT = f"""
You are an events formatter. Today is {today_str}.
Return AT LEAST 12 upcoming U.S. market-moving events covering the next 4 weeks.
Allowed categories:
- U.S. macro (CPI, PPI, Nonfarm Payrolls, Retail Sales, PCE, GDP, PMI)
- Fed (FOMC meeting/Minutes, Powell/Regional speeches)
- Major earnings (Tech/Financials/Energy) — include at least 3
- Geopolitical catalysts (elections, ceasefires, escalations)
- U.S. Presidential deadlines/executive actions

HARD REQUIREMENTS:
- EXACT format per line: [YYYY-MM-DD] — Short Event Description
- Strictly chronological (earliest first)
- No preamble, no caveats, no questions, no extra commentary
- If a date is uncertain, use your best estimate and add " (Est.)"
- Output ONLY the lines; nothing else.
"""


# =============================================================================
# OpenAI API Functions
# =============================================================================

def call_gpt5(client: OpenAI, text_prompt: str) -> str:
    """
    Call GPT-5 Responses API and return plain text.

    Args:
        client: OpenAI client instance
        text_prompt: Prompt text

    Returns:
        Response text or empty string
    """
    try:
        resp = client.responses.create(
            model="gpt-5",
            input=text_prompt
        )
        return (resp.output_text or "").strip()
    except Exception as e:
        logger.error(f"Error calling GPT-5 API: {e}")
        raise


# =============================================================================
# Parsing Functions
# =============================================================================

def is_valid_event_line(line: str) -> bool:
    """
    Check if a line matches any allowed date-description format.

    Args:
        line: Line to validate

    Returns:
        True if line is valid, False otherwise
    """
    return (
        re.match(r"\[\d{4}-\d{2}-\d{2}\]\s+[—–-]\s+.+", line) or
        re.match(r"\d{4}-\d{2}-\d{2}\s+[—–-]\s+.+", line) or
        re.match(r"\[[A-Za-z]+ \d{1,2}, \d{4}\]\s+[—–-]\s+.+", line)
    ) is not None


def parse_event_line(line: str) -> tuple:
    """
    Parse a single event line into (event_date: date, description: str) or (None, None).

    Args:
        line: Event line to parse

    Returns:
        Tuple of (date, description) or (None, None) if parsing fails
    """
    line = line.strip()
    if not line:
        return None, None

    # 1) [Month DD, YYYY] — Description
    m = re.match(r"\[([A-Za-z]+ \d{1,2}, \d{4})\]\s+[—–-]\s+(.+)", line)
    if m:
        date_str, description = m.groups()
        try:
            return datetime.strptime(date_str, "%B %d, %Y").date(), description
        except Exception:
            return None, None

    # 2) [YYYY-MM-DD] — Description
    m = re.match(r"\[(\d{4}-\d{2}-\d{2})\]\s+[—–-]\s+(.+)", line)
    if m:
        date_str, description = m.groups()
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date(), description
        except Exception:
            return None, None

    # 3) YYYY-MM-DD — Description
    m = re.match(r"(\d{4}-\d{2}-\d{2})\s+[—–-]\s+(.+)", line)
    if m:
        date_str, description = m.groups()
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date(), description
        except Exception:
            return None, None

    return None, None


def classify(description: str) -> str:
    """
    Classify event description into a category.

    Args:
        description: Event description

    Returns:
        Category string
    """
    desc_lower = description.lower()

    if 'earnings' in desc_lower:
        return 'earnings'
    if any(x in desc_lower for x in ['cpi', 'ppi', 'payroll', 'nonfarm', 'gdp', 'pmi', 'pce', 'jobs', 'retail sales']):
        return 'macro_data'
    if 'fomc' in desc_lower or 'interest rate' in desc_lower or 'fed' in desc_lower or 'powell' in desc_lower or 'minutes' in desc_lower:
        return 'fed_event'
    if 'presidential' in desc_lower or 'tariff' in desc_lower or 'executive' in desc_lower or 'white house' in desc_lower:
        return 'us_policy'
    if 'witching' in desc_lower:
        return 'market_event'
    if 'geopolitical' in desc_lower or 'summit' in desc_lower or 'election' in desc_lower or 'ceasefire' in desc_lower or 'escalation' in desc_lower:
        return 'geopolitics'

    return 'unknown'


# =============================================================================
# Database Functions
# =============================================================================

def insert_event(event_date: str, description: str, category: str, source: str = 'openai') -> None:
    """
    Insert an event into the database.

    Args:
        event_date: Event date as string (YYYY-MM-DD)
        description: Event description
        category: Event category
        source: Event source (default: 'openai')
    """
    # Check if db_config_loader has insert_event method, otherwise use raw SQL
    if hasattr(db_config_loader, 'insert_event'):
        db_config_loader.insert_event(event_date, description, category, source)
    else:
        # Fallback to raw SQL if method doesn't exist
        insert_query = """
            INSERT INTO dd_macro_event_heatmap (event_date, description, category, source, generated_at)
            VALUES (%s, %s, %s, %s, CURDATE())
            ON DUPLICATE KEY UPDATE
                description = VALUES(description),
                category = VALUES(category),
                generated_at = VALUES(generated_at)
        """
        db_config_loader.update_config_in_db(insert_query, (event_date, description, category, source))


def execute_query(query: str, params: tuple = None) -> None:
    """
    Execute a raw SQL query.

    Args:
        query: SQL query string
        params: Query parameters (optional)
    """
    db_config_loader.update_config_in_db(query, params)


# =============================================================================
# Main Function
# =============================================================================

def main():
    """Main entry point for the market heatmap script."""
    # Call GPT-5 (attempt 1)
    logger.info("Calling OpenAI (GPT-5) for market-moving events heatmap...")
    output_text = call_gpt5(client, PROMPT)

    logger.info("=== GPT Heatmap Output (attempt 1) ===")
    logger.info(output_text)

    lines = [ln.strip() for ln in output_text.splitlines() if ln.strip()]
    print(f"Parsing {len(lines)} event lines...")
    logger.info(f"Parsing {len(lines)} event lines...")

    valid_count = sum(1 for ln in lines if is_valid_event_line(ln))

    if valid_count == 0:
        # Retry with stricter fallback
        logger.info("No valid event lines parsed — retrying with strict fallback prompt.")
        output_text = call_gpt5(client, FALLBACK_PROMPT)
        logger.info("=== GPT Heatmap Output (attempt 2) ===")
        logger.info(output_text)
        lines = [ln.strip() for ln in output_text.splitlines() if ln.strip()]
        print(f"Parsing {len(lines)} event lines (retry)...")
        logger.info(f"Parsing {len(lines)} event lines (retry)...")

    inserted_count = 0

    for line in lines:
        event_date, description = parse_event_line(line)
        if not event_date or not description:
            print(f"⏭️ Line skipped: {line}")
            logger.info(f"⏭️ Line skipped: {line}")
            continue

        category = classify(description)

        # Insert into DB
        print(f"✅ Inserting: {event_date} | {category} | {description}")
        logger.info(f"Inserting: {event_date} | {category} | {description}")

        try:
            insert_event(
                event_date=str(event_date),
                description=description.strip(),
                category=category,
                source='openai'
            )
            inserted_count += 1
        except Exception as e:
            logger.error(f"Failed to insert event: {e}")
            print(f"❌ Failed to insert event: {e}")

    print(f"✅ {inserted_count} events inserted into the database.")
    logger.info(f"✅ {inserted_count} events inserted into the database.")

    # Prune old rows
    if inserted_count > 0:
        today_d = date.today()
        try:
            delete_query = """
                DELETE FROM dd_macro_event_heatmap
                WHERE generated_at < %s
                  AND source = 'openai'
            """
            execute_query(delete_query, (today_d,))
            print(f"🗑️ Deleted old events prior to {today_d}")
            logger.info(f"🗑️ Deleted old events prior to {today_d}")
        except Exception as e:
            logger.error(f"❌ Failed to delete old records — {e}")
            print(f"❌ Failed to delete old records — {e}")


if __name__ == "__main__":
    main()

