"""
Configuration File to Database Importer

This module processes INI configuration files and imports their settings into
the MySQL database. It handles multiple configuration tables including:
- dd_common_config
- dd_open_position_config
- dd_close_position_config
- dd_bar_order_range
- dd_choppy_bar_order_range

Usage:
    from scripts.admin.configToDB import process_ini_data
    process_ini_data(filename, profile_id, cursor)
"""

import os
import re
import sys
from collections import defaultdict

from deltadyno.config.loader import ConfigLoader


# =============================================================================
# Utility Functions
# =============================================================================

def get_profile_id(filename: str) -> int:
    """
    Extract profile ID from filename.

    Args:
        filename: Configuration file name (e.g., "config_123.ini")

    Returns:
        Profile ID as integer, or 0 if not found
    """
    match = re.search(r'config(?:_(\d+))?\.ini$', filename)
    return int(match.group(1)) if match and match.group(1) else 0


def clean_csv_string(csv_str: str) -> str:
    """
    Remove spaces from comma-separated values.

    Args:
        csv_str: Comma-separated string

    Returns:
        Cleaned comma-separated string
    """
    return ",".join([x.strip() for x in csv_str.split(",")])


# =============================================================================
# Database Functions
# =============================================================================

def insert_bar_order_data(
    cursor,
    profile_id: int,
    table_name: str,
    bar_strength: str,
    candle_sizes: list,
    limit_cutoffs: list,
    limit_qtys: list,
    market_qtys: list,
    order_amts: list,
    ignore_buys: list,
    buys_for_amount: list
) -> None:
    """
    Insert or update bar order data in MySQL.

    Args:
        cursor: Database cursor
        profile_id: Profile ID
        table_name: Table name (dd_bar_order_range or dd_choppy_bar_order_range)
        bar_strength: Bar strength range (e.g., "0.5-0.7")
        candle_sizes: List of candle sizes
        limit_cutoffs: List of limit order cutoff prices
        limit_qtys: List of limit order quantities
        market_qtys: List of market order quantities
        order_amts: List of max order amounts
        ignore_buys: List of buy-if-price-lt values
        buys_for_amount: List of buy-for-amount values
    """
    min_strength, max_strength = map(float, bar_strength.split('-'))

    # Clean spaces from CSV values
    candle_sizes_str = clean_csv_string(",".join(candle_sizes))
    limit_cutoffs_str = clean_csv_string(",".join(limit_cutoffs))
    limit_qtys_str = clean_csv_string(",".join(limit_qtys))
    market_qtys_str = clean_csv_string(",".join(market_qtys))
    order_amts_str = clean_csv_string(",".join(order_amts))
    ignore_buys_str = clean_csv_string(",".join(ignore_buys))
    buys_for_amount_str = clean_csv_string(",".join(buys_for_amount))

    query = f"""
        INSERT INTO {table_name} (profile_id, min_bar_strength, max_bar_strength, 
                                  candle_size_range, limit_order_cutoff_price, 
                                  limit_order_qty_to_buy, market_order_qty_to_buy, max_order_amount, buy_if_price_lt, buy_for_amount) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            candle_size_range = VALUES(candle_size_range),
            limit_order_cutoff_price = VALUES(limit_order_cutoff_price),
            limit_order_qty_to_buy = VALUES(limit_order_qty_to_buy),
            market_order_qty_to_buy = VALUES(market_order_qty_to_buy),
            max_order_amount = VALUES(max_order_amount),
            buy_if_price_lt = VALUES(buy_if_price_lt),
            buy_for_amount = VALUES(buy_for_amount)            
    """

    cursor.execute(
        query,
        (
            profile_id, min_strength, max_strength,
            candle_sizes_str, limit_cutoffs_str,
            limit_qtys_str, market_qtys_str, order_amts_str,
            ignore_buys_str, buys_for_amount_str
        )
    )


# =============================================================================
# INI Parsing Functions
# =============================================================================

def parse_ini_file(filename: str) -> dict:
    """
    Manually parse INI file and return nested dictionary structure.

    Args:
        filename: Path to INI file

    Returns:
        Nested dictionary: {section: {key: [values]}}
    """
    data = defaultdict(lambda: defaultdict(list))
    current_section = None

    with open(filename, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith(";") or line.startswith("#"):
                continue  # Skip empty and comment lines

            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1].strip()
            elif "=" in line and current_section:
                key, value = map(str.strip, line.split("=", 1))
                data[current_section][key].append(value)  # Store duplicate keys as lists

    return data


def process_ini_data(filename: str, profile_id: int, cursor) -> None:
    """
    Process parsed INI data and insert into the database.

    This function:
    1. Parses the INI file
    2. Maps sections to database tables
    3. Inserts/updates configuration values
    4. Handles bar order range data specially

    Args:
        filename: Path to INI configuration file
        profile_id: Profile ID to associate configurations with
        cursor: Database cursor for executing queries
    """
    parsed_data = parse_ini_file(filename)

    section_mapping = {
        "Common": "dd_common_config",
        "logging": "dd_common_config",
        "MainOpenPositions": "dd_open_position_config",
        "ClosePositions": "dd_close_position_config"
    }

    bar_data = defaultdict(list)

    # Delete old entries before inserting new ones
    delete_query = "DELETE FROM dd_bar_order_range WHERE profile_id = %s"
    cursor.execute(delete_query, (profile_id,))

    delete_query = "DELETE FROM dd_choppy_bar_order_range WHERE profile_id = %s"
    cursor.execute(delete_query, (profile_id,))

    for section, key_values in parsed_data.items():
        table_name = section_mapping.get(section, None)

        if table_name:
            for key, values in key_values.items():
                if key in ["regular_bar_strength", "choppy_bar_strength"]:
                    table = "dd_bar_order_range" if key.startswith("regular") else "dd_choppy_bar_order_range"
                    for value in values:
                        bar_data[table].append({"bar_strength": value})

                elif key in [
                    "regular_candle_size_range", "choppy_candle_size_range",
                    "regular_limit_order_cutoff_price", "choppy_limit_order_cutoff_price",
                    "regular_limit_order_qty_to_buy", "choppy_limit_order_qty_to_buy",
                    "regular_market_order_qty_to_buy", "choppy_market_order_qty_to_buy",
                    "regular_max_order_amount", "choppy_max_order_amount",
                    "regular_buy_if_price_lt", "choppy_buy_if_price_lt",
                    "regular_buy_for_amount", "choppy_buy_for_amount"
                ]:
                    table = "dd_bar_order_range" if key.startswith("regular") else "dd_choppy_bar_order_range"
                    property_name = key.split('_', 1)[1]  # Extract field name after "regular_" or "choppy_"

                    # Ensure corresponding bar_strength index exists
                    for i, value in enumerate(values):
                        if i < len(bar_data[table]):
                            bar_data[table][i][property_name] = value
                        else:
                            bar_data[table].append({property_name: value})
                else:
                    query = f"""
                        INSERT INTO {table_name} (profile_id, config_key, value) 
                        VALUES (%s, %s, %s) 
                        ON DUPLICATE KEY UPDATE value = VALUES(value)
                    """

                    cursor.execute(query, (profile_id, key, values[0]))

    # Insert bar order data (ensure correct alignment per bar strength)
    for table_name, data_list in bar_data.items():
        for data in data_list:
            if "bar_strength" in data and all(
                k in data for k in [
                    "candle_size_range", "limit_order_cutoff_price",
                    "limit_order_qty_to_buy", "market_order_qty_to_buy",
                    "max_order_amount", "buy_if_price_lt", "buy_for_amount"
                ]
            ):
                insert_bar_order_data(
                    cursor, profile_id, table_name, data["bar_strength"],
                    [data["candle_size_range"]], [data["limit_order_cutoff_price"]],
                    [data["limit_order_qty_to_buy"]], [data["market_order_qty_to_buy"]],
                    [data["max_order_amount"]], [data["buy_if_price_lt"]], [data["buy_for_amount"]]
                )


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main function to process INI files from command line."""
    import mysql.connector

    # Load configuration
    config_path = 'config/config.ini'
    if not os.path.exists(config_path):
        config_path = '/home/ec2-user/deltadynocode/config.ini'

    config = ConfigLoader(config_file=config_path)

    conn = mysql.connector.connect(
        host=config.db_host,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cursor = conn.cursor()

    if len(sys.argv) > 1:
        filename = sys.argv[1]
        if not filename.endswith(".ini"):
            print("Error: Please provide a valid .ini file.")
            return
        if not os.path.exists(filename):
            print(f"Error: File '{filename}' does not exist.")
            return
        ini_files = [filename]
    else:
        ini_files = [f for f in os.listdir() if f.endswith(".ini") and f != "config_client_base.ini"]

    print(f"Processing INI files: {ini_files}")

    if ini_files:
        profile_id = get_profile_id(ini_files[0])

        for ini_file in ini_files:
            process_ini_data(ini_file, profile_id, cursor)

        conn.commit()

    cursor.close()
    conn.close()

    print("\nDatabase update complete.")


if __name__ == "__main__":
    main()

