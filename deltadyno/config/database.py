"""
Database configuration loader for the DeltaDyno trading system.

This module provides a thread-safe configuration loader that reads
settings from a MySQL database and periodically refreshes them.

Features:
- Thread-safe configuration access
- Auto-refresh of configuration values
- Support for multiple configuration tables
- Type conversion with default values
"""

import threading
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import mysql.connector

from deltadyno.config.defaults import CONFIG_DEFAULTS


class DatabaseConfigLoader:
    """
    Load and manage configuration from a MySQL database.

    This class provides:
    - Connection management with auto-reconnect
    - Thread-safe configuration access
    - Periodic auto-refresh of configuration values
    - Type-safe value retrieval with defaults
    """

    def __init__(
        self,
        profile_id: int,
        db_host: str,
        db_user: str,
        db_password: str,
        db_name: str,
        tables: Optional[List[str]] = None,
        refresh_interval: int = 30
    ):
        """
        Initialize the database configuration loader.

        Args:
            profile_id: Profile ID to load configuration for
            db_host: MySQL database host
            db_user: Database username
            db_password: Database password
            db_name: Database name
            tables: List of tables to load configuration from
            refresh_interval: Seconds between configuration refreshes
        """
        self.profile_id = profile_id
        self.host = db_host
        self.user = db_user
        self.password = db_password
        self.database = db_name
        self.tables = tables if tables else ["dd_common_config"]
        self.refresh_interval = refresh_interval
        self.config_data: Dict[str, Any] = {}
        self.lock = threading.Lock()

        # Initialize database connection
        self.db_connection = self._create_connection()

        # Load initial configuration
        if profile_id is not None:
            self._load_config_from_db()

        # Start auto-refresh (unless user_profile table is being loaded)
        if "user_profile" not in self.tables:
            self._start_auto_refresh()
        else:
            print("User Profile table found in list. Skipping auto-refresh.")

    # =========================================================================
    # Connection Management
    # =========================================================================

    def _create_connection(self) -> mysql.connector.MySQLConnection:
        """Establish a connection to the MySQL database."""
        return mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def get_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        """
        Get the current database connection, reconnecting if necessary.

        Returns:
            Active database connection, or None on error
        """
        with self.lock:
            try:
                if self.db_connection is None or not self.db_connection.is_connected():
                    print("Re-establishing lost DB connection...")
                    # Close old connection if it exists
                    if self.db_connection is not None:
                        try:
                            self.db_connection.close()
                        except Exception:
                            pass
                    self.db_connection = self._create_connection()
                return self.db_connection
            except Exception as e:
                print(f"Error while getting DB connection: {e}")
                return None

    def close_connection(self) -> None:
        """Close the database connection if open."""
        with self.lock:
            if self.db_connection is not None:
                try:
                    if self.db_connection.is_connected():
                        self.db_connection.close()
                except Exception:
                    pass  # Ignore errors when closing
                finally:
                    self.db_connection = None

    def _ensure_connection(self) -> None:
        """Ensure database connection is active."""
        with self.lock:
            try:
                if self.db_connection is None or not self.db_connection.is_connected():
                    # Close old connection if it exists
                    if self.db_connection is not None:
                        try:
                            self.db_connection.close()
                        except Exception:
                            pass  # Ignore errors when closing old connection
                    # Create new connection
                    self.db_connection = self._create_connection()
            except Exception as e:
                print(f"Error ensuring connection: {e}")
                # Try to create a fresh connection
                try:
                    if self.db_connection is not None:
                        self.db_connection.close()
                except Exception:
                    pass
                self.db_connection = self._create_connection()

    # =========================================================================
    # Configuration Loading
    # =========================================================================

    def _load_config_from_db(self) -> None:
        """Fetch configuration settings from specified tables."""
        print("Fetching configuration data from database...")

        max_retries = 3
        cursor = None
        
        for attempt in range(max_retries):
            try:
                self._ensure_connection()
                
                # Get connection reference within lock
                with self.lock:
                    if self.db_connection is None or not self.db_connection.is_connected():
                        continue  # Skip this attempt if connection is bad
                    
                    # Create cursor and fetch data within the lock
                    cursor = self.db_connection.cursor(dictionary=True)
                    self.db_connection.commit()  # Ensure fresh data

                # Process data outside lock to avoid holding it too long
                new_config_data: Dict[str, Any] = {}

                for table in self.tables:
                    query = f"SELECT * FROM {table} WHERE profile_id = {self.profile_id}"
                    cursor.execute(query)
                    rows = cursor.fetchall()

                    # Handle special order range tables
                    if table in {"dd_bar_order_range", "dd_choppy_bar_order_range"}:
                        new_config_data[table] = self._parse_order_range_rows(rows)
                        continue

                    # Handle standard config tables
                    if rows:
                        for row in rows:
                            if "config_key" in row and "value" in row:
                                key = row["config_key"]
                                value = self._parse_value(key, row["value"])
                                new_config_data[key] = value
                            else:
                                print(f"Skipping table {table}, missing required columns.")

                # Close cursor
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = None

                # Update config data
                if new_config_data:
                    with self.lock:
                        self.config_data = new_config_data
                    break
                else:
                    print(f"Warning: No data fetched. Retrying {attempt + 1}/{max_retries}...")

            except Exception as e:
                print(f"Error fetching config data: {e}. Retrying {attempt + 1}/{max_retries}...")
                # Clean up cursor
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = None
                # Reset connection on error
                with self.lock:
                    try:
                        if self.db_connection is not None:
                            self.db_connection.close()
                    except Exception:
                        pass
                    self.db_connection = None

        # Final cleanup
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass

        if not self.config_data:
            print("Critical: Failed to fetch config data. Using last known values.")

    def _parse_order_range_rows(self, rows: List[Dict]) -> List[Dict]:
        """Parse order range configuration rows."""
        return [
            {
                "min_bar_strength": row["min_bar_strength"],
                "max_bar_strength": row["max_bar_strength"],
                "candle_size_range": [
                    [float(v.split("-")[0].strip()), float(v.split("-")[1].strip())]
                    for v in row["candle_size_range"].split(",")
                ],
                "limit_order_cutoff_price": [
                    float(v.strip()) for v in row["limit_order_cutoff_price"].split(",")
                ],
                "limit_order_qty_to_buy": [
                    float(v.strip()) for v in row["limit_order_qty_to_buy"].split(",")
                ],
                "market_order_qty_to_buy": [
                    float(v.strip()) for v in row["market_order_qty_to_buy"].split(",")
                ],
                "max_order_amount": [
                    float(v.strip()) for v in row["max_order_amount"].split(",")
                ],
                "buy_if_price_lt": [
                    float(v.strip()) for v in row["buy_if_price_lt"].split(",")
                ],
                "buy_for_amount": [
                    float(v.strip()) for v in row["buy_for_amount"].split(",")
                ],
            }
            for row in rows
        ]

    def _parse_value(self, key: str, value: Any) -> Any:
        """
        Parse a configuration value based on its expected data type.

        Args:
            key: Configuration key
            value: Raw value from database

        Returns:
            Parsed value in the correct type
        """
        default_value, data_type = CONFIG_DEFAULTS.get(key, (None, str))

        if value is None:
            return default_value

        if data_type == bool:
            try:
                return str(value).strip().lower() in ["true", "1", "yes"]
            except ValueError:
                print(f"Error: key {key} has issues for bool conversion.")
                return default_value

        if data_type in [int, float]:
            try:
                return data_type(value)
            except ValueError:
                print(f"Error: key {key} has issues for {data_type.__name__} conversion.")
                return default_value

        if isinstance(default_value, list):
            return value.split(",")

        return value

    def _start_auto_refresh(self) -> None:
        """Start background thread for periodic configuration refresh."""
        def refresh():
            while True:
                time.sleep(self.refresh_interval)
                self._load_config_from_db()

        thread = threading.Thread(target=refresh, daemon=True)
        thread.start()

    # =========================================================================
    # Configuration Access
    # =========================================================================

    def get(
        self,
        key: str,
        default: Any = None,
        data_type: type = str,
        parse_list: bool = False
    ) -> Any:
        """
        Retrieve a configuration value with optional type conversion.

        Args:
            key: Configuration key to retrieve
            default: Default value if key not found
            data_type: Expected data type (str, int, float, bool)
            parse_list: If True, parse value as comma-separated ranges

        Returns:
            Configuration value in the requested type
        """
        # Get value from config data or defaults
        value = self.config_data.get(
            key,
            CONFIG_DEFAULTS.get(key, (default, data_type))[0]
        )

        if value is None:
            return default

        # Handle boolean conversion
        if data_type == bool:
            return str(value).strip().lower() in ["true", "1", "yes"]

        # Handle numeric conversion
        if data_type in [int, float]:
            try:
                return data_type(value)
            except ValueError:
                return default

        # Handle list parsing (format: "1-10,20-30,40-50")
        if parse_list:
            try:
                return [tuple(map(int, x.split("-"))) for x in value.split(",")]
            except ValueError:
                return default

        return value

    def get_log_level(self) -> str:
        """Get the current logging level from configuration."""
        return self.config_data.get("log_level", "INFO")

    def get_attr(self, attr_config_key: str, table_name: str) -> List[str]:
        """
        Fetch list of values for a config key from a specific table.

        Args:
            attr_config_key: Configuration key to fetch
            table_name: Table to query

        Returns:
            List of values, or empty list on error
        """
        print(f"Fetching value for {attr_config_key}")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)

                query = f"""
                    SELECT value FROM {table_name}
                    WHERE profile_id = %s AND config_key = %s
                """
                cursor.execute(query, (self.profile_id, attr_config_key))
                result = cursor.fetchall()

            return [row["value"] for row in result] if result else []

        except Exception as e:
            print(f"Error fetching {attr_config_key} from table {table_name}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def update_attr(self, attr_config_key: str, attr_value: Any) -> None:
        """
        Update a configuration value in the database.

        Args:
            attr_config_key: Configuration key to update
            attr_value: New value to set
        """
        print(f"Updating {attr_config_key} with value {attr_value}...")

        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "UPDATE dd_common_config SET value = %s WHERE config_key = %s AND profile_id = %s",
                    (attr_value, attr_config_key, self.profile_id)
                )
                self.db_connection.commit()
        except Exception as e:
            print(f"Error updating {attr_config_key}: {e}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def __getattr__(self, key: str) -> Any:
        """
        Enable attribute-style access to configuration values.

        Args:
            key: Configuration key

        Returns:
            Configuration value, or empty string if not found
        """
        return self.config_data.get(key, "")

    # =========================================================================
    # Order Range Access
    # =========================================================================

    def get_bar_order_ranges(
        self,
        bar_strength: float,
        order_type: str = "regular"
    ) -> Dict[str, str]:
        """
        Retrieve order configuration for a given bar strength.

        Args:
            bar_strength: Bar strength value (0.0 to 1.0)
            order_type: "regular" or "choppy"

        Returns:
            Dictionary with order configuration strings
        """
        with self.lock:
            if order_type == "choppy":
                ranges = self.config_data.get("dd_choppy_bar_order_range", [])
            else:
                ranges = self.config_data.get("dd_bar_order_range", [])

        for entry in ranges:
            if entry["min_bar_strength"] <= bar_strength <= entry["max_bar_strength"]:
                return {
                    "candle_size_range": ",".join(
                        [f"{v[0]}-{v[1]}" for v in entry["candle_size_range"]]
                    ),
                    "limit_order_cutoff_price": ",".join(
                        [str(round(v / 100, 2)) for v in entry["limit_order_cutoff_price"]]
                    ),
                    "limit_order_qty_to_buy": ",".join(
                        map(str, entry["limit_order_qty_to_buy"])
                    ),
                    "market_order_qty_to_buy": ",".join(
                        map(str, entry["market_order_qty_to_buy"])
                    ),
                    "max_order_amount": ",".join(map(str, entry["max_order_amount"])),
                    "buy_if_price_lt": ",".join(map(str, entry["buy_if_price_lt"])),
                    "buy_for_amount": ",".join(map(str, entry["buy_for_amount"])),
                }

        # Return empty defaults
        return {
            "candle_size_range": "",
            "limit_order_cutoff_price": "",
            "limit_order_qty_to_buy": "",
            "market_order_qty_to_buy": "",
            "max_order_amount": "",
            "buy_if_price_lt": "",
            "buy_for_amount": "",
        }

    # =========================================================================
    # Profile Management
    # =========================================================================

    def get_active_profile_list(self) -> List[int]:
        """Fetch list of active profile IDs."""
        print("Fetching all active profile IDs...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute("SELECT profile_id FROM user_profile WHERE is_active = 1")
                result = cursor.fetchall()
            
            print(f"Raw active profile data: {result}")
            return [row["profile_id"] for row in result] if result else []
        except Exception as e:
            print(f"Error fetching active profile list: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_active_profile_list_with_type(self) -> List[Dict]:
        """Fetch list of active profile IDs with account types."""
        print("Fetching all active profile IDs and account types...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT profile_id, account_type FROM user_profile WHERE is_active = 1"
                )
                result = cursor.fetchall()
            
            print(f"Raw active profile data: {result}")
            return result if result else []
        except Exception as e:
            print(f"Error fetching active profile list: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_inactive_profile_list(self) -> List[int]:
        """Fetch list of inactive profile IDs."""
        print("Fetching all inactive profile IDs...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute("SELECT profile_id FROM user_profile WHERE is_active = 0")
                result = cursor.fetchall()
            
            return [row["profile_id"] for row in result] if result else []
        except Exception as e:
            print(f"Error fetching inactive profile list: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_active_profile_id(self) -> Optional[int]:
        """Fetch the is_active status for the current profile."""
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return None
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT is_active FROM user_profile WHERE profile_id = %s",
                    (self.profile_id,)
                )
                result = cursor.fetchone()
            
            return result["is_active"] if result else None
        except Exception as e:
            print(f"Error fetching active profile_id: {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    # =========================================================================
    # Trading Rules
    # =========================================================================

    def fetch_active_rules(self) -> List[Dict]:
        """Fetch all active trading rules for the current profile."""
        print("Fetching all active rules...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT * FROM dd_trading_rules WHERE profile_id = %s AND is_active = 1",
                    (self.profile_id,)
                )
                rules = cursor.fetchall()
            
            return rules
        except Exception as e:
            print(f"Error fetching active rules: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_rule_conditions(self, rule_id: int) -> List[Dict]:
        """Fetch conditions for a specific trading rule."""
        print("Fetching all rule conditions...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT * FROM dd_trading_rule_conditions WHERE rule_id = %s",
                    (rule_id,)
                )
                conditions = cursor.fetchall()
            
            return conditions
        except Exception as e:
            print(f"Error fetching rule conditions: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def fetch_rule_actions(self, rule_id: int) -> List[Dict]:
        """Fetch actions for a specific trading rule."""
        print("Fetching all rule actions...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "SELECT * FROM dd_trading_rule_actions WHERE rule_id = %s",
                    (rule_id,)
                )
                actions = cursor.fetchall()
            
            return actions
        except Exception as e:
            print(f"Error fetching rule actions: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def update_last_executed(self, rule_id: int) -> None:
        """Update the last_executed timestamp for a trading rule."""
        print(f"Updating last_executed timestamp for rule {rule_id}")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    "UPDATE dd_trading_rules SET last_executed = NOW() WHERE id = %s",
                    (rule_id,)
                )
                self.db_connection.commit()
        except Exception as e:
            print(f"Error updating last_executed: {e}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    # =========================================================================
    # Membership Management
    # =========================================================================

    def downgrade_expired_memberships(self) -> None:
        """Downgrade expired premium memberships to free tier."""
        print("Checking for expired premium memberships...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return
                
                cursor = self.db_connection.cursor()
                cursor.execute("""
                    UPDATE dd_membership
                    SET membership_type = 'Free', status = 'Expired'
                    WHERE end_date < CURDATE()
                """)
                affected_rows = cursor.rowcount
                self.db_connection.commit()
            
            print(f"{affected_rows} membership(s) downgraded from Premium to Free.")
        except mysql.connector.Error as err:
            print(f"Error updating membership table: {err}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_active_memberships_by_type(self, membership_type: str) -> List[Dict]:
        """Fetch active memberships of a specific type."""
        print(f"Fetching active memberships with type '{membership_type}'...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    """
                    SELECT * FROM dd_membership
                    WHERE membership_type = %s AND end_date >= CURDATE()
                    """,
                    (membership_type,)
                )
                result = cursor.fetchall()
            
            print(f"Found {len(result)} active '{membership_type}' memberships.")
            return result
        except Exception as e:
            print(f"Error fetching active memberships: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_expired_memberships_by_type(self, membership_type: str) -> List[Dict]:
        """Fetch expired memberships of a specific type."""
        print(f"Fetching expired memberships with type '{membership_type}'...")
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return []
                
                cursor = self.db_connection.cursor(dictionary=True)
                cursor.execute(
                    """
                    SELECT * FROM dd_membership
                    WHERE membership_type = %s AND end_date < CURDATE()
                    """,
                    (membership_type,)
                )
                result = cursor.fetchall()
            
            print(f"Found {len(result)} expired '{membership_type}' memberships.")
            return result
        except Exception as e:
            print(f"Error fetching expired memberships: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    # =========================================================================
    # Database Operations
    # =========================================================================

    def execute_query(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Execute a database query safely.

        Args:
            query: SQL query to execute
            params: Optional query parameters
        """
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return
                
                cursor = self.db_connection.cursor()
                cursor.execute(query, params or ())
                self.db_connection.commit()
        except Exception as e:
            print(f"❌ Error executing SQL query: {e}")
            with self.lock:
                if self.db_connection is not None and self.db_connection.is_connected():
                    try:
                        self.db_connection.rollback()
                    except Exception:
                        pass
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def update_config_in_db(self, query: str, params: Optional[tuple] = None) -> None:
        """
        Execute an update query with error handling.

        Args:
            query: SQL update query
            params: Optional query parameters
        """
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    return
                
                cursor = self.db_connection.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                self.db_connection.commit()
        except mysql.connector.Error as err:
            print(f"Error updating config in DB: {err}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def insert_event(
        self,
        event_date: str,
        description: str,
        category: str,
        source: str = "openai_web_search"
    ) -> None:
        """
        Insert a market event into the event heatmap table.

        Args:
            event_date: Event date in YYYY-MM-DD format
            description: Event description
            category: Event category (macro_data, fed_event, earnings, etc.)
            source: Data source identifier
        """
        cursor = None
        try:
            self._ensure_connection()
            
            with self.lock:
                if self.db_connection is None or not self.db_connection.is_connected():
                    raise Exception("Database connection not available")
                
                cursor = self.db_connection.cursor(dictionary=True)

                cursor.execute(
                    """
                    INSERT INTO dd_macro_event_heatmap (event_date, description, category, source)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (event_date, description, category, source)
                )
                self.db_connection.commit()

        except Exception as e:
            print(f"❌ Failed to insert event: {description[:40]}... Error: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

