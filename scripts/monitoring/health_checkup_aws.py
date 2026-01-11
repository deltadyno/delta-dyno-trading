"""
Health Check Monitor for AWS EC2 Instance

This script monitors the running status of trading system scripts and automatically
restarts them if they are not running. It also manages inactive profiles by closing
their associated processes.

Features:
- Automatic process monitoring and restart
- Health status logging to Excel files
- Inactive profile management
- tmux session management
"""

import os
import signal
import subprocess
import time
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import psutil

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader


# =============================================================================
# Configuration
# =============================================================================

# Virtual environment activation path
ENV_PATH = "/home/ec2-user/mypythonenv/myenv/bin/activate"
# ENV_PATH = "source /Users/gagandeepsingh/Documents/AlpacaScripts/mypythonenv/myenv3.9/bin/activate"

# Directory where all scripts are located
SCRIPTS_DIRECTORY = "/home/ec2-user/deltadynocode"
# SCRIPTS_DIRECTORY = "/Users/gagandeepsingh/Documents/AlpacaScripts/GGv1.7_queue"


# =============================================================================
# Database Functions
# =============================================================================

def create_connection(db_host: str, db_user: str, db_password: str, db_name: str):
    """Create a database connection."""
    import mysql.connector
    return mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )


def get_active_profile_list(db_host: str, db_user: str, db_password: str, db_name: str) -> List[int]:
    """Fetch the list of active profile IDs."""
    print("Fetching all active profile IDs...")
    try:
        db_connection = create_connection(db_host, db_user, db_password, db_name)
        cursor = db_connection.cursor(dictionary=True)
        cursor.execute("SELECT SQL_NO_CACHE profile_id FROM user_profile WHERE is_active = 1")
        result = cursor.fetchall()
        cursor.close()
        db_connection.close()
        return [row["profile_id"] for row in result] if result else []
    except Exception as e:
        print(f"Error fetching active profile list: {e}")
        return []


def get_inactive_profile_list(db_host: str, db_user: str, db_password: str, db_name: str) -> List[int]:
    """Fetch the list of inactive profile IDs."""
    print("Fetching all inactive profile IDs...")
    try:
        db_connection = create_connection(db_host, db_user, db_password, db_name)
        cursor = db_connection.cursor(dictionary=True)
        cursor.execute("SELECT SQL_NO_CACHE profile_id FROM user_profile WHERE is_active = 0")
        result = cursor.fetchall()
        cursor.close()
        db_connection.close()
        return [row["profile_id"] for row in result] if result else []
    except Exception as e:
        print(f"Error fetching inactive profile list: {e}")
        return []


# =============================================================================
# Script Monitoring
# =============================================================================

def generate_scripts_to_monitor(profile_list: List[int]) -> List[Tuple[str, List[str]]]:
    """
    Generate list of scripts to monitor.

    Args:
        profile_list: List of active profile IDs

    Returns:
        List of tuples: (script_name, [args])
    """
    scripts = [
        ("main", []),  # Breakout detector (main.py)
        ("leaderboard_summary", []),  # Leaderboard summary script
        ("option_manager", ['2']),  # Option manager (if needed)
    ]

    for profile_id in profile_list:
        profile_str = str(profile_id)
        # Note: If you have a profiles.py script, add it here
        # scripts.append(("profiles", [profile_str]))
        scripts.append(("order_monitor", [profile_str]))  # Limit order monitor
        scripts.append(("equity_monitor", [profile_str]))  # Market equity monitor

    return scripts


def is_script_running(script_name: str, args: List[str]) -> bool:
    """
    Check if a script with the given name is running.

    Args:
        script_name: Name of the script (without .py extension)
        args: List of command line arguments

    Returns:
        True if script is running, False otherwise
    """
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']

            if cmdline:
                # Check if script name is in any part of command line
                script_found = any(script_name in part for part in cmdline)
                # Check if all args are in command line
                args_found = all(arg in cmdline for arg in args) if args else True

                if script_found and args_found:
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, KeyError):
            continue

    print(f"[HEALTH CHECK] Script {script_name} with args {args} is NOT running.")
    return False


def restart_script_in_new_terminal(script_name: str, args: List[str]) -> Tuple[bool, str]:
    """
    Restart a missing script in a new tmux session with the virtual environment activated.

    Args:
        script_name: Name of the script (without .py extension)
        args: List of command line arguments

    Returns:
        Tuple of (success: bool, message: str)
    """
    time.sleep(2)
    try:
        # Determine script path
        script_path = os.path.join(SCRIPTS_DIRECTORY, f"{script_name}.py")

        if not os.path.exists(script_path):
            return False, f"Script not found: {script_path}"

        args_str = " ".join(args) if args else ""
        terminal_name = f"{script_name}.py {args_str}".strip()

        print(f"[HEALTH CHECK] Restarting script: {script_name} with args {args} in tmux session: {terminal_name}")

        # Create tmux session with script execution
        cmd = f"cd {SCRIPTS_DIRECTORY} && source {ENV_PATH} && python {script_path} {args_str}"
        subprocess.Popen([
            "tmux", "new-session", "-d", "-s", terminal_name, cmd
        ])

        return True, "Success"
    except Exception as e:
        return False, str(e)


# =============================================================================
# Health Check Main
# =============================================================================

def health_check(profile_list: List[int]) -> None:
    """
    Perform the health check, log status, and restart scripts as needed.

    Args:
        profile_list: List of active profile IDs
    """
    print("[HEALTH CHECK] Starting health check.")
    if not profile_list:
        print("No active profiles found!")
        return
    else:
        print(f"Checking health for profiles: {profile_list}")

    scripts_to_monitor = generate_scripts_to_monitor(profile_list)

    status_report = []
    for script_name, args in scripts_to_monitor:
        is_running = is_script_running(script_name, args)
        if not is_running:
            print(f"[HEALTH CHECK] {script_name} with args {args} is not running. Attempting to restart.")
            restarted, message = restart_script_in_new_terminal(script_name, args)
            status = f"Not Running | Restarted: {restarted} ({message if not restarted else 'Success'})"
            status_report.append({
                "Script": script_name,
                "Arguments": " ".join(args),
                "Status": status,
            })

    if status_report:
        log_status(status_report)
    else:
        print("[HEALTH CHECK] All scripts are running. No log entry created.")
    print("[HEALTH CHECK] Health check completed.")


# =============================================================================
# Logging
# =============================================================================

def log_status(
    status_report: List[dict],
    folder_name: str = "health_checkup_report",
    file_name: str = "health_status_log.xlsx"
) -> None:
    """
    Log the health check status to an Excel file.

    Args:
        status_report: List of status dictionaries
        folder_name: Folder name for logs
        file_name: Excel file name
    """
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"[DEBUG] Created folder: {folder_name}")

    log_file = os.path.join(folder_name, file_name)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Add timestamp to entries
    for entry in status_report:
        entry['Timestamp'] = timestamp

    df = pd.DataFrame(status_report)

    try:
        if os.path.exists(log_file):
            # Append to existing file
            existing_df = pd.read_excel(log_file)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_excel(log_file, index=False)
        else:
            df.to_excel(log_file, index=False)

        print(f"[HEALTH CHECK] Status successfully logged to {log_file}.")
    except Exception as e:
        print(f"Failed to log status: {e}")


# =============================================================================
# Process Management
# =============================================================================

def tmux_list_sessions() -> List[str]:
    """List all active tmux sessions."""
    result = subprocess.run(["tmux", "list-sessions", "-F", "#S"], stdout=subprocess.PIPE, text=True)
    return result.stdout.splitlines()


def close_processes(
    process_name: Optional[str] = None,
    process_arg: Optional[str] = None,
    close_all: bool = False
) -> None:
    """
    Kill all processes matching the given name and optional argument, and close their tmux sessions.

    Args:
        process_name: Process name to match (e.g., "order_monitor")
        process_arg: Optional argument to match (e.g., profile ID)
        close_all: If True, close all relevant processes
    """
    current_pid = os.getpid()

    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']

            if close_all or (cmdline and process_name and any(process_name in part for part in cmdline)):
                if process_arg and process_arg not in cmdline:
                    continue  # Skip processes not matching the specified argument

                pid = proc.info['pid']
                if pid == current_pid:
                    continue  # Skip the current process

                print(f"[CLOSE] Killing process {cmdline} with PID {pid}.")
                os.kill(pid, signal.SIGTERM)

                # Kill associated tmux session
                if close_all:
                    for session in tmux_list_sessions():
                        if ".py" in session:
                            print(f"[CLOSE] Killing session: {session}")
                            subprocess.Popen(["tmux", "kill-session", "-t", session])
                else:
                    specific_window_name = f"{process_name}.py {process_arg}" if process_arg else f"{process_name}.py"
                    session_name = specific_window_name.replace(" ", "_")
                    print(f"[CLOSE] Killing session: {session_name}")
                    try:
                        subprocess.Popen(["tmux", "kill-session", "-t", session_name])
                    except Exception:
                        pass  # Session might not exist

        except (psutil.NoSuchProcess, psutil.AccessDenied, KeyError):
            continue


# =============================================================================
# Main Entry Point
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor and manage trading system scripts.",
        epilog=(
            "Examples:\n"
            "  python health_checkup_aws.py --close equity_monitor\n"
            "  python health_checkup_aws.py --close equity_monitor:3\n"
            "  python health_checkup_aws.py --close equity_monitor:3,order_monitor:1\n"
            "  python health_checkup_aws.py --close ALL\n"
            "  python health_checkup_aws.py --close_profile_id 3\n\n"
            "Use `:` to specify an argument for a specific process.\n"
            "If no argument is provided, all processes matching the name will be terminated."
        ),
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "--close",
        type=str,
        help=(
            "Comma-separated list of process names to terminate.\n"
            "You can specify arguments using `name:arg` format, or use 'ALL' to close all relevant processes.\n\n"
            "Examples:\n"
            "  --close equity_monitor (close all processes with this name)\n"
            "  --close equity_monitor:3 (close processes with name and argument 3)\n"
            "  --close ALL (close all processes matching relevant strings)"
        ),
        default=None
    )

    parser.add_argument(
        "--close_profile_id",
        type=str,
        help=(
            "Client ID for which to close associated processes.\n"
            "This will terminate the following processes with the given ID as an argument:\n"
            "  - equity_monitor.py\n"
            "  - order_monitor.py"
        ),
        default=None
    )

    args = parser.parse_args()

    if args.close:
        if args.close.upper() == "ALL":
            print("[CLOSE] Closing all relevant processes.")
            patterns = ["order_monitor", "equity_monitor", "main"]
            for pattern in patterns:
                close_processes(pattern)
        else:
            close_targets = args.close.split(',')
            for target in close_targets:
                if ':' in target:
                    process_name, process_arg = target.split(':', 1)
                    print(f"[CLOSE] Closing process: {process_name} with argument: {process_arg}")
                    close_processes(process_name, process_arg)
                else:
                    print(f"[CLOSE] Closing all processes matching: {target}")
                    close_processes(target)

    elif args.close_profile_id:
        profile_id = args.close_profile_id
        processes_to_close = [
            ("equity_monitor", profile_id),
            ("order_monitor", profile_id),
        ]
        for process_name, process_arg in processes_to_close:
            print(f"[CLOSE] Closing process: {process_name} with client ID: {process_arg}")
            close_processes(process_name, process_arg)

    else:
        # Load configuration
        file_config = ConfigLoader(config_file='config/config.ini')

        # Main monitoring loop
        while True:
            inactive_profile_list = get_inactive_profile_list(
                file_config.db_host,
                file_config.db_user,
                file_config.db_password,
                file_config.db_name
            )

            # Close all inactive processes
            if inactive_profile_list:
                print(f"[CLOSE] Closing NEW inactive profiles: {inactive_profile_list}")
                for profile_id in inactive_profile_list:
                    processes_to_close = [
                        ("equity_monitor", str(profile_id)),
                        ("order_monitor", str(profile_id)),
                    ]
                    for process_name, process_arg in processes_to_close:
                        print(f"[CLOSE] Closing process: {process_name} with profile ID: {process_arg}")
                        close_processes(process_name, process_arg)

            active_profile_list = get_active_profile_list(
                file_config.db_host,
                file_config.db_user,
                file_config.db_password,
                file_config.db_name
            )
            health_check(active_profile_list)
            time.sleep(15)

