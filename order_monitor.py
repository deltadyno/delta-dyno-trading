#!/usr/bin/env python3
"""
DeltaDyno - Limit Order Monitor

Entry point for running the limit order monitor.
Monitors open limit orders and converts them to market orders
or cancels them based on age and price conditions.

Usage:
    python order_monitor.py <profile_id>
    python order_monitor.py <profile_id> --log_to_console
    python order_monitor.py --help
"""

import argparse
import sys

from deltadyno.trading.order_monitor import run_order_monitor


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="DeltaDyno - Limit Order Monitor",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "profile_id",
        help="Client profile ID to monitor"
    )
    parser.add_argument(
        "--log_to_console",
        action="store_true",
        help="Log to console instead of file"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()

    print(f"""
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║   ██████╗ ███████╗██╗  ████████╗ █████╗ ██████╗ ██╗   ██╗   ║
    ║   ██╔══██╗██╔════╝██║  ╚══██╔══╝██╔══██╗██╔══██╗╚██╗ ██╔╝   ║
    ║   ██║  ██║█████╗  ██║     ██║   ███████║██║  ██║ ╚████╔╝    ║
    ║   ██║  ██║██╔══╝  ██║     ██║   ██╔══██║██║  ██║  ╚██╔╝     ║
    ║   ██████╔╝███████╗███████╗██║   ██║  ██║██████╔╝   ██║      ║
    ║   ╚═════╝ ╚══════╝╚══════╝╚═╝   ╚═╝  ╚═╝╚═════╝    ╚═╝      ║
    ║                                                               ║
    ║              Limit Order Monitor System                       ║
    ╚═══════════════════════════════════════════════════════════════╝

    Configuration:
    - Profile ID: {args.profile_id}
    - Logging: {'Console' if args.log_to_console else 'File'}
    """)

    try:
        run_order_monitor(
            profile_id=args.profile_id,
            log_to_console=args.log_to_console
        )
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

