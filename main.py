#!/usr/bin/env python3
"""
DeltaDyno - Automated Breakout Detection Trading System

Entry point for running the breakout detector.

Usage:
    python main.py --symbol SPY --length 15 --timeframe_minutes 3
    python main.py --help
"""

import argparse
import sys

from deltadyno.core.breakout_detector import main as run_breakout_detector


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="DeltaDyno - Automated Breakout Detection Trading System",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--symbol",
        default="SPY",
        help="Stock symbol to trade"
    )
    parser.add_argument(
        "--length",
        type=int,
        default=15,
        help="Data length for analysis"
    )
    parser.add_argument(
        "--timeframe_minutes",
        type=int,
        default=3,
        help="Timeframe in minutes"
    )
    parser.add_argument(
        "--slope_method",
        default="Atr",
        choices=["Atr", "Atr2", "Atr3"],
        help="Method for slope calculation"
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
    ║            Automated Breakout Detection System               ║
    ╚═══════════════════════════════════════════════════════════════╝
    
    Configuration:
    - Symbol: {args.symbol}
    - Length: {args.length}
    - Timeframe: {args.timeframe_minutes} minutes
    - Slope Method: {args.slope_method}
    - Logging: {'Console' if args.log_to_console else 'File'}
    """)
    
    try:
        run_breakout_detector(
            symbol=args.symbol,
            length=args.length,
            timeframe_minutes=args.timeframe_minutes,
            slope_method=args.slope_method,
            log_to_file=not args.log_to_console
        )
    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

