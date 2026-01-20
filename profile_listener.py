#!/usr/bin/env python3
"""
Entry point script for the Profile Listener.

This script listens to Redis streams for breakout messages and
creates orders based on configuration for a specific profile.

Usage:
    python profile_listener.py <profile_id>

Example:
    python profile_listener.py 1
"""

import asyncio
import sys

from deltadyno.trading.profile_listener import run_profile_listener


async def main():
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python profile_listener.py <profile_id>")
        print("\nThis script listens to Redis breakout messages and creates")
        print("orders based on the configuration for the specified profile.")
        sys.exit(1)

    profile_id = sys.argv[1]
    print(f"Starting Profile Listener for profile: {profile_id}")
    await run_profile_listener(profile_id)


if __name__ == "__main__":
    asyncio.run(main())


