"""
Client Management Script

This script manages client accounts, profiles, and API credentials.
It can add new clients, remove existing clients, and update profile settings.

Features:
- Add new clients with multiple profiles
- Remove clients and associated configurations
- Update API keys/secrets and active status
- AWS SSM Parameter Store integration for credentials
- Database configuration management

Usage:
    python manage_clients.py ADD --api_key key1,key2,key3,key4 --api_secret secret1,secret2,secret3,secret4 --email user@example.com
    python manage_clients.py REMOVE --email user@example.com
    python manage_clients.py UPDATE --profile_id 123 --api_key newkey --api_secret newsecret --is_active
"""

import argparse
import os
import sys
from typing import Optional

import boto3
import mysql.connector

from deltadyno.config.loader import ConfigLoader

# Import from same directory
from scripts.admin.configToDB import process_ini_data


# =============================================================================
# AWS SSM Parameter Functions
# =============================================================================

def delete_parameter(name: str, region: str = "us-east-1") -> None:
    """
    Delete an SSM parameter in AWS Systems Manager.

    Args:
        name: The name of the parameter to delete
        region: AWS region where the parameter is stored
    """
    ssm = boto3.client('ssm', region_name=region)

    try:
        ssm.delete_parameter(Name=name)
        print(f"Parameter '{name}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting parameter '{name}': {e}")


def create_or_update_parameter(
    name: str,
    value: str,
    overwrite_flag: bool,
    parameter_type: str = "SecureString",
    region: str = "us-east-1"
) -> Optional[dict]:
    """
    Create or update an SSM parameter in AWS Systems Manager.

    Args:
        name: The name of the parameter
        value: The value to store in the parameter
        overwrite_flag: Whether to overwrite if parameter exists
        parameter_type: The type of the parameter (String, StringList, SecureString)
        region: AWS region where the parameter should be stored

    Returns:
        Response dictionary or None on error
    """
    ssm = boto3.client('ssm', region_name=region)

    try:
        response = ssm.put_parameter(
            Name=name,
            Value=value,
            Type=parameter_type,
            Overwrite=overwrite_flag
        )
        print(f"Parameter '{name}' created/updated successfully.")
        return response
    except Exception as e:
        print(f"Error creating/updating parameter '{name}': {e}")
        return None


# =============================================================================
# Database Functions
# =============================================================================

def get_db_connection(config: ConfigLoader):
    """
    Establish a connection to the MySQL database.

    Args:
        config: ConfigLoader instance with database credentials

    Returns:
        MySQL connection object
    """
    print("Establishing database connection...")
    return mysql.connector.connect(
        host=config.db_host,
        database=config.db_name,
        user=config.db_user,
        password=config.db_password
    )


# =============================================================================
# Client Management Functions
# =============================================================================

def create_profiles(
    config: ConfigLoader,
    cursor,
    user_id: int,
    is_active: bool,
    api_key: str,
    api_secret: str
) -> None:
    """
    Create 4 profiles for the user with different config settings.

    Args:
        config: ConfigLoader instance
        cursor: Database cursor
        user_id: User ID to create profiles for
        is_active: Whether profiles should be active
        api_key: Comma-separated API keys (4 values)
        api_secret: Comma-separated API secrets (4 values)
    """
    print(f"Creating profiles for user_id {user_id}...")

    config_files = ["beleiveBreakout.ini", "agressive.ini", "smallbuy.ini", "breakoutAggressive.ini"]
    account_types = ["LIVE", "PAPER", "PAPER", "PAPER"]

    # Split api_key and api_secret into lists
    api_keys = api_key.split(",")
    api_secrets = api_secret.split(",")

    if len(api_keys) != 4 or len(api_secrets) != 4:
        print("Error: api_key and api_secret must each contain exactly 4 comma-separated values.")
        return

    # Determine config file base directory
    config_base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if os.path.exists('/home/ec2-user/deltadynocode'):
        config_base_dir = '/home/ec2-user/deltadynocode'

    for i in range(4):
        config_file = config_files[i]
        account_type = account_types[i]
        config_file_path = os.path.join(config_base_dir, config_file)

        # Set profile_name dynamically
        profile_name = config_file.split('.')[0]
        print(f"Creating profile {profile_name} for account type {account_type}...")

        # Insert the profile for the user
        cursor.execute(
            "INSERT INTO user_profile (user_id, profile_name, account_type, is_active) VALUES (%s, %s, %s, %s)",
            (user_id, profile_name, account_type, int(is_active))
        )

        profile_id = cursor.lastrowid
        print(f"Profile created with profile_id {profile_id}")

        # Insert configurations specific to this profile
        if os.path.exists(config_file_path):
            process_ini_data(config_file_path, profile_id, cursor)
        else:
            print(f"Warning: Config file {config_file_path} not found. Skipping configuration import.")

        # Assign corresponding API key and secret for this profile
        create_or_update_parameter(
            f"profile{profile_id}_apikey",
            api_keys[i].strip(),
            False,
            "SecureString"
        )
        create_or_update_parameter(
            f"profile{profile_id}_apisecret",
            api_secrets[i].strip(),
            False,
            "SecureString"
        )


def add_client(
    config: ConfigLoader,
    api_key: str,
    api_secret: str,
    email: str,
    name: Optional[str] = None,
    is_active: bool = False
) -> None:
    """
    Add a new user and create associated profiles.

    Args:
        config: ConfigLoader instance
        api_key: Comma-separated API keys (4 values)
        api_secret: Comma-separated API secrets (4 values)
        email: User email address
        name: Optional user name
        is_active: Whether user should be active
    """
    print(f"Adding client with email {email}...")
    conn = get_db_connection(config)
    cursor = conn.cursor()

    try:
        # Insert user details into users table
        cursor.execute(
            "INSERT INTO users (username, email, hashed_password, is_active) VALUES (%s, %s, %s, %s)",
            (name if name else email, email, '$2b$12$eUJ6A6W/HZ9ncMffD/dkI.k2ZKXyqeMZmrfRFA/Q0a173OimKfrh6', int(is_active))
        )
        conn.commit()

        user_id = cursor.lastrowid
        print(f"User added with ID: {user_id}")

        # Create 4 profiles (1 LIVE, 3 PAPER) for the new user
        create_profiles(config, cursor, user_id, is_active, api_key, api_secret)
        conn.commit()

        print(f"User {email} added successfully with user_id {user_id}")

    except mysql.connector.Error as err:
        print(f"Error while adding client: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def remove_client(config: ConfigLoader, email: str) -> None:
    """
    Remove a user and all related configurations.

    Args:
        config: ConfigLoader instance
        email: User email address
    """
    print(f"Removing client with email {email}...")
    conn = get_db_connection(config)
    cursor = conn.cursor()

    try:
        # Fetch the user_id from users table
        cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
        user = cursor.fetchone()

        if user:
            user_id = user[0]
            print(f"Deleting user with ID: {user_id}...")

            # Get profile IDs associated with the user
            cursor.execute("SELECT profile_id FROM user_profile WHERE user_id = %s", (user_id,))
            profiles = cursor.fetchall()
            print(f"Found {len(profiles)} profiles for user_id {user_id}.")

            if profiles:
                # Delete related configuration entries
                for profile in profiles:
                    profile_id = profile[0]
                    print(f"Deleting configurations for profile_id {profile_id}...")
                    cursor.execute("DELETE FROM dd_common_config WHERE profile_id = %s", (profile_id,))
                    cursor.execute("DELETE FROM dd_open_position_config WHERE profile_id = %s", (profile_id,))
                    cursor.execute("DELETE FROM dd_close_position_config WHERE profile_id = %s", (profile_id,))
                    cursor.execute("DELETE FROM dd_choppy_bar_order_range WHERE profile_id = %s", (profile_id,))
                    cursor.execute("DELETE FROM dd_bar_order_range WHERE profile_id = %s", (profile_id,))
                    print(f"Deleted profile_id {profile_id} configurations.")

                    # Delete SSM parameters
                    delete_parameter(f"profile{profile_id}_apikey")
                    delete_parameter(f"profile{profile_id}_apisecret")

                # Delete the profiles from user_profile table
                cursor.execute("DELETE FROM user_profile WHERE user_id = %s", (user_id,))
                print(f"Deleted profiles for user_id {user_id}.")

            # Finally, delete the user from the users table
            cursor.execute("DELETE FROM users WHERE id = %s", (user_id,))
            conn.commit()

            print(f"User with email {email} and associated data removed successfully.")
        else:
            print(f"User with email {email} not found.")

    except mysql.connector.Error as err:
        print(f"Error while removing client: {err}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def update_profile(
    config: ConfigLoader,
    profile_id: int,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    is_active: Optional[bool] = None
) -> None:
    """
    Update profile settings (API keys/secrets and active status).

    Args:
        config: ConfigLoader instance
        profile_id: Profile ID to update
        api_key: New API key (optional)
        api_secret: New API secret (optional)
        is_active: New active status (optional)
    """
    conn = get_db_connection(config)
    cursor = conn.cursor()

    try:
        # Update is_active flag if provided
        if is_active is not None:
            cursor.execute(
                "UPDATE user_profile SET is_active = %s WHERE profile_id = %s",
                (int(is_active), profile_id)
            )
            print(f"Updated is_active flag to {int(is_active)} for profile_id {profile_id}")

        # Update API key in SSM if provided
        if api_key:
            create_or_update_parameter(f"profile{profile_id}_apikey", api_key, True, "SecureString")
            print(f"Updated api_key in SSM for profile_id {profile_id}")

        # Update API secret in SSM if provided
        if api_secret:
            create_or_update_parameter(f"profile{profile_id}_apisecret", api_secret, True, "SecureString")
            print(f"Updated api_secret in SSM for profile_id {profile_id}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error updating profile_id {profile_id}: {e}")
    finally:
        cursor.close()
        conn.close()


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main entry point for the client management script."""
    parser = argparse.ArgumentParser(
        description="Manage client credentials and configuration files.",
        usage="""%(prog)s ACTION [OPTIONS]

Actions:
  ADD      Add a new client and create associated profiles.
  REMOVE   Remove an existing client and delete associated configurations.
  UPDATE   Update API key/secret or is_active flag for a profile.

Examples:
  ADD a new client:
    python %(prog)s ADD --api_key key1,key2,key3,key4 --api_secret secret1,secret2,secret3,secret4 --email user@example.com --name "User Name" --is_active

  REMOVE a client:
    python %(prog)s REMOVE --email user@example.com

  UPDATE a profile's API keys:
    python %(prog)s UPDATE --profile_id 123 --api_key newkey --api_secret newsecret

  UPDATE a profile's is_active status:
    python %(prog)s UPDATE --profile_id 123 --is_active
        """,
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument("action", choices=["ADD", "REMOVE", "UPDATE"], nargs="?", help="Action to perform.")
    parser.add_argument("--api_key", help="API key (Required for ADD or UPDATE).")
    parser.add_argument("--api_secret", help="API secret (Required for ADD or UPDATE).")
    parser.add_argument("--email", help="Email of the client (Required for ADD and REMOVE).")
    parser.add_argument("--name", help="Optional client name.")
    parser.add_argument("--is_active", action="store_true", help="To mark client/profile as active.")
    parser.add_argument("--profile_id", type=int, help="Profile ID (Required for UPDATE).")

    args = parser.parse_args()

    if not args.action:
        parser.print_help()
        sys.exit(1)

    # Load configuration
    config_path = 'config/config.ini'
    if not os.path.exists(config_path):
        config_path = '/home/ec2-user/deltadynocode/config.ini'

    config = ConfigLoader(config_file=config_path)

    if args.action == "ADD":
        if not args.api_key or not args.api_secret or not args.email:
            print("\nERROR: API key, API secret, and email are required for ADD.\n")
            parser.print_help()
            sys.exit(1)
        add_client(config, args.api_key, args.api_secret, args.email, args.name, args.is_active)

    elif args.action == "REMOVE":
        if not args.email:
            print("\nERROR: Email is required for REMOVE.\n")
            parser.print_help()
            sys.exit(1)

        conn = get_db_connection(config)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT id FROM users WHERE email = %s", (args.email,))
            user = cursor.fetchone()
            if user:
                print(f"User found: {user}")
                remove_client(config, args.email)
            else:
                print("User not found.")
        finally:
            cursor.close()
            conn.close()

    elif args.action == "UPDATE":
        if not args.profile_id:
            print("\nERROR: profile_id is required for UPDATE.\n")
            parser.print_help()
            sys.exit(1)

        update_profile(config, args.profile_id, args.api_key, args.api_secret, args.is_active)


if __name__ == "__main__":
    main()

