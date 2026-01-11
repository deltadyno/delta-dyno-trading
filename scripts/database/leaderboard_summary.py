"""
Leaderboard Summary Script

This script aggregates leaderboard data from dd_leaderboard table into summary
statistics for daily, weekly, and monthly periods. It's typically run via cron job
to update the dd_leaderboard_summary table which is read by the frontend UI.

Usage:
    python leaderboard_summary.py

Note: This script should be run periodically (e.g., daily via cron) to keep
the summary table up to date.
"""

import os

from deltadyno.config.database import DatabaseConfigLoader
from deltadyno.config.loader import ConfigLoader
from deltadyno.utils.logger import setup_logger


def update_leaderboard_summary(db_config_loader: DatabaseConfigLoader, logger):
    """
    Update the leaderboard summary table with aggregated statistics.

    This function:
    1. Fetches leaderboard data from the last 32 days
    2. Aggregates data into daily, weekly, and monthly buckets
    3. Calculates ROI percentages and rankings
    4. Inserts/updates the dd_leaderboard_summary table

    Args:
        db_config_loader: Database configuration loader instance
        logger: Logger instance
    """
    try:
        insert_query = """
            INSERT INTO dd_leaderboard_summary (
                profile_id,
                period_type,
                period_key,
                period_end_date,
                rank_position,
                roi_percent,
                return_amount,
                investment
            )
            WITH raw AS (
                SELECT
                    profile_id,
                    snapdate AS d_date,
                    YEARWEEK(snapdate, 3) AS iso_week,
                    DATE_FORMAT(snapdate, '%Y-%m') AS y_month,
                    SUM(return_amount) AS r_amount,
                    SUM(investment) AS inv_amount
                FROM dd_leaderboard
                WHERE snapdate >= CURDATE() - INTERVAL 32 DAY
                GROUP BY profile_id, d_date, iso_week, y_month
            ),
            buckets AS (
                SELECT
                    profile_id,
                    'daily' AS period_type,
                    DATE_FORMAT(d_date, '%Y-%m-%d') AS period_key,
                    d_date AS period_end_date,
                    r_amount AS return_amount,
                    inv_amount AS investment
                FROM raw

                UNION ALL

                SELECT
                    profile_id,
                    'weekly',
                    CONCAT(LEFT(iso_week, 4), '-W', RIGHT(iso_week, 2)),
                    STR_TO_DATE(CONCAT(iso_week, ' Sunday'), '%X%V %W'),
                    SUM(r_amount),
                    SUM(inv_amount)
                FROM raw
                GROUP BY profile_id, iso_week

                UNION ALL

                SELECT
                    profile_id,
                    'monthly',
                    y_month,
                    LAST_DAY(STR_TO_DATE(CONCAT(y_month, '-01'), '%Y-%m-%d')),
                    SUM(r_amount),
                    SUM(inv_amount)
                FROM raw
                GROUP BY profile_id, y_month
            ),
            ranked AS (
                SELECT
                    profile_id,
                    period_type,
                    period_key,
                    period_end_date,
                    IFNULL(100 * return_amount / NULLIF(investment, 0), 0) AS roi_percent,
                    return_amount,
                    investment,
                    ROW_NUMBER() OVER (
                        PARTITION BY period_type, period_key
                        ORDER BY 100 * return_amount / NULLIF(investment, 0) DESC
                    ) AS rank_position
                FROM buckets
            )
            SELECT *
            FROM (
                SELECT
                    profile_id,
                    period_type,
                    period_key,
                    period_end_date,
                    rank_position,
                    roi_percent,
                    return_amount,
                    investment
                FROM ranked
            ) AS ranked_final
            ON DUPLICATE KEY UPDATE
                rank_position = VALUES(rank_position),
                roi_percent   = VALUES(roi_percent),
                return_amount = VALUES(return_amount),
                investment    = VALUES(investment);
        """

        logger.debug(f"Insert Query: {insert_query}")
        db_config_loader.update_config_in_db(insert_query, None)
        logger.info("[update_leaderboard_summary] Leaderboard summary successfully updated")
        print("[update_leaderboard_summary] Leaderboard summary successfully updated")

    except Exception as e:
        print(f"update_leaderboard_summary Database update failed: {e}")
        logger.error(f"[update_leaderboard_summary] Database update failed: {e}")


def main():
    """Main entry point for the leaderboard summary script."""
    # Determine config file path (AWS EC2 vs local)
    if os.path.exists('/home/ec2-user/deltadynocode/config.ini'):
        config_path = '/home/ec2-user/deltadynocode/config.ini'
    else:
        config_path = 'config/config.ini'

    logs_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Load configuration
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

    # Initialize logger
    logger = setup_logger(
        db_config_loader,
        log_to_file=True,
        file_name=os.path.join(logs_dir, "leaderboard_summary.log")
    )

    # Update leaderboard summary
    update_leaderboard_summary(db_config_loader, logger)


if __name__ == "__main__":
    main()

