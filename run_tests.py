#!/usr/bin/env python3
"""
Test runner script with HTML report generation.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py unit               # Run unit tests only
    python run_tests.py integration        # Run integration tests only
    python run_tests.py scenario           # Run scenario tests only
    python run_tests.py --quick            # Run smoke/quick tests only
    python run_tests.py --coverage         # Run with detailed coverage
    python run_tests.py --failed           # Re-run only failed tests
"""

import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.absolute()


def ensure_reports_dir() -> Path:
    """Ensure reports directory exists."""
    reports_dir = get_project_root() / "reports"
    reports_dir.mkdir(exist_ok=True)
    return reports_dir


def run_tests(
    test_type: str = "all",
    quick: bool = False,
    coverage: bool = True,
    failed_only: bool = False,
    verbose: bool = True,
) -> int:
    """
    Run pytest with HTML report generation.
    
    Args:
        test_type: Type of tests to run (all, unit, integration, scenario)
        quick: Run only smoke/quick tests
        coverage: Enable coverage reporting
        failed_only: Re-run only previously failed tests
        verbose: Enable verbose output
    
    Returns:
        Exit code from pytest
    """
    project_root = get_project_root()
    reports_dir = ensure_reports_dir()
    
    # Generate timestamp for report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Build pytest command
    cmd = ["python", "-m", "pytest"]
    
    # Test selection
    if test_type == "unit":
        cmd.extend(["-m", "unit", "tests/unit/"])
    elif test_type == "integration":
        cmd.extend(["-m", "integration", "tests/integration/"])
    elif test_type == "scenario":
        cmd.extend(["-m", "scenario", "tests/scenarios/"])
    elif quick:
        cmd.extend(["-m", "smoke or unit", "--maxfail=3"])
    
    # Re-run failed tests
    if failed_only:
        cmd.append("--lf")
    
    # Verbose output
    if verbose:
        cmd.append("-v")
    
    # HTML report
    html_report = reports_dir / f"test_report_{timestamp}.html"
    cmd.extend([
        f"--html={html_report}",
        "--self-contained-html",
    ])
    
    # Coverage options
    if coverage:
        coverage_dir = reports_dir / f"coverage_{timestamp}"
        cmd.extend([
            "--cov=deltadyno",
            f"--cov-report=html:{coverage_dir}",
            "--cov-report=term-missing",
            "--cov-fail-under=0",
        ])
    
    # Additional options
    cmd.extend([
        "--tb=short",
        "-ra",  # Show summary of all test outcomes
        "--strict-markers",
        f"--junitxml={reports_dir / f'junit_{timestamp}.xml'}",
    ])
    
    # Print command
    print(f"\n{'='*60}")
    print(f"Running tests: {' '.join(cmd)}")
    print(f"{'='*60}\n")
    
    # Run pytest
    os.chdir(project_root)
    result = subprocess.run(cmd)
    
    # Print report locations
    print(f"\n{'='*60}")
    print("Test Reports Generated:")
    print(f"{'='*60}")
    print(f"  HTML Report: {html_report}")
    if coverage:
        print(f"  Coverage Report: {coverage_dir}/index.html")
    print(f"  JUnit XML: {reports_dir / f'junit_{timestamp}.xml'}")
    print(f"{'='*60}\n")
    
    # Create symlink to latest report
    latest_html = reports_dir / "test_report_latest.html"
    if latest_html.exists() or latest_html.is_symlink():
        latest_html.unlink()
    latest_html.symlink_to(html_report.name)
    
    if coverage:
        latest_cov = reports_dir / "coverage_latest"
        if latest_cov.exists() or latest_cov.is_symlink():
            latest_cov.unlink()
        latest_cov.symlink_to(f"coverage_{timestamp}")
    
    return result.returncode


def print_usage():
    """Print usage information."""
    print(__doc__)


def main():
    """Main entry point."""
    args = sys.argv[1:]
    
    if "--help" in args or "-h" in args:
        print_usage()
        return 0
    
    test_type = "all"
    quick = False
    coverage = True
    failed_only = False
    
    for arg in args:
        if arg in ("unit", "integration", "scenario"):
            test_type = arg
        elif arg == "--quick":
            quick = True
        elif arg == "--no-coverage":
            coverage = False
        elif arg == "--failed":
            failed_only = True
    
    return run_tests(
        test_type=test_type,
        quick=quick,
        coverage=coverage,
        failed_only=failed_only,
    )


if __name__ == "__main__":
    sys.exit(main())

