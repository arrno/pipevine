#!/usr/bin/env python3
"""
Test runner script for parllel library.

Usage:
    python run_tests.py                    # Run all tests
    python run_tests.py --unit             # Run only unit tests
    python run_tests.py --integration      # Run only integration tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --verbose          # Run with verbose output
"""

import sys
import subprocess
from pathlib import Path


def run_command(cmd: list[str]) -> int:
    """Run a command and return its exit code."""
    print(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode
    except FileNotFoundError:
        print(f"Error: Command '{cmd[0]}' not found. Make sure pytest is installed.")
        return 1


def main() -> int:
    """Main test runner."""
    # Base pytest command
    cmd = ["python", "-m", "pytest", "tests/"]
    
    # Parse simple command line arguments
    if "--unit" in sys.argv:
        cmd.extend(["-m", "unit"])
    elif "--integration" in sys.argv:
        cmd.extend(["-m", "integration"])
    
    if "--verbose" in sys.argv:
        cmd.append("-v")
    
    if "--coverage" in sys.argv:
        cmd.extend(["--cov=.", "--cov-report=html", "--cov-report=term"])
    
    # Add common options
    cmd.extend([
        "--tb=short",           # Shorter traceback format
        "--strict-markers",     # Strict marker checking
        "--disable-warnings",   # Disable pytest warnings for cleaner output
    ])
    
    # Run the tests
    exit_code = run_command(cmd)
    
    if exit_code == 0:
        print("\n✅ All tests passed!")
    else:
        print(f"\n❌ Tests failed with exit code {exit_code}")
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())