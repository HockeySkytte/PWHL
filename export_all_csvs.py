"""
Convenience wrapper to run the bulk CSV exporter from the project root.
Usage:
    python export_all_csvs.py
Optional:
    Set PWHL_BASE_URL to point to your deployed server instead of localhost.
"""

from scripts.export_all_csvs import main

if __name__ == "__main__":
    main()
