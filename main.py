"""
Entry point for mobile.de car scraper.
Run: python main.py "https://suchen.mobile.de/fahrzeuge/search.html?..."
"""
import sys
from scraper import run_scraper

if __name__ == "__main__":
    search_url = sys.argv[1] if len(sys.argv) > 1 else ""
    if not search_url.strip():
        print("Usage: python main.py <mobile.de search URL>")
        print("Example: python main.py \"https://suchen.mobile.de/fahrzeuge/search.html?dam=0&isSearchRequest=true&ms=3500&ref=quickSearch&sb=rel\"")
        sys.exit(1)
    run_scraper(search_url)
