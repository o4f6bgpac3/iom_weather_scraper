from weather_scraper import check_run_success
from datetime import date, datetime
import sys
import argparse

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format. Use YYYY-MM-DD.")

def main():
    parser = argparse.ArgumentParser(description="Check the status of the weather scraper run for a specific date.")
    parser.add_argument('-d', '--date', type=parse_date, default=date.today(),
                        help="The date to check in YYYY-MM-DD format. Defaults to today's date if not provided.")
    
    args = parser.parse_args()
    check_date = args.date

    success, html_content_id = check_run_success(check_date)
    
    if success:
        print(f"Run on {check_date} was successful. HTML Content ID: {html_content_id}")
        sys.exit(0)
    else:
        print(f"Run on {check_date} failed or did not occur.")
        sys.exit(1)

if __name__ == "__main__":
    main()