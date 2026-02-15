from datetime import datetime
import logging
import json
import os


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COUNTRIES_METADATA_FILE = os.path.join(BASE_DIR, "data", "Countries Metadata.json")

with open(COUNTRIES_METADATA_FILE, "r", encoding="utf-8") as f:
    country_metadata = {c["name"]: c for c in json.load(f)}


class DataMapper:
    @staticmethod
    def combine_data(subscribers):
        """
        Combines subscriber and purchase data into a unified format.

        Args:
            subscribers (list): List of subscriber dictionaries.
            purchases (list): List of purchase dictionaries.

        Returns:
            list: Combined data in a unified format.
        """
        # Create a map of purchases by email for quick lookup
        combined_data = []

        for subscriber in subscribers:
            try:
                # Extract subscriber email and created_at
                subscriber_email = subscriber.get('email', '')
                subscriber_created_at = subscriber.get('created_at', '')

                formatted_date = None  # Default fallback value
                if subscriber_created_at:
                    try:
                        parsed_date = datetime.strptime(subscriber_created_at, "%Y-%m-%dT%H:%M:%SZ")
                        # Calculate days since Excel epoch (1900-01-01)
                        excel_epoch = datetime(1900, 1, 1)
                        delta = parsed_date - excel_epoch
                        # Add 1 day because Excel counts 1900-01-01 as day 1
                        serial_date = delta.days + 2
                        formatted_date = serial_date
                    except ValueError as e:
                        logging.error(f"Invalid date format for {subscriber_created_at}: {e}")
                        formatted_date = 0  # Fallback to 0 for spreadsheet compatibility

                # Safely handle referrer_info
                referrer_info = subscriber.get("referrer_info", {})
                if referrer_info is None:
                    referrer_info = {}  # Default to an empty dictionary if None
                    logging.warning(f"Referrer info is None for subscriber: {subscriber_email}")

                # Safely access nested referrer_info fields
                referrer_name = referrer_info.get("origin", {}).get("name", "")
                referrer_domain = referrer_info.get("referrer_domain", "")
                referrer_utm = referrer_info.get("referrer_utm", {})
                referrer_utm_source = referrer_utm.get("source", "")
                referrer_utm_medium = referrer_utm.get("medium", "")
                referrer_utm_campaign = referrer_utm.get("campaign", "")
                referrer_utm_content = referrer_utm.get("content", "")

                country = subscriber.get("location_country", "")
                subscriber_region = country_metadata.get(country, {}).get("region", "N/A")
                purchasing_power = country_metadata.get(country, {}).get("purchasing_power", "N/A")
                purchase_score = country_metadata.get(country, {}).get("purchase_score", "N/A")

                # Combine all data into a single record
                combined_record = {
                    "subscriber_created_at": formatted_date,
                    "subscriber_state": subscriber.get("status", ""),
                    "subscriber_email": subscriber_email,
                    "referrer_name": referrer_name,
                    "referrer_domain": referrer_domain,
                    "referrer_utm_source": referrer_utm_source,
                    "referrer_utm_medium": referrer_utm_medium,
                    "referrer_utm_campaign": referrer_utm_campaign,
                    "referrer_utm_content": referrer_utm_content,
                    "subscriber_physical_state": subscriber.get("location_state", ""),
                    "subscriber_country": subscriber.get("location_country", ""),
                    "Subscriber Region": subscriber_region,
                    "Subscriber Purchase Power" : purchasing_power,
                    "Subscriber Purchase Score": purchase_score 
                }

                combined_data.append(combined_record)

            except Exception as e:
                logging.error(f"Error processing subscriber {subscriber_email}: {e}")
                continue  # Skip this subscriber and continue with the next one

        return combined_data