from datetime import datetime

class SubscriberFilter:
    def __init__(self, start_date, end_date):
        """
        Initialize the filter with a date range.

        Args:
            start_date (datetime): Start date for filtering.
            end_date (datetime): End date for filtering.
        """
        self.start_date = start_date
        self.end_date = end_date

    def filter_by_date(self, subscribers):
        """
        Filter subscribers based on their 'created_at' date.

        Args:
            subscribers (list): List of subscriber dictionaries.

        Returns:
            list: Filtered list of subscribers.
        """
        filtered_subscribers = []
        for subscriber in subscribers:
            created_at = subscriber.get('created_at')
            if created_at:
                filtered_subscribers.append({
                    "id": subscriber.get('id'),
                    "created_at": subscriber.get('created_at'),
                    "name": subscriber.get('first_name'),
                    "email": subscriber.get('email_address'),
                    "status": subscriber.get('state'),
                    "location_state": subscriber.get("location_state"),
                    "location_country": subscriber.get("location_country")
                })
        print(f"Total fetched: {len(filtered_subscribers)}")
        return filtered_subscribers
