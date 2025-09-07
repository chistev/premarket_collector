import yfinance as yf
from django.http import JsonResponse
from django.views import View
from .models import MarketMetrics
import logging
import pandas as pd
from django.utils import timezone

# Set up logging
logger = logging.getLogger(__name__)

class CollectNQCloseView(View):
    def get(self, request):
        try:
            # Define the date range for January 2024
            start_date = "2024-01-01"
            end_date = "2024-01-31"

            # Try primary ticker: ^NDXFUTER (Nasdaq 100 Futures Index)
            tickers = ["^NDXFUTER", "NQ=F"]  # Fallback to NQ=F if needed
            data = None

            for ticker in tickers:
                try:
                    nq_futures = yf.Ticker(ticker)
                    data = nq_futures.history(start=start_date, end=end_date, interval="1d")
                    if not data.empty:
                        logger.info(f"Successfully retrieved data for {ticker}")
                        break
                except Exception as e:
                    logger.warning(f"Failed to retrieve data for {ticker}: {str(e)}")
                    continue

            if data is None or data.empty:
                logger.error("No data retrieved from Yahoo Finance for any ticker")
                return JsonResponse({"status": "error", "message": "No data retrieved from Yahoo Finance"}, status=500)

            # Process each day's closing price
            for date, row in data.iterrows():
                # Handle timezone conversion
                timestamp = pd.Timestamp(date)
                if timestamp.tzinfo is None:
                    # If naive, localize to UTC (yfinance default) then convert to US/Eastern
                    timestamp = timestamp.tz_localize("UTC").tz_convert("US/Eastern")
                else:
                    # If already timezone-aware, convert to US/Eastern
                    timestamp = timestamp.tz_convert("US/Eastern")

                # Extract the adjusted close price
                close_price = float(row["Close"])

                # Store in MarketMetrics model
                MarketMetrics.objects.update_or_create(
                    timestamp=timestamp,
                    metric_name="nq_close",
                    defaults={
                        "metric_value": close_price,
                        "data_type": "premarket",
                        "source": f"Yahoo Finance ({ticker})",
                        "created_at": timezone.now(),
                    }
                )
                logger.info(f"Stored NQ close price {close_price} for {timestamp.date()}")

            return JsonResponse({
                "status": "success",
                "message": f"Collected NQ closing prices for {len(data)} days in January 2024"
            })

        except Exception as e:
            logger.error(f"Error collecting NQ closing prices: {str(e)}")
            return JsonResponse({"status": "error", "message": str(e)}, status=500)