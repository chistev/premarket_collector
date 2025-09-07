from django.conf import settings
import requests
import yfinance as yf
from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from .models import MarketMetrics
import logging
import pandas as pd
from django.utils import timezone
from pytz import timezone as pytz_timezone
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseMarketDataView(View):
    """Base class for market data views"""
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    eastern_tz = pytz_timezone('US/Eastern')
    expected_trading_days = 22  # Jan 1 (New Year's) and Jan 15 (MLK Day) are holidays

    @method_decorator(csrf_exempt)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def convert_timestamp(self, timestamp):
        """Convert timestamp to Django-compatible timezone-aware datetime"""
        timestamp = pd.Timestamp(timestamp)
        
        if timestamp.tzinfo:
            eastern_time = timestamp.tz_convert(self.eastern_tz)
            naive_datetime = eastern_time.replace(tzinfo=None)
        else:
            utc_time = timestamp.tz_localize("UTC")
            eastern_time = utc_time.tz_convert(self.eastern_tz)
            naive_datetime = eastern_time.replace(tzinfo=None)
            
        return timezone.make_aware(naive_datetime, self.eastern_tz)

    def store_metric(self, timestamp, metric_name, metric_value, source, data_type="eod"):
        """Store market metric in database"""
        try:
            MarketMetrics.objects.update_or_create(
                timestamp=timestamp,
                metric_name=metric_name,
                defaults={
                    "metric_value": float(metric_value),
                    "data_type": data_type,
                    "source": source,
                }
            )
            logger.info(f"Stored {metric_name} {metric_value} for {timestamp.date()}")
            return True
        except Exception as e:
            logger.error(f"Error storing {metric_name} for {timestamp.date()}: {str(e)}")
            return False

    def format_response(self, status, message, details=None):
        """Format JSON response"""
        response = {"status": status, "message": message}
        if details:
            response["details"] = details
        return JsonResponse(response, status=500 if status == "error" else 200)

class CollectNQCloseView(BaseMarketDataView):
    def get(self, request):
        try:
            logger.info(f"Collecting NQ closing prices from {self.start_date} to {self.end_date}")
            tickers = ["NQ=F", "^NDX", "QQQ"]
            data = None
            successful_ticker = None

            for ticker in tickers:
                try:
                    logger.info(f"Trying ticker: {ticker}")
                    instrument = yf.Ticker(ticker)
                    data = instrument.history(start=self.start_date, end=self.end_date, interval="1d")
                    
                    if not data.empty:
                        successful_ticker = ticker
                        logger.info(f"Successfully retrieved {len(data)} records for {ticker}")
                        break
                except Exception as e:
                    logger.warning(f"Failed to retrieve data for {ticker}: {str(e)}")
                    continue

            if data is None or data.empty:
                return self.format_response(
                    "error",
                    "No data retrieved from Yahoo Finance for any ticker symbol"
                )

            successful_inserts = 0
            for date, row in data.iterrows():
                if self.store_metric(
                    timestamp=self.convert_timestamp(date),
                    metric_name="nq_close",
                    metric_value=row["Close"],
                    source=f"Yahoo Finance ({successful_ticker})"
                ):
                    successful_inserts += 1

            return self.format_response(
                "success",
                f"Collected NQ closing prices for {successful_inserts} days in January 2024",
                {
                    "ticker_used": successful_ticker,
                    "records_processed": successful_inserts,
                    "expected_trading_days": self.expected_trading_days,
                    "source": "Yahoo Finance"
                }
            )

        except Exception as e:
            logger.error(f"Error collecting NQ closing prices: {str(e)}")
            return self.format_response("error", str(e))

class GetNQDataView(BaseMarketDataView):
    def get(self, request):
        try:
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            nq_data = MarketMetrics.objects.filter(
                metric_name='nq_close',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            results = [{
                "date": entry.timestamp.strftime("%Y-%m-%d"),
                "timestamp": entry.timestamp.isoformat(),
                "close_price": float(entry.metric_value),
                "source": entry.source
            } for entry in nq_data]
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })

        except Exception as e:
            logger.error(f"Error retrieving NQ data: {str(e)}")
            return self.format_response("error", str(e))

class CollectVIXLevelView(BaseMarketDataView):
    def get(self, request):
        try:
            logger.info(f"Collecting VIX levels from {self.start_date} to {self.end_date}")
            ticker = "^VIX"
            instrument = yf.Ticker(ticker)
            data = instrument.history(start=self.start_date, end=self.end_date, interval="1d")
            
            if data.empty:
                return self.format_response("error", f"No data retrieved for {ticker}")

            successful_inserts = 0
            for date, row in data.iterrows():
                if self.store_metric(
                    timestamp=self.convert_timestamp(date),
                    metric_name="vix_level",
                    metric_value=row["Close"],
                    source=f"Yahoo Finance ({ticker})"
                ):
                    successful_inserts += 1

            return self.format_response(
                "success",
                f"Collected VIX levels for {successful_inserts} days in January 2024",
                {
                    "ticker_used": ticker,
                    "records_processed": successful_inserts,
                    "expected_trading_days": self.expected_trading_days,
                    "source": "Yahoo Finance"
                }
            )

        except Exception as e:
            logger.error(f"Error collecting VIX levels: {str(e)}")
            return self.format_response("error", str(e))

class GetVIXDataView(BaseMarketDataView):
    def get(self, request):
        try:
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            vix_data = MarketMetrics.objects.filter(
                metric_name='vix_level',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            results = [{
                "date": entry.timestamp.strftime("%Y-%m-%d"),
                "timestamp": entry.timestamp.isoformat(),
                "vix_level": float(entry.metric_value),
                "source": entry.source
            } for entry in vix_data]
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })

        except Exception as e:
            logger.error(f"Error retrieving VIX data: {str(e)}")
            return self.format_response("error", str(e))

class CollectTreasuryYieldView(BaseMarketDataView):
    def get(self, request):
        try:
            logger.info(f"Collecting 10-Year Treasury Yield from {self.start_date} to {self.end_date}")
            
            api_key = settings.FRED_API_KEY
            if not api_key:
                return self.format_response(
                    "error",
                    "FRED API key not found. Please set FRED_API_KEY environment variable."
                )

            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=DGS10"
                f"&api_key={api_key}"
                f"&file_type=json"
                f"&observation_start={self.start_date}"
                f"&observation_end={self.end_date}"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if 'observations' not in data or not data['observations']:
                return self.format_response(
                    "error",
                    "No data retrieved for 10-Year Treasury Yield from FRED"
                )

            successful_inserts = 0
            for observation in data['observations']:
                treasury_yield = observation['value']
                if treasury_yield == '.' or treasury_yield is None:
                    logger.warning(f"Skipping invalid yield value for {observation['date']}")
                    continue

                if self.store_metric(
                    timestamp=self.convert_timestamp(observation['date']),
                    metric_name="treasury_10y_yield",
                    metric_value=treasury_yield,
                    source="FRED (DGS10)"
                ):
                    successful_inserts += 1

            return self.format_response(
                "success",
                f"Collected 10-Year Treasury Yield for {successful_inserts} days in January 2024",
                {
                    "series_id": "DGS10",
                    "records_processed": successful_inserts,
                    "expected_trading_days": self.expected_trading_days,
                    "source": "FRED"
                }
            )

        except requests.RequestException as e:
            logger.error(f"Failed to retrieve 10-Year Treasury Yield data: {str(e)}")
            return self.format_response("error", f"Failed to retrieve 10-Year Treasury Yield data: {str(e)}")
        except Exception as e:
            logger.error(f"Error collecting 10-Year Treasury Yield: {str(e)}")
            return self.format_response("error", str(e))

class GetTreasuryYieldDataView(BaseMarketDataView):
    def get(self, request):
        try:
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            treasury_data = MarketMetrics.objects.filter(
                metric_name='treasury_10y_yield',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            results = [{
                "date": entry.timestamp.strftime("%Y-%m-%d"),
                "timestamp": entry.timestamp.isoformat(),
                "treasury_10y_yield": float(entry.metric_value),
                "source": entry.source
            } for entry in treasury_data]
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })

        except Exception as e:
            logger.error(f"Error retrieving 10-Year Treasury Yield data: {str(e)}")
            return self.format_response("error", str(e))

# ---------------------------
# NEW: Overnight Gap Views
# ---------------------------

class CollectOvernightGapView(BaseMarketDataView):
    def get(self, request):
        try:
            logger.info(f"Collecting Overnight Gaps for NQ from {self.start_date} to {self.end_date}")
            
            ticker = "NQ=F"
            instrument = yf.Ticker(ticker)
            data = instrument.history(start=self.start_date, end=self.end_date, interval="1d")
            
            if data.empty:
                return self.format_response("error", f"No data retrieved for {ticker}")

            successful_inserts = 0
            prev_close = None

            for date, row in data.iterrows():
                if prev_close is not None:
                    # Gap = today's open – yesterday's close
                    gap_points = row["Open"] - prev_close
                    gap_percent = (gap_points / prev_close) * 100 if prev_close != 0 else None

                    ts = self.convert_timestamp(date)

                    ok1 = self.store_metric(
                        timestamp=ts,
                        metric_name="overnight_gap_points",
                        metric_value=gap_points,
                        source=f"Yahoo Finance ({ticker})"
                    )
                    ok2 = self.store_metric(
                        timestamp=ts,
                        metric_name="overnight_gap_percent",
                        metric_value=gap_percent,
                        source=f"Yahoo Finance ({ticker})"
                    )

                    if ok1 and ok2:
                        successful_inserts += 1

                prev_close = row["Close"]

            return self.format_response(
                "success",
                f"Collected Overnight Gaps for {successful_inserts} days in January 2024",
                {
                    "ticker_used": ticker,
                    "records_processed": successful_inserts,
                    "expected_trading_days": self.expected_trading_days,
                    "source": "Yahoo Finance"
                }
            )

        except Exception as e:
            logger.error(f"Error collecting Overnight Gaps: {str(e)}")
            return self.format_response("error", str(e))

class GetOvernightGapDataView(BaseMarketDataView):
    def get(self, request):
        try:
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)

            gap_data = MarketMetrics.objects.filter(
                metric_name__in=['overnight_gap_points', 'overnight_gap_percent'],
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')

            results = {}
            for entry in gap_data:
                date_str = entry.timestamp.strftime("%Y-%m-%d")
                if date_str not in results:
                    results[date_str] = {"date": date_str, "timestamp": entry.timestamp.isoformat()}
                if entry.metric_name == "overnight_gap_points":
                    results[date_str]["gap_points"] = float(entry.metric_value)
                elif entry.metric_name == "overnight_gap_percent":
                    results[date_str]["gap_percent"] = float(entry.metric_value)

            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": list(results.values())
            })

        except Exception as e:
            logger.error(f"Error retrieving Overnight Gap data: {str(e)}")
            return self.format_response("error", str(e))
