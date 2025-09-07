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

@method_decorator(csrf_exempt, name='dispatch')
class CollectNQCloseView(View):
    def get(self, request):
        try:
            start_date = "2024-01-01"
            end_date = "2024-01-31"
            
            logger.info(f"Collecting NQ closing prices from {start_date} to {end_date}")

            # Try multiple tickers for robustness
            tickers = ["NQ=F", "^NDX", "QQQ"]  # Futures, Index, ETF
            data = None
            successful_ticker = None

            for ticker in tickers:
                try:
                    logger.info(f"Trying ticker: {ticker}")
                    instrument = yf.Ticker(ticker)
                    data = instrument.history(start=start_date, end=end_date, interval="1d")
                    
                    if not data.empty:
                        successful_ticker = ticker
                        logger.info(f"Successfully retrieved {len(data)} records for {ticker}")
                        break
                    else:
                        logger.warning(f"Empty data for {ticker}")
                        
                except Exception as e:
                    logger.warning(f"Failed to retrieve data for {ticker}: {str(e)}")
                    continue

            if data is None or data.empty:
                logger.error("No data retrieved from Yahoo Finance for any ticker")
                return JsonResponse({
                    "status": "error", 
                    "message": "No data retrieved from Yahoo Finance for any ticker symbol"
                }, status=500)

            # Set up timezone
            eastern_tz = pytz_timezone('US/Eastern')
            
            # Count successful inserts
            successful_inserts = 0
            
            # Process each day's closing price
            for date, row in data.iterrows():
                try:
                    # Handle timezone conversion - FIXED
                    timestamp = pd.Timestamp(date)
                    
                    # Convert to timezone-naive datetime in US/Eastern time
                    if timestamp.tzinfo is not None:
                        # If timezone-aware, convert to US/Eastern and make naive
                        eastern_time = timestamp.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    else:
                        # If naive, assume UTC and convert to US/Eastern
                        utc_time = timestamp.tz_localize("UTC")
                        eastern_time = utc_time.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    
                    # Create timezone-aware datetime for Django
                    django_timestamp = timezone.make_aware(naive_datetime, eastern_tz)
                    
                    # Extract the closing price
                    close_price = float(row["Close"])
                    
                    # Store in MarketMetrics model
                    MarketMetrics.objects.update_or_create(
                        timestamp=django_timestamp,
                        metric_name="nq_close",
                        defaults={
                            "metric_value": close_price,
                            "data_type": "eod",  # End of day data
                            "source": f"Yahoo Finance ({successful_ticker})",
                        }
                    )
                    
                    successful_inserts += 1
                    logger.info(f"Stored NQ close price {close_price} for {django_timestamp.date()}")
                    
                except Exception as e:
                    logger.error(f"Error processing data for {date}: {str(e)}")
                    continue

            # Expected trading days in January 2024 (excluding holidays)
            expected_trading_days = 22  # Jan 1 (NY) and Jan 15 (MLK) are holidays
            
            return JsonResponse({
                "status": "success",
                "message": f"Collected NQ closing prices for {successful_inserts} days in January 2024",
                "details": {
                    "ticker_used": successful_ticker,
                    "records_processed": successful_inserts,
                    "expected_trading_days": expected_trading_days,
                    "source": "Yahoo Finance"
                }
            })

        except Exception as e:
            logger.error(f"Error collecting NQ closing prices: {str(e)}")
            return JsonResponse({
                "status": "error", 
                "message": str(e)
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class GetNQDataView(View):
    def get(self, request):
        try:
            # Define the date range for January 2024
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            # Query NQ closing prices
            nq_data = MarketMetrics.objects.filter(
                metric_name='nq_close',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            # Format response
            results = []
            for entry in nq_data:
                results.append({
                    "date": entry.timestamp.strftime("%Y-%m-%d"),
                    "timestamp": entry.timestamp.isoformat(),
                    "close_price": float(entry.metric_value),
                    "source": entry.source
                })
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })
            
        except Exception as e:
            logger.error(f"Error retrieving NQ data: {str(e)}")
            return JsonResponse({
                "status": "error", 
                "message": str(e)
            }, status=500)
        
@method_decorator(csrf_exempt, name='dispatch')
class CollectVIXLevelView(View):
    def get(self, request):
        try:
            start_date = "2024-01-01"
            end_date = "2024-01-31"
            
            logger.info(f"Collecting VIX levels from {start_date} to {end_date}")

            # VIX ticker
            ticker = "^VIX"
            try:
                logger.info(f"Trying ticker: {ticker}")
                instrument = yf.Ticker(ticker)
                data = instrument.history(start=start_date, end=end_date, interval="1d")
                
                if data.empty:
                    logger.error(f"Empty data for {ticker}")
                    return JsonResponse({
                        "status": "error", 
                        "message": f"No data retrieved for {ticker}"
                    }, status=500)
                
                logger.info(f"Successfully retrieved {len(data)} records for {ticker}")
                
            except Exception as e:
                logger.error(f"Failed to retrieve data for {ticker}: {str(e)}")
                return JsonResponse({
                    "status": "error", 
                    "message": f"Failed to retrieve data for {ticker}: {str(e)}"
                }, status=500)

            # Set up timezone
            eastern_tz = pytz_timezone('US/Eastern')
            
            # Count successful inserts
            successful_inserts = 0
            
            # Process each day's closing price
            for date, row in data.iterrows():
                try:
                    # Handle timezone conversion
                    timestamp = pd.Timestamp(date)
                    
                    # Convert to timezone-naive datetime in US/Eastern time
                    if timestamp.tzinfo is not None:
                        eastern_time = timestamp.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    else:
                        utc_time = timestamp.tz_localize("UTC")
                        eastern_time = utc_time.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    
                    # Create timezone-aware datetime for Django
                    django_timestamp = timezone.make_aware(naive_datetime, eastern_tz)
                    
                    # Extract the VIX closing level
                    vix_level = float(row["Close"])
                    
                    # Store in MarketMetrics model
                    MarketMetrics.objects.update_or_create(
                        timestamp=django_timestamp,
                        metric_name="vix_level",
                        defaults={
                            "metric_value": vix_level,
                            "data_type": "eod",
                            "source": f"Yahoo Finance ({ticker})",
                        }
                    )
                    
                    successful_inserts += 1
                    logger.info(f"Stored VIX level {vix_level} for {django_timestamp.date()}")
                    
                except Exception as e:
                    logger.error(f"Error processing data for {date}: {str(e)}")
                    continue

            # Expected trading days in January 2024 (excluding holidays)
            expected_trading_days = 22  # Jan 1 (New Year's) and Jan 15 (MLK Day) are holidays
            
            return JsonResponse({
                "status": "success",
                "message": f"Collected VIX levels for {successful_inserts} days in January 2024",
                "details": {
                    "ticker_used": ticker,
                    "records_processed": successful_inserts,
                    "expected_trading_days": expected_trading_days,
                    "source": "Yahoo Finance"
                }
            })

        except Exception as e:
            logger.error(f"Error collecting VIX levels: {str(e)}")
            return JsonResponse({
                "status": "error", 
                "message": str(e)
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class GetVIXDataView(View):
    def get(self, request):
        try:
            # Define the date range for January 2024
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            # Query VIX levels
            vix_data = MarketMetrics.objects.filter(
                metric_name='vix_level',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            # Format response
            results = []
            for entry in vix_data:
                results.append({
                    "date": entry.timestamp.strftime("%Y-%m-%d"),
                    "timestamp": entry.timestamp.isoformat(),
                    "vix_level": float(entry.metric_value),
                    "source": entry.source
                })
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })
            
        except Exception as e:
            logger.error(f"Error retrieving VIX data: {str(e)}")
            return JsonResponse({
                "status": "error", 
                "message": str(e)
            }, status=500)
              
@method_decorator(csrf_exempt, name='dispatch')
class CollectTreasuryYieldView(View):
    def get(self, request):
        try:
            start_date = "2024-01-01"
            end_date = "2024-01-31"
            
            logger.info(f"Collecting 10-Year Treasury Yield from {start_date} to {end_date}")

            # FRED API setup
            api_key = settings.FRED_API_KEY
            if not api_key:
                logger.error("FRED API key not found in environment variables")
                return JsonResponse({
                    "status": "error",
                    "message": "FRED API key not found. Please set FRED_API_KEY environment variable."
                }, status=500)

            # FRED API endpoint for DGS10 series
            url = (
                f"https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=DGS10"
                f"&api_key={api_key}"
                f"&file_type=json"
                f"&observation_start={start_date}"
                f"&observation_end={end_date}"
            )

            # Fetch data from FRED API
            try:
                response = requests.get(url)
                response.raise_for_status()  # Raise exception for bad status codes
                data = response.json()
                
                if 'observations' not in data or not data['observations']:
                    logger.error("No data retrieved for 10-Year Treasury Yield from FRED")
                    return JsonResponse({
                        "status": "error",
                        "message": "No data retrieved for 10-Year Treasury Yield from FRED"
                    }, status=500)
                
                logger.info(f"Successfully retrieved {len(data['observations'])} records for 10-Year Treasury Yield")
                
            except requests.RequestException as e:
                logger.error(f"Failed to retrieve 10-Year Treasury Yield data: {str(e)}")
                return JsonResponse({
                    "status": "error",
                    "message": f"Failed to retrieve 10-Year Treasury Yield data: {str(e)}"
                }, status=500)

            # Set up timezone
            eastern_tz = pytz_timezone('US/Eastern')
            
            # Count successful inserts
            successful_inserts = 0
            
            # Process each day's yield
            for observation in data['observations']:
                try:
                    # Extract date and value
                    date_str = observation['date']
                    treasury_yield = observation['value']
                    
                    # Skip invalid or missing values
                    if treasury_yield == '.' or treasury_yield is None:
                        logger.warning(f"Skipping invalid yield value for {date_str}")
                        continue
                        
                    treasury_yield = float(treasury_yield)
                    
                    # Handle timezone conversion
                    timestamp = pd.Timestamp(date_str)
                    
                    # Convert to timezone-naive datetime in US/Eastern time
                    if timestamp.tzinfo is not None:
                        eastern_time = timestamp.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    else:
                        utc_time = timestamp.tz_localize("UTC")
                        eastern_time = utc_time.tz_convert(eastern_tz)
                        naive_datetime = eastern_time.replace(tzinfo=None)
                    
                    # Create timezone-aware datetime for Django
                    django_timestamp = timezone.make_aware(naive_datetime, eastern_tz)
                    
                    # Store in MarketMetrics model
                    MarketMetrics.objects.update_or_create(
                        timestamp=django_timestamp,
                        metric_name="treasury_10y_yield",
                        defaults={
                            "metric_value": treasury_yield,
                            "data_type": "eod",
                            "source": "FRED (DGS10)",
                        }
                    )
                    
                    successful_inserts += 1
                    logger.info(f"Stored 10-Year Treasury Yield {treasury_yield} for {django_timestamp.date()}")
                    
                except Exception as e:
                    logger.error(f"Error processing data for {date_str}: {str(e)}")
                    continue

            # Expected trading days in January 2024 (excluding holidays)
            expected_trading_days = 22  # Jan 1 (New Year's) and Jan 15 (MLK Day) are holidays
            
            return JsonResponse({
                "status": "success",
                "message": f"Collected 10-Year Treasury Yield for {successful_inserts} days in January 2024",
                "details": {
                    "series_id": "DGS10",
                    "records_processed": successful_inserts,
                    "expected_trading_days": expected_trading_days,
                    "source": "FRED"
                }
            })

        except Exception as e:
            logger.error(f"Error collecting 10-Year Treasury Yield: {str(e)}")
            return JsonResponse({
                "status": "error",
                "message": str(e)
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class GetTreasuryYieldDataView(View):
    def get(self, request):
        try:
            # Define the date range for January 2024
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 2, 1)
            
            # Query 10-Year Treasury Yield data
            treasury_data = MarketMetrics.objects.filter(
                metric_name='treasury_10y_yield',
                timestamp__date__gte=start_date,
                timestamp__date__lt=end_date
            ).order_by('timestamp')
            
            # Format response
            results = []
            for entry in treasury_data:
                results.append({
                    "date": entry.timestamp.strftime("%Y-%m-%d"),
                    "timestamp": entry.timestamp.isoformat(),
                    "treasury_10y_yield": float(entry.metric_value),
                    "source": entry.source
                })
            
            return JsonResponse({
                "status": "success",
                "count": len(results),
                "data": results
            })
            
        except Exception as e:
            logger.error(f"Error retrieving 10-Year Treasury Yield data: {str(e)}")
            return JsonResponse({
                "status": "error",
                "message": str(e)
            }, status=500)
        