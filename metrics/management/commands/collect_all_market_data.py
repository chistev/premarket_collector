from django.core.management.base import BaseCommand
from django.http import HttpRequest
from metrics.views import (
    CollectNQCloseView,
    CollectVIXLevelView,
    CollectTreasuryYieldView,
    CollectOvernightGapView,
    CollectPutCallRatioView,
)
import json
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Collects all market data (NQ Close, VIX, Treasury Yield, Overnight Gaps, Put/Call Ratio) and stores it in the database'

    def handle(self, *args, **options):
        # Create a minimal HttpRequest object to pass to views
        request = HttpRequest()
        request.method = 'GET'

        collectors = [
            (CollectNQCloseView(), 'NQ Closing Prices'),
            (CollectVIXLevelView(), 'VIX Levels'),
            (CollectTreasuryYieldView(), '10-Year Treasury Yield'),
            (CollectOvernightGapView(), 'Overnight Gaps'),
            (CollectPutCallRatioView(), 'Put/Call Ratio'),
        ]

        self.stdout.write(self.style.SUCCESS('Starting market data collection...'))
        success_count = 0
        total_collectors = len(collectors)

        for view_instance, data_type in collectors:
            try:
                self.stdout.write(f'Collecting {data_type}...')
                # Call the view's get method directly
                response = view_instance.get(request)

                if response.status_code == 200:
                    # Parse JsonResponse content
                    response_data = json.loads(response.content.decode('utf-8'))
                    if response_data.get('status') == 'success':
                        self.stdout.write(
                            self.style.SUCCESS(
                                f'Successfully collected {data_type}: {response_data.get("message")}'
                            )
                        )
                        success_count += 1
                    else:
                        self.stdout.write(
                            self.style.ERROR(
                                f'Failed to collect {data_type}: {response_data.get("message")}'
                            )
                        )
                        logger.error(f'Failed to collect {data_type}: {response_data.get("message")}')
                else:
                    self.stdout.write(
                        self.style.ERROR(f'Error collecting {data_type}: Status {response.status_code}')
                    )
                    logger.error(f'Error collecting {data_type}: Status {response.status_code}')

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error collecting {data_type}: {str(e)}')
                )
                logger.error(f'Error collecting {data_type}: {str(e)}')

        self.stdout.write(
            self.style.SUCCESS(
                f'\nData collection completed: {success_count}/{total_collectors} collectors succeeded'
            )
        )