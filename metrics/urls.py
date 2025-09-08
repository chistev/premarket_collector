from django.urls import path
from .views import (
    CollectNQCloseView,
    GetNQDataView,
    CollectVIXLevelView,
    GetVIXDataView,
    CollectTreasuryYieldView,
    GetTreasuryYieldDataView,
    CollectOvernightGapView,
    GetOvernightGapDataView,
    CollectPutCallRatioView,
    GetPutCallRatioDataView,
)

app_name = 'metrics'

urlpatterns = [
    path('collect-nq-close/', CollectNQCloseView.as_view(), name='collect_nq_close'),
    path('get-nq-data/', GetNQDataView.as_view(), name='get_nq_data'),
    path('collect-vix-level/', CollectVIXLevelView.as_view(), name='collect_vix_level'),
    path('get-vix-data/', GetVIXDataView.as_view(), name='get_vix_data'),
    path('collect-treasury-yield/', CollectTreasuryYieldView.as_view(), name='collect_treasury_yield'),
    path('get-treasury-yield-data/', GetTreasuryYieldDataView.as_view(), name='get_treasury_yield_data'),
    path('collect-overnight-gaps/', CollectOvernightGapView.as_view(), name='collect_overnight_gaps'),
    path('get-overnight-gaps/', GetOvernightGapDataView.as_view(), name='get_overnight_gaps'),
    path('collect-put-call-ratio/', CollectPutCallRatioView.as_view(), name='collect_put_call_ratio'),
    path('get-put-call-ratio/', GetPutCallRatioDataView.as_view(), name='get_put_call_ratio'),
]