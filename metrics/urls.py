from django.urls import path
from .views import CollectNQCloseView, CollectTreasuryYieldView, GetNQDataView, CollectVIXLevelView, GetTreasuryYieldDataView, GetVIXDataView

urlpatterns = [
    path('collect-nq-close/', CollectNQCloseView.as_view(), name='collect_nq_close'),
    path('get-nq-data/', GetNQDataView.as_view(), name='get_nq_data'),
    path('collect-vix-level/', CollectVIXLevelView.as_view(), name='collect_vix_level'),
    path('get-vix-data/', GetVIXDataView.as_view(), name='get_vix_data'),
    path('collect-treasury-yield/', CollectTreasuryYieldView.as_view(), name='collect-treasury-yield'),
    path('get-treasury-yield-data/', GetTreasuryYieldDataView.as_view(), name='get-treasury-yield-data'),
]