from django.urls import path
from .views import CollectNQCloseView

urlpatterns = [
    path('collect-nq-close/', CollectNQCloseView.as_view(), name='collect_nq_close'),
]