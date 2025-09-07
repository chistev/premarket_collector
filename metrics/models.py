from django.db import models

class MarketMetrics(models.Model):
    timestamp = models.DateTimeField()
    metric_name = models.CharField(max_length=50)
    metric_value = models.DecimalField(max_digits=20, decimal_places=6, null=True)
    data_type = models.CharField(max_length=20, default='premarket')
    source = models.CharField(max_length=50, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('timestamp', 'metric_name')
        indexes = [
            models.Index(fields=['metric_name', '-timestamp']),
        ]


class DailySnapshots(models.Model):
    date = models.DateField(primary_key=True)
    snapshot_time = models.DateTimeField()
    metrics = models.JSONField()  # ✅ works as JSONB in Postgres, text in SQLite
    is_complete = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=['-date'], name='idx_daily_date'),
        ]
