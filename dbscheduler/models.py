from django.db import models


class PeriodicTask(models.Model):
    name = models.CharField(max_length=200)

    last_run_at = models.DateTimeField(
        auto_now=False, auto_now_add=False,
        editable=False, blank=True, null=True,
    )
