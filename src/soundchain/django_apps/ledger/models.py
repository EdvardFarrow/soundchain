import uuid
from decimal import Decimal
from django.db import models

class Account(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    balance = models.DecimalField(max_digits=20, decimal_places=4, default=Decimal("0.0000"))
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return f"{self.name} ({self.balance})"

class Posting(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    debit_account = models.ForeignKey(Account, related_name="debits", on_delete=models.PROTECT)
    credit_account = models.ForeignKey(Account, related_name="credits", on_delete=models.PROTECT)
    
    amount = models.DecimalField(max_digits=20, decimal_places=4)
    
    reference_id = models.CharField(max_length=100, unique=True)
    description = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    # Hash Chaining
    prev_hash = models.CharField(max_length=64, blank=True, null=True)
    hash = models.CharField(max_length=64, blank=True, null=True)

    class Meta:
        ordering = ["created_at"]
        indexes = [
            models.Index(fields=["reference_id"]),
            models.Index(fields=["created_at"]),
        ]