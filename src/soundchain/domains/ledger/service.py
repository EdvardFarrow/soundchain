import hashlib
from typing import List, Dict
from dataclasses import dataclass
from decimal import Decimal

from django.db import transaction
from soundchain.django_apps.ledger.models import Posting, Account

@dataclass
class TransactionDTO:
    """DTO - Data Transfer Object"""
    debit_account_id: str
    credit_account_id: str
    amount: Decimal
    reference_id: str
    description: str

class LedgerService:
    def __init__(self):
        self._last_hash_cache = None

    def _get_last_hash(self) -> str:
        if self._last_hash_cache:
            return self._last_hash_cache
            
        last_posting = Posting.objects.order_by("-created_at").first()
        return last_posting.hash if last_posting else "GENESIS_HASH"

    def _calculate_hash(self, dto: TransactionDTO, prev_hash: str) -> str:
        payload = f"{dto.debit_account_id}{dto.credit_account_id}{dto.amount}{dto.reference_id}{prev_hash}"
        return hashlib.sha256(payload.encode()).hexdigest()

    @transaction.atomic
    def process_batch(self, transactions: List[TransactionDTO]):
        """
        It takes a batch of raw data, turns it into a blockchain, and stores it with a SINGLE request.
        """
        if not transactions:
            return

        current_prev_hash = self._get_last_hash()
        
        postings_to_create = []
        
        # Dicts for aggregating balance changes (to avoid retrieving the database for each transaction)
        balance_updates: Dict[str, Decimal] = {}

        # In-Memory Processing
        for tx in transactions:
            new_hash = self._calculate_hash(tx, current_prev_hash)
            
            posting = Posting(
                debit_account_id=tx.debit_account_id,
                credit_account_id=tx.credit_account_id,
                amount=tx.amount,
                reference_id=tx.reference_id,
                description=tx.description,
                prev_hash=current_prev_hash,
                hash=new_hash
            )
            postings_to_create.append(posting)
            
            current_prev_hash = new_hash
            
            balance_updates[tx.debit_account_id] = balance_updates.get(tx.debit_account_id, Decimal(0)) - tx.amount
            balance_updates[tx.credit_account_id] = balance_updates.get(tx.credit_account_id, Decimal(0)) + tx.amount

        Posting.objects.bulk_create(postings_to_create)
        
        accounts = Account.objects.filter(id__in=balance_updates.keys()).select_for_update()
        account_map = {acc.id: acc for acc in accounts}
        
        for acc_id, delta in balance_updates.items():
            if acc_id in account_map:
                account_map[acc_id].balance += delta
                account_map[acc_id].save() 
        
        self._last_hash_cache = current_prev_hash
        
        print(f"✅ Processed batch of {len(transactions)} txs. Last hash: {current_prev_hash[:8]}...")