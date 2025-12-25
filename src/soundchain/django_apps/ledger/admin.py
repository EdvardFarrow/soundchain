from django.contrib import admin
from django.utils.html import format_html
from .models import Account, Posting


@admin.register(Account)
class AccountAdmin(admin.ModelAdmin):
    list_display = ('name_short', 'balance_formatted', 'id', 'created_at')
    search_fields = ('name', 'id')
    readonly_fields = ('id', 'created_at')
    ordering = ('-created_at',)

    def name_short(self, obj):
        return obj.name[:30] + '...' if len(obj.name) > 30 else obj.name
    name_short.short_description = 'Name'

    def balance_formatted(self, obj):
        color = 'green' if obj.balance > 0 else 'black'
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            f"{obj.balance:,.4f}"
        )
    balance_formatted.short_description = 'Balance'


@admin.register(Posting)
class PostingAdmin(admin.ModelAdmin):
    list_display = ('created_at', 'reference_id', 'amount', 'debit_link', 'credit_link', 'is_valid')
    list_filter = ('created_at',)
    search_fields = ('reference_id', 'debit_account__id', 'credit_account__id', 'hash')
    
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False
    
    # def has_delete_permission(self, request, obj=None):
    #     return False

    def debit_link(self, obj):
        return format_html('<a href="/admin/ledger/account/{}/change/">{}</a>', obj.debit_account.id, obj.debit_account.name)
    debit_link.short_description = 'From (Debit)'

    def credit_link(self, obj):
        return format_html('<a href="/admin/ledger/account/{}/change/">{}</a>', obj.credit_account.id, obj.credit_account.name)
    credit_link.short_description = 'To (Credit)'

    def is_valid(self, obj):
        return bool(obj.hash and obj.prev_hash)
    is_valid.boolean = True