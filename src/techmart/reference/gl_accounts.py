"""Techmart chart of accounts (engine-agnostic reference data).

The fact derivation (fact_gl_actuals / fact_inventory_valuation) references the
account_number business keys directly, so the derivation-target accounts must
keep their exact numbers. Extra accounts add realism (not every account carries
activity every period) and are intentionally left unpopulated by the facts.
"""
from __future__ import annotations


def _a(number, name, atype, statement, section, category, normal, contra=False):
    return {
        "account_number": number,
        "account_name": name,
        "account_type": atype,
        "statement": statement,
        "statement_section": section,
        "account_category": category,
        "normal_balance": normal,
        "is_contra": contra,
    }


GL_ACCOUNTS: list[dict] = [
    # --- Revenue (P&L) ---
    _a("4000", "Gross Product Sales", "Revenue", "P&L", "Net Sales", "Product Sales", "Credit"),
    _a("4010", "Service & Warranty Revenue", "Revenue", "P&L", "Net Sales", "Service Revenue", "Credit"),
    _a("4020", "Shipping Revenue", "Revenue", "P&L", "Net Sales", "Other Revenue", "Credit"),
    _a("4100", "Sales Returns", "Revenue", "P&L", "Net Sales", "Contra Revenue", "Debit", contra=True),
    _a("4200", "Sales Allowances", "Revenue", "P&L", "Net Sales", "Contra Revenue", "Debit", contra=True),
    # --- Cost of goods sold (P&L) ---
    _a("5000", "Product COGS", "COGS", "P&L", "Cost of Goods Sold", "Merchandise Cost", "Debit"),
    _a("5100", "Freight-In", "COGS", "P&L", "Cost of Goods Sold", "Inbound Freight", "Debit"),
    _a("5200", "Markdowns", "COGS", "P&L", "Cost of Goods Sold", "Markdowns", "Debit"),
    _a("5300", "Inventory Shrink", "COGS", "P&L", "Cost of Goods Sold", "Shrink", "Debit"),
    _a("5400", "Vendor Allowances", "COGS", "P&L", "Cost of Goods Sold", "Vendor Funding", "Credit", contra=True),
    # --- Operating expense (P&L) ---
    _a("6000", "Store Payroll", "Opex", "P&L", "Operating Expenses", "Payroll", "Debit"),
    _a("6010", "Store Benefits", "Opex", "P&L", "Operating Expenses", "Payroll", "Debit"),
    _a("6100", "Occupancy", "Opex", "P&L", "Operating Expenses", "Occupancy", "Debit"),
    _a("6110", "Utilities", "Opex", "P&L", "Operating Expenses", "Occupancy", "Debit"),
    _a("6200", "Marketing", "Opex", "P&L", "Operating Expenses", "Marketing", "Debit"),
    _a("6210", "Digital Advertising", "Opex", "P&L", "Operating Expenses", "Marketing", "Debit"),
    _a("6300", "Supply-Chain Opex", "Opex", "P&L", "Operating Expenses", "Supply Chain", "Debit"),
    _a("6310", "Distribution Center Costs", "Opex", "P&L", "Operating Expenses", "Supply Chain", "Debit"),
    _a("6400", "General & Administrative", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6410", "IT & Systems", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6420", "Professional Fees", "Opex", "P&L", "Operating Expenses", "G&A", "Debit"),
    _a("6500", "Depreciation", "Opex", "P&L", "Operating Expenses", "Depreciation", "Debit"),
    _a("6510", "Amortization", "Opex", "P&L", "Operating Expenses", "Depreciation", "Debit"),
    _a("6600", "Credit Card Fees", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6610", "Insurance", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6700", "Bad Debt Expense", "Opex", "P&L", "Operating Expenses", "Other Opex", "Debit"),
    _a("6800", "Interest Expense", "Opex", "P&L", "Operating Expenses", "Interest", "Debit"),
    _a("6900", "Income Tax Expense", "Opex", "P&L", "Operating Expenses", "Taxes", "Debit"),
    # --- Assets (Balance-Sheet) ---
    _a("1000", "Cash & Equivalents", "Asset", "Balance-Sheet", "Current Assets", "Cash", "Debit"),
    _a("1100", "Accounts Receivable", "Asset", "Balance-Sheet", "Current Assets", "Receivables", "Debit"),
    _a("1400", "Merchandise Inventory", "Asset", "Balance-Sheet", "Current Assets", "Inventory", "Debit"),
    _a("1410", "Inventory Reserve", "Asset", "Balance-Sheet", "Current Assets", "Inventory", "Credit", contra=True),
    _a("1500", "Prepaid Expenses", "Asset", "Balance-Sheet", "Current Assets", "Prepaids", "Debit"),
    _a("1600", "Property & Equipment", "Asset", "Balance-Sheet", "Non-Current Assets", "PP&E", "Debit"),
    _a("1610", "Accumulated Depreciation", "Asset", "Balance-Sheet", "Non-Current Assets", "PP&E", "Credit", contra=True),
    _a("1700", "Right-of-Use Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Leases", "Debit"),
    _a("1800", "Goodwill", "Asset", "Balance-Sheet", "Non-Current Assets", "Intangibles", "Debit"),
    _a("1810", "Intangible Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Intangibles", "Debit"),
    _a("1900", "Other Assets", "Asset", "Balance-Sheet", "Non-Current Assets", "Other", "Debit"),
    _a("1920", "Deferred Tax Asset", "Asset", "Balance-Sheet", "Non-Current Assets", "Other", "Debit"),
]
