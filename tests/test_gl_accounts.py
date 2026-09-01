from techmart.reference.gl_accounts import GL_ACCOUNTS

_REQUIRED = {
    "4000", "4100", "4200", "5000", "5100", "5200", "5300",
    "6000", "6100", "6200", "6300", "6400", "6500", "1400",
}


def test_required_accounts_present():
    nums = {a["account_number"] for a in GL_ACCOUNTS}
    assert _REQUIRED <= nums


def test_unique_account_numbers():
    nums = [a["account_number"] for a in GL_ACCOUNTS]
    assert len(nums) == len(set(nums))
    assert len(GL_ACCOUNTS) >= 40


def test_contra_flags_and_enums():
    by_num = {a["account_number"]: a for a in GL_ACCOUNTS}
    assert by_num["4100"]["is_contra"] and by_num["4200"]["is_contra"]
    assert by_num["4000"]["is_contra"] is False
    for a in GL_ACCOUNTS:
        assert a["account_type"] in {"Revenue", "COGS", "Opex", "Asset"}
        assert a["statement"] in {"P&L", "Balance-Sheet"}
        assert a["normal_balance"] in {"Debit", "Credit"}
        assert isinstance(a["is_contra"], bool)


def test_asset_on_balance_sheet():
    by_num = {a["account_number"]: a for a in GL_ACCOUNTS}
    assert by_num["1400"]["account_type"] == "Asset"
    assert by_num["1400"]["statement"] == "Balance-Sheet"
