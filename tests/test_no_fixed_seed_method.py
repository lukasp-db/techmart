import re
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src" / "techmart"
_FIXED_SEED_RE = re.compile(r"""randomSeedMethod\s*=\s*['"]fixed['"]""")


def test_no_builder_uses_fixed_seed_method():
    offenders = [str(p.relative_to(_SRC)) for p in _SRC.rglob("*.py")
                 if _FIXED_SEED_RE.search(p.read_text())]
    assert offenders == [], (
        f"randomSeedMethod=\"fixed\" correlates seeded columns; use "
        f"\"hash_fieldname\". Offending files: {offenders}"
    )
