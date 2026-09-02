import re
from techmart.dashboards.theme import PALETTE, ROLES, ui_theme

HEX = re.compile(r"^#[0-9A-Fa-f]{6}$")

def test_palette_has_six_dmc_tokens_all_valid_hex():
    assert set(PALETTE) == {"blue-dark", "blue-med", "blue-light", "terra", "pink", "violet-dark"}
    assert all(HEX.match(v) for v in PALETTE.values())
    assert PALETTE["blue-dark"] == "#47527B"

def test_roles_reference_real_tokens():
    for role, token in ROLES.items():
        assert token in PALETTE, f"role {role} -> unknown token {token}"
    # cornflower carries the primary/positive role; antique violet is text
    assert ROLES["primary"] == "blue-dark"
    assert ROLES["text"] == "violet-dark"

def test_ui_theme_shape_and_ordering():
    t = ui_theme()
    assert HEX.match(t["canvasBackgroundColor"]["light"])
    assert t["canvasBackgroundColor"]["light"] == "#FAF7F3"
    vc = t["visualizationColors"]
    assert vc[0] == PALETTE["blue-dark"]           # primary series first
    assert PALETTE["terra"] in vc and PALETTE["pink"] in vc
    assert all(HEX.match(c) for c in vc)
    assert HEX.match(t["fontColor"]["light"])
