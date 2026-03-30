from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_mode_switch_stale_guard_checks_request_id_and_active_mode():
    src = _read("frontend/static/js/settings-manager.js")
    assert "return requestId === this.modelLoadRequestId && this.modeKey(this.settings.mode) === this.modeKey(requestedMode);" in src


def test_no_models_state_does_not_blindly_wipe_other_provider_selection():
    src = _read("frontend/static/js/settings-manager.js")
    assert "this.setProviderScopedSelection(settingsKey, providerKey, null);" in src
    assert "if (this.modeKey(this.settings.mode) === providerKey) {" in src
    assert "this.settings[settingsKey] = null;" in src


def test_provider_switch_aihub_to_local_preserves_local_scoped_selection_logic():
    src = _read("frontend/static/js/settings-manager.js")
    assert "const rememberedValue = this.getProviderScopedSelection(settingsKey, providerKey);" in src
    assert "const currentValue = rememberedValue || this.settings[settingsKey];" in src
    assert "this.setProviderScopedSelection(settingsKey, providerKey, selectedValue);" in src


def test_aihub_switch_uses_switch_only_forced_default_selection_path():
    src = _read("frontend/static/js/settings-manager.js")
    assert "const switchingToAihub = selectedProviderKey === 'aihub' && this.previousModeKey !== 'aihub';" in src
    assert "forceDefaultSelection: switchingToAihub" in src
    assert "if (forceDefaultSelection) {" in src
    assert "selectedValue = availableDefaultModel;" in src


def test_mode_switch_tracks_previous_provider_without_removing_stale_guard():
    src = _read("frontend/static/js/settings-manager.js")
    assert "this.previousModeKey = this.modeKey(this.settings.mode);" in src
    assert "return requestId === this.modelLoadRequestId && this.modeKey(this.settings.mode) === this.modeKey(requestedMode);" in src
