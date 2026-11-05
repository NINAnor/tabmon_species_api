"""Keyboard shortcuts for Streamlit form widgets."""

import json

import streamlit as st


def add_form_shortcuts(**shortcuts):
    """Bind single-key shortcuts to checkbox, radio, and button widgets.

    Bindings use stable widget-key PREFIXES (e.g. "species_0_" matches the
    widget keyed "species_0_7"), resolved at keypress time. The listener
    therefore keeps working after the form key changes on the next clip,
    without depending on this script being re-executed. Key presses inside
    text fields are ignored so typing is never intercepted.

    Args:
        **shortcuts: widget-key prefix -> key, or a descriptor / list of
            descriptors. For example, species_0_="1", user_confidence_=[
            {"key": "q", "option": "Low"}, ...], and expert_submit_={
            "key": " ", "action": "click"}.
    """
    js = (
        """<script>
    (function() {
    const shortcuts = """
        + json.dumps(shortcuts)
        + """;
    const shortcutsVersion = 8;

    if (window.__formShortcutsInitialized &&
        window.__formShortcutsVersion !== shortcutsVersion) {
        document.removeEventListener('keydown', window.__formShortcutsListener);
        window.__formShortcutsInitialized = false;
    }

    function findWidget(prefix) {
        const pattern = new RegExp('^st-key-' + prefix + '\\\\d+$');
        const matches = Array.from(
            document.querySelectorAll('[class*="st-key-' + prefix + '"]')
        ).filter(element =>
            Array.from(element.classList).some(name => pattern.test(name))
        );
        return matches.length ? matches[matches.length - 1] : null;
    }

    function findTarget(root, definition) {
        if (definition.action === 'click') {
            return root.querySelector('button');
        }
        if (definition.option !== undefined) {
            return Array.from(root.querySelectorAll('label')).find(
                label => label.querySelector('input[type="radio"]') &&
                    label.innerText.trim() === definition.option
            ) || null;
        }
        return root.querySelector('input[type="checkbox"]');
    }

    if (!window.__formShortcutsInitialized) {
        window.__formShortcutsMap = {};
        window.__formShortcutsListener = function(e) {
            if (e.ctrlKey || e.altKey || e.metaKey) {
                return;
            }
            if (e.target && e.target.closest && e.target.closest(
                'input:not([type="checkbox"]):not([type="radio"]), ' +
                'select, textarea, [contenteditable="true"]'
            )) {
                return;
            }
            const pressed = (e.key || '').toLowerCase();
            for (const [prefix, entry] of Object.entries(window.__formShortcutsMap)) {
                for (const binding of [].concat(entry)) {
                    const definition = typeof binding === 'string'
                        ? {key: binding}
                        : binding;
                    if (definition.key !== pressed) {
                        continue;
                    }
                    const root = findWidget(prefix);
                    const target = root && findTarget(root, definition);
                    if (!target) {
                        continue;
                    }
                    e.preventDefault();
                    target.click();
                    return;
                }
            }
        };
        document.addEventListener('keydown', window.__formShortcutsListener);
        window.__formShortcutsInitialized = true;
        window.__formShortcutsVersion = shortcutsVersion;
    }

    Object.assign(window.__formShortcutsMap, shortcuts);
    })();
    </script>"""
    )

    st.html(js, unsafe_allow_javascript=True)
