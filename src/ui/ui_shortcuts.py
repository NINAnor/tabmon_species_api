"""Keyboard shortcut helper with input-field safety.

A guarded port of `streamlit_shortcuts.add_shortcuts` that also ignores
key events originating inside text fields. That is required when plain
keys like "1" or "q" are bound: the original library calls
`preventDefault()` on every match, which would silently swallow typed
characters. Native `st.button(shortcut=...)` is unaffected; Streamlit's
own implementation already filters those elements via hotkeys-js.
"""

import json

import streamlit.components.v1 as components


def add_form_shortcuts(**shortcuts):
    """Bind keyboard shortcuts to widgets by key, safe while typing.

    The listener is registered once per browser session and its mapping
    is updated on every rerun, so bindings may change as widgets change.

    Args:
        **shortcuts: widget key -> hotkey string or list of hotkeys.
            e.g. species_0_4="1", none_of_above_4="2".
            Keys follow streamlit-shortcuts syntax: plain letters/numbers
            or modifier combos like "ctrl+1".
    """
    assert shortcuts, "No shortcuts provided"

    normalized = {
        key: value if isinstance(value, list) else [value]
        for key, value in shortcuts.items()
    }

    js = (
        """<script>
    const doc = window.parent.document;
    const parentWindow = window.parent.window;
    const shortcuts = """
        + json.dumps(normalized)
        + """;

    if (!doc) throw new Error('Not running in Streamlit context');

    if (!parentWindow.__formShortcutsInitialized) {
        parentWindow.__formShortcutsMap = {};
        parentWindow.__formShortcutsListener = function(e) {
            const target = e.target || {};
            const tag = target.tagName || '';
            if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA' ||
                target.isContentEditable) {
                return;
            }
            const allShortcuts = parentWindow.__formShortcutsMap || {};
            for (const [key, shortcutList] of Object.entries(allShortcuts)) {
                for (const shortcut of shortcutList) {
                    const parts = shortcut.toLowerCase().split('+');
                    const hasCtrl = parts.includes('ctrl');
                    const hasAlt = parts.includes('alt');
                    const hasShift = parts.includes('shift');
                    const hasMeta = parts.includes('meta') || parts.includes('cmd');
                    const mainKey = parts.find(
                        p => !['ctrl', 'alt', 'shift', 'meta', 'cmd'].includes(p)
                    );

                    if (hasCtrl === e.ctrlKey &&
                        hasAlt === e.altKey &&
                        hasShift === e.shiftKey &&
                        hasMeta === e.metaKey &&
                        e.key.toLowerCase() === mainKey) {

                        e.preventDefault();

                        let el = doc.querySelector(`.st-key-${key} button`) ||
                                 doc.querySelector(`.st-key-${key} input`) ||
                                 doc.querySelector(`[data-testid="${key}"]`) ||
                                 doc.querySelector(
                                     `button:has([data-testid="baseButton-${key}"])`
                                 ) ||
                                 doc.querySelector(`[aria-label="${key}"]`);

                        if (!el) {
                            return;
                        }
                        el.click();
                        el.focus();
                        return;
                    }
                }
            }
        };
        doc.addEventListener('keydown', parentWindow.__formShortcutsListener);
        parentWindow.__formShortcutsInitialized = true;
    }

    Object.assign(parentWindow.__formShortcutsMap, shortcuts);
    </script>"""
    )

    components.html(js, height=0, width=0)
