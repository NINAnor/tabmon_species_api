"""Keyboard shortcuts for Streamlit form widgets."""

import json

import streamlit.components.v1 as components


def add_form_shortcuts(**shortcuts):
    """Bind keyboard shortcuts to checkbox and radio widgets.

    The listener is registered once per browser session and its mapping
    is replaced on every rerun, so removed widget keys cannot shadow the
    current form's shortcuts.

    Args:
        **shortcuts: widget key -> hotkey, list of hotkeys, or radio option
            descriptors. For example, species_0_4="1" and confidence_4=[
            {"shortcut": "q", "option": "Low"}, ...].
    """
    if not shortcuts:
        raise ValueError("At least one shortcut binding is required")

    js = (
        """<script>
    const doc = window.parent.document;
    const parentWindow = window.parent.window;
    const shortcuts = """
        + json.dumps(shortcuts)
        + """;

    if (!doc) throw new Error('Not running in Streamlit context');

    const shortcutsVersion = 2;
    if (parentWindow.__formShortcutsInitialized &&
        parentWindow.__formShortcutsVersion !== shortcutsVersion) {
        doc.removeEventListener(
            'keydown', parentWindow.__formShortcutsListener
        );
        parentWindow.__formShortcutsInitialized = false;
    }

    if (!parentWindow.__formShortcutsInitialized) {
        parentWindow.__formShortcutsMap = {};
        parentWindow.__formShortcutsListener = function(e) {
            if (e.target && e.target.closest && e.target.closest(
                'input:not([type="checkbox"]):not([type="radio"]), ' +
                'select, textarea, [contenteditable="true"]'
            )) {
                return;
            }
            const allShortcuts = parentWindow.__formShortcutsMap || {};
            for (const [key, shortcutDefinitions] of Object.entries(allShortcuts)) {
                const definitions = Array.isArray(shortcutDefinitions)
                    ? shortcutDefinitions
                    : [shortcutDefinitions];
                for (const definition of definitions) {
                    const shortcut = typeof definition === 'string'
                        ? definition
                        : definition.shortcut;
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

                        const root = doc.querySelector(`.st-key-${CSS.escape(key)}`);
                        if (!root) {
                            continue;
                        }

                        let el;
                        if (definition.option !== undefined) {
                            const labels = Array.from(root.querySelectorAll('label'));
                            el = labels.find(label => {
                                const radio = label.querySelector(
                                    'input[type="radio"]'
                                );
                                return (
                                    radio &&
                                    label.innerText.trim() === definition.option
                                );
                            });
                            if (!el) {
                                const options = Array.from(
                                    root.querySelectorAll('[role="radio"]')
                                );
                                el = options.find(option => {
                                    const label = (
                                        option.getAttribute('aria-label') ||
                                        option.innerText ||
                                        option.textContent || ''
                                    ).trim();
                                    return label === definition.option;
                                });
                            }
                        } else {
                            el = root.querySelector('input[type="checkbox"]') ||
                                 root.querySelector('[role="checkbox"]');
                        }

                        if (!el) {
                            continue;
                        }
                        e.preventDefault();
                        el.click();
                        if (el.focus) el.focus();
                        return;
                    }
                }
            }
        };
        doc.addEventListener('keydown', parentWindow.__formShortcutsListener);
        parentWindow.__formShortcutsInitialized = true;
        parentWindow.__formShortcutsVersion = shortcutsVersion;
    }

    parentWindow.__formShortcutsMap = shortcuts;
    </script>"""
    )

    components.html(js, height=0, width=0)
