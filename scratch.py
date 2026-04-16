import re

with open("admin_panel.py", "r") as f:
    content = f.read()

# 1. Routes
content = re.sub(r'\"/\{panel_hash\}/', r'"/', content)
content = re.sub(r'\"/\{panel_hash\}\"', r'"/"', content)

# 2. Endpoint params. E.g., `def panel_logout(panel_hash: str) -> RedirectResponse:`
content = re.sub(r',\s*panel_hash:\s*str', r'', content)
content = re.sub(r'panel_hash:\s*str,\s*', r'', content)
content = re.sub(r'\(panel_hash:\s*str\)', r'()', content)

# 3. f"/\{panel_hash\}"
content = re.sub(r'f\"/\{panel_hash\}/', r'"/', content)
content = re.sub(r'f\"/\{panel_hash\}\"', r'"/"', content)

# 4. _assert_hash function calls
content = re.sub(r'_assert_hash\(panel_hash\)', r'pass # _assert_hash removed', content)

# 5. Functions taking panel_hash
content = re.sub(r'def _render_mora_rotation_report_html\(panel_hash: str,', r'def _render_mora_rotation_report_html(', content)
content = re.sub(r'def _render_dashboard_html\(panel_hash: str,', r'def _render_dashboard_html(', content)
content = re.sub(r'def _render_admin_html\(\n\s*\*,', r'def _render_admin_html(\n    *,', content) # just formatting
content = content.replace('def _render_admin_html(\n    *,\n    panel_hash: str,', 'def _render_admin_html(\n    *,')
content = content.replace('panel_hash=panel_hash,', '')
content = content.replace('panel_hash: str,', '')
content = content.replace('panel_hash=panel_hash', '')

with open("admin_panel.py", "w") as f:
    f.write(content)
