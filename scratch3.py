import re

with open("admin_panel.py", "r") as f:
    text = f.read()

# Remove panel_hash usage dynamically:
text = text.replace("{html.escape(panel_hash)}/", "")
text = text.replace("{html.escape(panel_hash)}", "")
text = text.replace("/{html.escape(panel_hash)}/", "/")
text = text.replace("panel_hash=panel_hash", "")
text = text.replace(", panel_hash: str", "")
text = text.replace("panel_hash: str", "")

# Fix _safe_next_path and _build_login_redirect
text = text.replace("def _safe_next_path(raw_next: str, panel_hash: str) -> str:", "def _safe_next_path(raw_next: str) -> str:")
text = text.replace("def _build_login_redirect(panel_hash: str, next_path: str) -> RedirectResponse:", "def _build_login_redirect(next_path: str) -> RedirectResponse:")
text = text.replace("_safe_next_path(next_path, panel_hash)", "_safe_next_path(next_path)")
text = text.replace("safe_next = _safe_next_path(next, panel_hash)", "safe_next = _safe_next_path(next)")
text = text.replace("safe_next = _safe_next_path(next_path)", "safe_next = _safe_next_path(next_path)") # NO, just make sure there's no panel_hash arg left.
text = re.sub(r'_safe_next_path\(([^,]+)(,\s*panel_hash)?\)', r'_safe_next_path(\1)', text)
text = re.sub(r'_build_login_redirect\((panel_hash=panel_hash,\s*)?next_path=([^)]+)\)', r'_build_login_redirect(next_path=\2)', text)

# Just fix all instances of panel_hash that I missed.
text = text.replace("panel_hash: str,", "")
text = text.replace("panel_hash: str", "")
text = text.replace("panel_hash,", "")
text = text.replace(", panel_hash", "")
text = re.sub(r'panel_hash=panel_hash([^\\n]*)', r'\1', text) 
text = text.replace('f"/{panel_hash}"', 'f"/"')
text = text.replace('f"/{panel_hash}/', 'f"/')

with open("admin_panel.py", "w") as f:
    f.write(text)
