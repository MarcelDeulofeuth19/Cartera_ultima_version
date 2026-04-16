with open("admin_panel.py", "r") as f:
    text = f.read()

text = text.replace(", panel_hash", "")
text = text.replace("panel_hash=", "panel_hash='', ") # or remove it
text = text.replace("panel_hash: str", "")

with open("admin_panel.py", "w") as f:
    f.write(text)
