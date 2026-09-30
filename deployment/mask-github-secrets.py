#!/usr/bin/env python3
"""Register fetched SSM values with Actions redaction before commands can log them.

This emits workflow masking commands only, never uploadable secret artifacts.
Raw values and common serialized forms cover Docker/Kubernetes error messages.
"""
import base64
import json
import os
import re
from pathlib import Path
import sys


def mask(value):
    if not value:
        return
    forms = {value, json.dumps(value)[1:-1], base64.b64encode(value.encode()).decode()}
    for form in sorted(forms):
        escaped = form.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print("::add-mask::" + escaped, flush=True)


if os.environ.get("GITHUB_ACTIONS") != "true":
    sys.exit("This helper may only run inside GitHub Actions (its output contains masking commands).")

for filename in sys.argv[1:]:
    for line in Path(filename).read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            # Mask secrets without hiding short, ordinary settings such as prod, 0, or true.
            sensitive = re.search(r"PASSWORD|SECRET|TOKEN|KEY|CRED|AUTH|DSN|CONNECTION_STRING|CERT", key, re.I)
            credential_url = re.search(r"://[^/]*@", value)
            if not sensitive and not credential_url:
                continue
            mask(value)
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                mask(value[1:-1])
