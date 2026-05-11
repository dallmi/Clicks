"""
SharePoint URL normalisation.

SharePoint exposes the same logical document under multiple URL shapes.
This module collapses them to one canonical form so AppInsights
(`CP_Link_address`) and Splunk (`ObjectId`) can be compared.

Canonical form:
    https://<host-lower>/<path-decoded-without-shortlink-prefix>

Transformations applied (in order):
  1. Strip query string and fragment.
  2. Lower-case the host.
  3. Unwrap Office Online short links: /:b:/r/<path>, /:w:/r/<path>,
     /:x:/r/<path>, /:p:/r/<path>, etc. -> /<path>
     Also handles /:b:/s/, /:w:/g/, ... (sharing variants).
  4. Unwrap `_layouts/15/Doc.aspx?sourcedoc=...&file=<name>` by replacing
     the path with the resolved file path when extractable; otherwise
     leave the doc.aspx path so the URL stays comparable.
  5. Percent-decode the path repeatedly until stable (handles double
     encoding such as %2520 -> %20 -> space).
  6. Trim trailing slash.

The output is intentionally NOT a strict RFC 3986 URI — spaces remain
literal so the result is human-readable and matches what Splunk's
`ObjectId` field shows.
"""
from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit, parse_qs

_SHORTLINK_RE = re.compile(r"^/:[a-z]:/[a-z]/", re.IGNORECASE)
_DOC_ASPX_RE = re.compile(r"/_layouts/15/Doc\.aspx$", re.IGNORECASE)


def _full_unquote(s: str) -> str:
    prev = None
    cur = s
    # Cap at 5 iterations to avoid pathological loops on malformed input.
    for _ in range(5):
        if cur == prev:
            break
        prev = cur
        cur = unquote(cur)
    return cur


def normalize(url: str | None) -> str | None:
    if url is None:
        return None
    url = url.strip()
    if not url:
        return None

    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        # Relative or non-URL — just decode and return.
        return _full_unquote(url).rstrip("/") or None

    host = parts.netloc.lower()
    path = parts.path or "/"

    # Unwrap Doc.aspx?sourcedoc=...&file=<name>
    if _DOC_ASPX_RE.search(path):
        qs = parse_qs(parts.query)
        file_name = (qs.get("file") or [None])[0]
        if file_name:
            base = path[: _DOC_ASPX_RE.search(path).start()]
            base = base.rsplit("/_layouts", 1)[0]
            path = f"{base.rstrip('/')}/{file_name}"

    # Unwrap short-link prefix /:b:/r/, /:w:/r/, ...
    m = _SHORTLINK_RE.match(path)
    if m:
        path = "/" + path[m.end():]

    path = _full_unquote(path)
    path = path.rstrip("/")

    return f"{parts.scheme.lower()}://{host}{path}" if path else f"{parts.scheme.lower()}://{host}"


if __name__ == "__main__":
    import sys

    samples = sys.argv[1:] or [
        "https://UBSCloudCHE.sharepoint.com/teams/dwwm0401wmapac/Shared%20Documents/ACRM-WM%20APAC/APAC%20MGT/file.pdf",
        "https://ubscloudche.sharepoint.com/:b:/r/teams/dwwm0401wmapac/Shared%20Documents/ACRM-WM%20APAC/APAC%20MGT/file.pdf?csf=1&web=1&e=abc",
        "https://ubscloudche.sharepoint.com/teams/x/_layouts/15/Doc.aspx?sourcedoc=%7B123%7D&file=Report.docx&action=default",
        "https://ubscloudche.sharepoint.com/teams/dwwm0401wmapac/Shared%2520Documents/file.pdf",
    ]
    for s in samples:
        print(f"IN : {s}")
        print(f"OUT: {normalize(s)}")
        print()
