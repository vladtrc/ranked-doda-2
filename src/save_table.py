from dataclasses import dataclass
import duckdb, pandas as pd
from pathlib import Path
from html import escape

@dataclass
class ReportSection:
    title: str
    sql: str | None = None
    table: str | None = None
    note: str | None = None

def save_tables_tailwind_html(conn, sections: list[ReportSection], save_as: str,
                              title: str | None = None, limit: int = 1000):
    import re

    def _wl_badge(c: str) -> str:
        if c == 'W':
            return '<span class="inline-block px-0.5 text-emerald-400">W</span>'
        if c == 'L':
            return '<span class="inline-block px-0.5 text-rose-400">L</span>'
        return escape(c)

    # colorize "+N"/"-N"
    def _colorize_delta(s: str) -> str:
        s = '' if s is None else str(s)
        if s.startswith('+'):
            return f'<span class="text-emerald-400">{escape(s)}</span>'
        if s.startswith('-'):
            return f'<span class="text-rose-400">{escape(s)}</span>'
        return escape(s)

    # colorize embedded W/L inside "a | b | W | 2025-01-01"
    _pipe_wl = re.compile(r'(?<=\s\|\s)([WL])(?=\s\|)')

    def _colorize_text_cell(val) -> str:
        s = '' if val is None else str(val)
        if s in ('W', 'L'):
            return _wl_badge(s)
        if ' | W | ' in s or ' | L | ' in s:
            return _pipe_wl.sub(lambda m: 'W' if m.group(1) == 'W'
            else 'L',  # placeholder to keep length
                                s) \
                .replace(' | W | ', ' | ' + _wl_badge('W') + ' | ') \
                .replace(' | L | ', ' | ' + _wl_badge('L') + ' | ')
        return escape(s)

    def _colorize_wl_string(s: str) -> str:
        s = '' if s is None else str(s)
        return ''.join(_wl_badge(ch) if ch in ('W','L') else escape(ch) for ch in s)

    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        df2 = df.copy()

        # known columns
        if 'last10' in df2.columns:
            df2['last10'] = df2['last10'].map(_colorize_wl_string)
        if 'streak' in df2.columns:
            df2['streak'] = df2['streak'].map(_colorize_delta)
        if 'WL' in df2.columns:
            df2['WL'] = df2['WL'].map(_colorize_delta)
        if 'result' in df2.columns:
            df2['result'] = df2['result'].map(_colorize_text_cell)

        # generic pass for any object column with embedded W/L between pipes
        for col in df2.columns:
            if df2[col].dtype == 'object':
                series = df2[col].astype(str)
                if series.str.contains(r'\s\|\s[WL]\s\|', regex=True).any() or series.isin(['W','L']).any():
                    df2[col] = series.map(_colorize_text_cell)

        return df2

    blocks = []
    for s in sections:
        q = s.sql or f'SELECT * FROM "{s.table}"'
        df_raw = conn.sql(f"{q} LIMIT {limit}").df()
        df = _prepare_df(df_raw)

        tbl = df.to_html(
            index=False,
            border=0,
            escape=False,  # allow our spans
            classes=("text-sm font-light tracking-wide border-separate border-spacing-0"),
        )

        # Clean up default styles and remove line breaks
        tbl = tbl.replace(' style="text-align: center;"', '')

        # Remove <br> tags and unwanted whitespace/newlines in cells
        import re
        for br in ('<br>', '<br/>', '<br />'):
            tbl = tbl.replace(br, ' ')
        tbl = re.sub(r'>\s+<', '><', tbl)
        tbl = re.sub(r'(<t[dh][^>]*>)\s+', r'\1', tbl)
        tbl = re.sub(r'\s+(</t[dh]>)', r'\1', tbl)

        # Luxury header styling - uppercase, letter-spacing, compact padding
        tbl = tbl.replace(
            "<th>",
            '<th class="px-3 py-2 text-[10px] font-medium uppercase tracking-[0.2em] '
            'text-red-200/50 text-left border-b border-red-900/20 whitespace-nowrap">'
        )
        tbl = tbl.replace(
            "<th",
            '<th class="px-3 py-2 text-[10px] font-medium uppercase tracking-[0.2em] '
            'text-red-200/50 text-left border-b border-red-900/20 whitespace-nowrap"'
        )

        # Luxury cell styling - compact padding, same text size, prevent wrapping
        tbl = tbl.replace(
            "<td>",
            '<td class="px-3 py-1.5 text-gray-300 font-light '
            'border-b border-white/[0.02] transition-colors duration-300 whitespace-nowrap">'
        )
        tbl = tbl.replace(
            "<td",
            '<td class="px-3 py-1.5 text-gray-300 font-light '
            'border-b border-white/[0.02] transition-colors duration-300 whitespace-nowrap"'
        )

        # Section chrome
        tbl = tbl.replace("<thead>", '<thead class="backdrop-blur-sm">')
        tbl = tbl.replace(
            "<tbody>",
            '<tbody class="[&>tr:nth-child(odd)]:bg-red-950/30 '
            '[&>tr:nth-child(even)]:bg-black/40 '
            '[&>tr:hover]:bg-red-950/20 [&>tr]:transition-all [&>tr]:duration-500">'
        )

        note_html = (
            f'<p class="text-[11px] text-red-200/30 mt-3 font-light tracking-wide uppercase">'
            f'{escape(s.note)}</p>'
            if s.note else ""
        )

        blocks.append(f"""
<section class="group">
  <div class="mb-8">
    <h2 class="text-lg font-extralight text-white tracking-wider mb-1">
      {escape(s.title)}
    </h2>
    <div class="h-px bg-gradient-to-r from-red-900/40 via-red-950/20 to-transparent w-32"></div>
    {note_html}
  </div>
  <div class="overflow-x-auto scrollbar-thin scrollbar-thumb-amber-200/10 scrollbar-track-transparent">
    <div class="inline-block min-w-min backdrop-blur-sm bg-white/[0.01] rounded-sm">
      {tbl}
    </div>
  </div>
</section>""")

    page_title = escape(title or "Report")

    page = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{page_title}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@100;200;300;400;500&display=swap" rel="stylesheet">
  <style>
    * {{ font-family: 'Inter', sans-serif; }}
    body {{ 
      background: linear-gradient(180deg, #000000 0%, #0a0000 50%, #000000 100%);
      min-height: 100vh;
    }}
    tr:hover td {{ text-shadow: 0 0 30px rgba(127, 29, 29, 0.15); }}
    ::-webkit-scrollbar {{ height: 6px; width: 6px; }}
    ::-webkit-scrollbar-track {{ background: transparent; }}
    ::-webkit-scrollbar-thumb {{ background: rgba(127, 29, 29, 0.2); border-radius: 3px; }}
    ::-webkit-scrollbar-thumb:hover {{ background: rgba(127, 29, 29, 0.3); }}
  </style>
</head>
<body class="text-gray-200 antialiased">
  <header class="px-8 lg:px-12 py-12 border-b border-white/[0.05]">
    <div class="max-w-7xl mx-auto">
      <div class="flex items-center gap-3">
        <div class="w-1 h-8 bg-gradient-to-b from-red-800/40 to-red-950/20"></div>
        <h1 class="text-2xl font-extralight tracking-wider text-white">{page_title}</h1>
      </div>
      <p class="text-[10px] uppercase tracking-[0.3em] text-red-200/30 mt-2 ml-4">Data Intelligence Report</p>
    </div>
  </header>
  <main class="px-8 lg:px-12 py-12">
    <div class="max-w-7xl mx-auto space-y-16">
      {''.join(blocks)}
    </div>
  </main>
  <footer class="px-8 lg:px-12 py-8 mt-20 border-t border-white/[0.02]">
    <div class="max-w-7xl mx-auto flex justify-between items-center">
      <p class="text-[10px] uppercase tracking-[0.3em] text-red-200/20">Generated from DuckDB</p>
      <div class="flex gap-1">
        <div class="w-1 h-1 rounded-full bg-red-900/30"></div>
        <div class="w-1 h-1 rounded-full bg-red-900/40"></div>
        <div class="w-1 h-1 rounded-full bg-red-900/30"></div>
      </div>
    </div>
  </footer>
</body>
</html>"""

    Path(save_as).write_text(page, encoding="utf-8")
