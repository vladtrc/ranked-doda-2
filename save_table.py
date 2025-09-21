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
    blocks = []
    for s in sections:
        q = s.sql or f'SELECT * FROM "{s.table}"'
        df = conn.sql(f"{q} LIMIT {limit}").df()

        # Minimal table with luxury spacing
        tbl = df.to_html(
            index=False,
            border=0,
            classes=(
                "w-full text-sm font-light tracking-wide "
                "border-separate border-spacing-0"
            ),
        )

        # Clean up default styles
        tbl = tbl.replace(' style="text-align: center;"', '')

        # Luxury header styling - uppercase, letter-spacing
        tbl = tbl.replace(
            "<th>",
            '<th class="px-6 py-4 text-[10px] font-medium uppercase tracking-[0.2em] '
            'text-amber-200/60 text-left border-b border-amber-200/10">'
        )
        tbl = tbl.replace(
            "<th",
            '<th class="px-6 py-4 text-[10px] font-medium uppercase tracking-[0.2em] '
            'text-amber-200/60 text-left border-b border-amber-200/10"'
        )

        # Luxury cell styling - generous padding, subtle borders
        tbl = tbl.replace(
            "<td>",
            '<td class="px-6 py-4 text-gray-300 font-light '
            'border-b border-white/[0.02] transition-colors duration-300">'
        )
        tbl = tbl.replace(
            "<td",
            '<td class="px-6 py-4 text-gray-300 font-light '
            'border-b border-white/[0.02] transition-colors duration-300"'
        )

        # Remove tbody/thead tags for cleaner structure
        tbl = tbl.replace("<thead>", '<thead class="backdrop-blur-sm">')
        tbl = tbl.replace(
            "<tbody>",
            '<tbody class="[&>tr:hover]:bg-white/[0.02] [&>tr]:transition-all [&>tr]:duration-500">'
        )

        note_html = (
            f'<p class="text-[11px] text-amber-200/40 mt-3 font-light tracking-wide uppercase">'
            f'{escape(s.note)}</p>'
            if s.note else ""
        )

        blocks.append(f"""
<section class="group">
  <div class="mb-8">
    <h2 class="text-lg font-extralight text-white tracking-wider mb-1">
      {escape(s.title)}
    </h2>
    <div class="h-px bg-gradient-to-r from-amber-200/20 via-amber-200/5 to-transparent w-32"></div>
    {note_html}
  </div>
  <div class="overflow-x-auto scrollbar-thin scrollbar-thumb-amber-200/10 scrollbar-track-transparent">
    <div class="min-w-full backdrop-blur-sm bg-white/[0.01] rounded-sm">
      {tbl}
    </div>
  </div>
</section>""")

    page_title = escape(title or "Report")

    # Luxury HTML with custom fonts and minimal dark theme
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
      background: linear-gradient(180deg, #0a0a0a 0%, #111111 100%);
      min-height: 100vh;
    }}
    /* Subtle glow effect on hover */
    tr:hover td {{
      text-shadow: 0 0 20px rgba(251, 191, 36, 0.1);
    }}
    /* Custom scrollbar */
    ::-webkit-scrollbar {{
      height: 6px;
      width: 6px;
    }}
    ::-webkit-scrollbar-track {{
      background: transparent;
    }}
    ::-webkit-scrollbar-thumb {{
      background: rgba(251, 191, 36, 0.1);
      border-radius: 3px;
    }}
    ::-webkit-scrollbar-thumb:hover {{
      background: rgba(251, 191, 36, 0.2);
    }}
  </style>
</head>
<body class="text-gray-200 antialiased">
  <!-- Luxury header with accent -->
  <header class="px-8 lg:px-12 py-12 border-b border-white/[0.05]">
    <div class="max-w-7xl mx-auto">
      <div class="flex items-center gap-3">
        <div class="w-1 h-8 bg-gradient-to-b from-amber-200/60 to-amber-200/10"></div>
        <h1 class="text-2xl font-extralight tracking-wider text-white">
          {page_title}
        </h1>
      </div>
      <p class="text-[10px] uppercase tracking-[0.3em] text-amber-200/40 mt-2 ml-4">
        Data Intelligence Report
      </p>
    </div>
  </header>
  
  <!-- Main content with luxury spacing -->
  <main class="px-8 lg:px-12 py-12">
    <div class="max-w-7xl mx-auto space-y-16">
      {''.join(blocks)}
    </div>
  </main>
  
  <!-- Minimal footer -->
  <footer class="px-8 lg:px-12 py-8 mt-20 border-t border-white/[0.02]">
    <div class="max-w-7xl mx-auto flex justify-between items-center">
      <p class="text-[10px] uppercase tracking-[0.3em] text-amber-200/30">
        Generated from DuckDB
      </p>
      <div class="flex gap-1">
        <div class="w-1 h-1 rounded-full bg-amber-200/20"></div>
        <div class="w-1 h-1 rounded-full bg-amber-200/30"></div>
        <div class="w-1 h-1 rounded-full bg-amber-200/20"></div>
      </div>
    </div>
  </footer>
</body>
</html>"""

    Path(save_as).write_text(page, encoding="utf-8")