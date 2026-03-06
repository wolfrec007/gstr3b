# GSTR Utilities — Streamlit App

Section-aware GSTR-3B PDF extraction and consolidation. Built with Python + Streamlit.

## Quick Start

```bash
cd final
pip install -r requirements.txt
streamlit run app.py
```

The app opens at **http://localhost:8501**.

## Features

| Page | What it does |
|------|-------------|
| 📤 Upload | Upload GSTR-3B PDFs, select extraction engine (pdfplumber/tabula) |
| ⚙️ Processing | Section-aware extraction — maps tables to §3.1, §4, §5, §6, etc. |
| 📊 Preview | View tables organized by GSTR-3B section with metadata |
| 📅 Consolidation | Stack monthly extractions into a consolidated Excel (one sheet per section) |

## Supported Sections

| Section | Content |
|---------|---------|
| §3.1 | Outward & Inward Supplies |
| §3.1.1 | E-Commerce Supplies |
| §3.1.2 | Inter-State Supplies |
| §4 | Eligible ITC |
| §5 | Exempt, Nil, Non-GST Supplies |
| §5.1 | Interest & Late Fee |
| §6 | Tax Payment |
| Breakup | Tax Summary per period |

## Architecture

- **No database** — all data lives in `st.session_state` (in-memory)
- **Engine selector** — choose pdfplumber (fast/pure Python) or tabula (more accurate/needs Java)
- **Section detection** — keyword-based mapping of extracted tables to GSTR-3B sections
- **Metadata extraction** — auto-detects GSTIN, Period, Year, Date of ARN from PDF text

## Dependencies

- `streamlit` — UI framework
- `pdfplumber` — PDF text + table extraction
- `tabula-py` — alternative table extraction (requires Java)
- `openpyxl` — Excel export
- `pandas` — data handling

## Roadmap

- [ ] GSTR-1 support
- [ ] GSTR-2A/2B support
