"""
GSTR PDF Converter — V3 (Streamlit)
GSTR-3B section-aware extraction and consolidation.
"""

import streamlit as st
import pandas as pd
import os
import json
import io
import sys
from pathlib import Path
from datetime import datetime

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from extractors.gstr3b_extractor import (
    extract_gstr3b_tables,
    build_consolidation_excel,
    GSTR3B_SECTIONS,
)

# ───────── Page Config ─────────
st.set_page_config(
    page_title="GSTR PDF Converter v3",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ───────── Directories ─────────
UPLOAD_DIR = Path(__file__).parent / "uploads"
EXPORT_DIR = Path(__file__).parent / "exports"
UPLOAD_DIR.mkdir(exist_ok=True)
EXPORT_DIR.mkdir(exist_ok=True)

# ───────── Session State ─────────
defaults = {
    "page": "Upload",
    "uploads": [],       # [{filename, path, size, form_type, status}]
    "extractions": [],   # [result from extract_gstr3b_tables()]
    "selected_extraction_idx": None,
    "consolidated": None,
    "engine": "pdfplumber",  # "tabula" or "pdfplumber"
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ───────── CSS ─────────
st.markdown("""
<style>
    /* Color system */
    :root {
        --primary: #4F46E5;
        --primary-light: #6366F1;
        --bg: #F8FAFC;
        --card: #FFFFFF;
        --text: #1E293B;
        --muted: #94A3B8;
    }

    /* Card containers */
    .card {
        background: var(--card);
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }

    /* Section badge */
    .section-badge {
        display: inline-block;
        background: linear-gradient(135deg, #4F46E5, #7C3AED);
        color: white;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        margin-right: 0.5rem;
    }

    /* Stat card */
    .stat-card {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
    }
    .stat-card .stat-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--text);
    }
    .stat-card .stat-label {
        font-size: 0.75rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-top: 0.25rem;
    }

    /* Meta row */
    .meta-row {
        display: flex;
        gap: 1.5rem;
        flex-wrap: wrap;
        margin: 1rem 0;
        padding: 0.75rem 1rem;
        background: #F1F5F9;
        border-radius: 8px;
        font-size: 0.85rem;
    }
    .meta-row strong {
        color: var(--primary);
    }

    /* Status indicators */
    .status-completed { color: #22c55e; }
    .status-processing { color: #eab308; }
    .status-pending { color: var(--muted); }
    .status-failed { color: #ef4444; }

    /* Footer hide */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1E293B 0%, #0F172A 100%);
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] span {
        color: #CBD5E1 !important;
    }

    /* Section tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.5rem 1rem;
        font-size: 0.85rem;
    }
</style>
""", unsafe_allow_html=True)


# ───────── Sidebar ─────────
with st.sidebar:
    st.markdown("## 📄 GSTR Utilities v3")
    st.caption("Section-aware extraction")
    st.markdown("---")

    nav_items = {
        "Upload": "📤",
        "Processing": "⚙️",
        "Preview": "📊",
        "Consolidation": "📅",
    }
    for page_name, icon in nav_items.items():
        is_current = st.session_state.page == page_name
        if st.button(
            f"{'🔴 ' if is_current else '⚫ '}{icon} {page_name}",
            key=f"nav_{page_name}",
            use_container_width=True,
        ):
            st.session_state.page = page_name
            st.rerun()

    st.markdown("---")
    st.caption(f"Uploads: {len(st.session_state.uploads)}")
    st.caption(f"Extractions: {len(st.session_state.extractions)}")


# ═══════════════════════════════════════════════
#  UPLOAD PAGE
# ═══════════════════════════════════════════════
def render_upload_page():
    st.markdown('<div class="card"><h2>📤 Upload GSTR-3B PDFs</h2>'
                '<p style="color:#94A3B8">Upload one or more GSTR-3B return PDFs for section-aware extraction</p></div>',
                unsafe_allow_html=True)

    col_form, col_engine = st.columns(2)
    with col_form:
        form_type = st.selectbox(
            "GSTR Form Type",
            ["GSTR-3B (Monthly Return)"],
            index=0,
        )
    with col_engine:
        engine = st.selectbox(
            "Extraction Engine",
            ["pdfplumber", "tabula"],
            index=0 if st.session_state.engine == "pdfplumber" else 1,
            help="pdfplumber: fast, pure Python. tabula: more accurate but needs Java & slower startup.",
        )
        st.session_state.engine = engine

    uploaded_files = st.file_uploader(
        "Choose PDF files",
        type=["pdf"],
        accept_multiple_files=True,
        key="upload_widget",
    )

    if uploaded_files:
        st.markdown(f"**{len(uploaded_files)} file(s) selected:**")
        for uf in uploaded_files:
            size_kb = uf.size / 1024
            st.markdown(f"- `{uf.name}` ({size_kb:.1f} KB)")

    if st.button(f"📤 Upload {len(uploaded_files or [])} File(s)", type="primary",
                 disabled=not uploaded_files):
        for uf in uploaded_files:
            file_path = UPLOAD_DIR / uf.name
            file_path.write_bytes(uf.getvalue())

            st.session_state.uploads.append({
                "filename": uf.name,
                "path": str(file_path),
                "size": uf.size,
                "form_type": form_type,
                "status": "pending",
                "upload_time": datetime.now().isoformat(),
            })

        st.success(f"✅ Uploaded {len(uploaded_files)} file(s)")
        st.session_state.page = "Processing"
        st.rerun()


# ═══════════════════════════════════════════════
#  PROCESSING PAGE
# ═══════════════════════════════════════════════
def render_processing_page():
    st.markdown('<div class="card"><h2>⚙️ Processing GSTR-3B PDFs</h2>'
                '<p style="color:#94A3B8">Extract tables section by section</p></div>',
                unsafe_allow_html=True)

    pending = [u for u in st.session_state.uploads if u["status"] == "pending"]
    completed = [u for u in st.session_state.uploads if u["status"] == "completed"]
    failed = [u for u in st.session_state.uploads if u["status"] == "failed"]

    if not st.session_state.uploads:
        st.info("No files to process. Go to **Upload** to add files.")
        if st.button("📤 Back to Upload"):
            st.session_state.page = "Upload"
            st.rerun()
        return

    # File list
    st.markdown("### Extraction Progress")
    for upload in st.session_state.uploads:
        status = upload["status"]
        icon = {"pending": "⏳", "processing": "🔄", "completed": "✅", "failed": "❌"}
        st.markdown(f"{icon.get(status, '❓')} **{upload['filename']}** — `{status}`")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Pending", len(pending))
    with col2:
        st.metric("Completed", len(completed))
    with col3:
        st.metric("Failed", len(failed))

    # Start extraction
    if pending:
        if st.button("🚀 Start Extraction", type="primary"):
            progress = st.progress(0)
            status_text = st.empty()

            for i, upload in enumerate(st.session_state.uploads):
                if upload["status"] != "pending":
                    continue

                upload["status"] = "processing"
                status_text.text(f"Processing {upload['filename']}...")
                progress.progress((i + 1) / len(st.session_state.uploads))

                try:
                    use_tabula = st.session_state.engine == "tabula"
                    result = extract_gstr3b_tables(upload["path"], use_tabula=use_tabula)
                    result["filename"] = upload["filename"]
                    result["upload_idx"] = i
                    st.session_state.extractions.append(result)
                    upload["status"] = "completed"

                    meta = result.get("metadata", {})
                    sections_found = list(result.get("sections", {}).keys())
                    total_rows = sum(
                        s.get("row_count", 0)
                        for s in result.get("sections", {}).values()
                    )
                    st.success(
                        f"✅ **{upload['filename']}**: "
                        f"GSTIN `{meta.get('gstin', 'N/A')}`, "
                        f"Period: {meta.get('period', '?')} {meta.get('year', '')}, "
                        f"Sections: {', '.join(sections_found) or 'None'}, "
                        f"{total_rows} total rows"
                    )
                except Exception as e:
                    upload["status"] = "failed"
                    st.error(f"❌ **{upload['filename']}**: {str(e)}")

            progress.progress(1.0)
            status_text.text("Done!")
            st.rerun()

    # After extraction
    if completed:
        st.markdown("---")
        colA, colB = st.columns(2)
        with colA:
            if st.button("📊 View Extracted Tables"):
                st.session_state.page = "Preview"
                st.rerun()
        with colB:
            if st.button("📅 Go to Consolidation"):
                st.session_state.page = "Consolidation"
                st.rerun()


# ═══════════════════════════════════════════════
#  PREVIEW PAGE
# ═══════════════════════════════════════════════
def render_preview_page():
    st.markdown('<div class="card"><h2>📊 Preview Extracted Tables</h2>'
                '<p style="color:#94A3B8">View tables organized by GSTR-3B section</p></div>',
                unsafe_allow_html=True)

    if not st.session_state.extractions:
        st.info("No extractions available. Go to **Processing** first.")
        if st.button("⚙️ Go to Processing"):
            st.session_state.page = "Processing"
            st.rerun()
        return

    # Select extraction
    ext_options = [
        f"{ext['filename']} — {ext.get('metadata', {}).get('period', '?')} "
        f"{ext.get('metadata', {}).get('year', '')}"
        for ext in st.session_state.extractions
    ]
    selected_idx = st.selectbox(
        "Select extraction",
        range(len(ext_options)),
        format_func=lambda i: ext_options[i],
    )

    ext = st.session_state.extractions[selected_idx]
    meta = ext.get("metadata", {})
    sections = ext.get("sections", {})
    unclassified = ext.get("unclassified", [])

    # Metadata bar
    st.markdown(
        f'<div class="meta-row">'
        f'<span><strong>GSTIN:</strong> {meta.get("gstin", "N/A")}</span>'
        f'<span><strong>Period:</strong> {meta.get("period", "N/A")} {meta.get("year", "")}</span>'
        f'<span><strong>ARN Date:</strong> {meta.get("arn_date", "N/A")}</span>'
        f'<span><strong>Pages:</strong> {ext.get("total_pages", "?")}</span>'
        f'<span><strong>Sections:</strong> {len(sections)}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Section tabs
    all_sections = []
    for sid in ["3.1", "3.1.1", "3.1.2", "4", "5", "5.1", "6"]:
        if sid in sections:
            all_sections.append((sid, sections[sid]))
    for ut in unclassified:
        all_sections.append(("other", ut))

    if not all_sections:
        st.warning("No tables were extracted from this PDF.")
        return

    tab_names = [
        f"§{sid}" if sid != "other" else sec.get("name", "Other")
        for sid, sec in all_sections
    ]
    tabs = st.tabs(tab_names)

    for tab, (section_id, section_data) in zip(tabs, all_sections):
        with tab:
            title = section_data.get("title", section_data.get("name", "Table"))
            row_count = section_data.get("row_count", len(section_data.get("rows", [])))
            st.markdown(
                f'<span class="section-badge">§{section_id}</span> '
                f'**{title}** — {row_count} rows',
                unsafe_allow_html=True,
            )

            rows = section_data.get("rows", [])
            if rows:
                df = pd.DataFrame(rows)

                # Deduplicate column names
                seen = {}
                new_cols = []
                for col in df.columns:
                    if col in seen:
                        seen[col] += 1
                        new_cols.append(f"{col}_{seen[col]}")
                    else:
                        seen[col] = 0
                        new_cols.append(col)
                df.columns = new_cols

                try:
                    st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception:
                    st.table(df)
            else:
                st.info("No rows extracted for this section.")

    # Export single extraction
    st.markdown("---")
    st.markdown("### Export This Extraction")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("⬇️ Export as Excel"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                for sid in ["3.1", "3.1.1", "3.1.2", "4", "5", "5.1", "6"]:
                    if sid in sections:
                        rows = sections[sid].get("rows", [])
                        if rows:
                            df = pd.DataFrame(rows)
                            df.to_excel(writer, sheet_name=sid, index=False)
                for ut in unclassified:
                    rows = ut.get("rows", [])
                    if rows:
                        df = pd.DataFrame(rows)
                        df.to_excel(writer, sheet_name=ut.get("name", "Other")[:31], index=False)
            output.seek(0)
            st.download_button(
                "📥 Download Excel",
                output.getvalue(),
                file_name=f"{ext['filename'].replace('.pdf', '')}_extracted.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    with col2:
        if st.button("⬇️ Export as CSV (Zip)"):
            import zipfile
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zf:
                for sid in ["3.1", "3.1.1", "3.1.2", "4", "5", "5.1", "6"]:
                    if sid in sections:
                        rows = sections[sid].get("rows", [])
                        if rows:
                            df = pd.DataFrame(rows)
                            csv_data = df.to_csv(index=False)
                            zf.writestr(f"section_{sid.replace('.', '_')}.csv", csv_data)
            zip_buffer.seek(0)
            st.download_button(
                "📥 Download CSV Zip",
                zip_buffer.getvalue(),
                file_name=f"{ext['filename'].replace('.pdf', '')}_extracted.zip",
                mime="application/zip",
            )


# ═══════════════════════════════════════════════
#  CONSOLIDATION PAGE
# ═══════════════════════════════════════════════
def render_consolidation_page():
    st.markdown('<div class="card"><h2>📅 Yearly Consolidation</h2>'
                '<p style="color:#94A3B8">Consolidate monthly GSTR-3B extractions into a single report</p></div>',
                unsafe_allow_html=True)

    if not st.session_state.extractions:
        st.info("No extractions available. Process some PDFs first.")
        if st.button("⚙️ Go to Processing"):
            st.session_state.page = "Processing"
            st.rerun()
        return

    # Summary cards
    total_extractions = len(st.session_state.extractions)
    total_sections = sum(
        len(ext.get("sections", {})) for ext in st.session_state.extractions
    )
    total_rows = sum(
        sum(s.get("row_count", 0) for s in ext.get("sections", {}).values())
        for ext in st.session_state.extractions
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            '<div class="stat-card">'
            f'<div class="stat-value">{total_extractions}</div>'
            '<div class="stat-label">PDFs Extracted</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<div class="stat-card">'
            f'<div class="stat-value">{total_sections}</div>'
            '<div class="stat-label">Total Sections</div>'
            '</div>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<div class="stat-card">'
            f'<div class="stat-value">{total_rows}</div>'
            '<div class="stat-label">Total Rows</div>'
            '</div>',
            unsafe_allow_html=True,
        )

    # List of extractions included
    st.markdown("### Extractions to Consolidate")
    for i, ext in enumerate(st.session_state.extractions):
        meta = ext.get("metadata", {})
        sections = ext.get("sections", {})
        sections_str = ", ".join(f"§{s}" for s in sections.keys())
        st.markdown(
            f"**{i+1}. {ext.get('filename', 'Unknown')}** — "
            f"GSTIN: `{meta.get('gstin', 'N/A')}`, "
            f"Period: {meta.get('period', '?')} {meta.get('year', '')}, "
            f"Sections: {sections_str}"
        )

    st.markdown("---")

    # Consolidation button
    if st.button("📊 Create Consolidated Report", type="primary"):
        with st.spinner("Building consolidated Excel..."):
            output_path = str(EXPORT_DIR / "GST_consolidated.xlsx")
            try:
                build_consolidation_excel(
                    st.session_state.extractions,
                    output_path,
                )
                st.session_state.consolidated = output_path
                st.success("✅ Consolidated report created!")
            except Exception as e:
                st.error(f"❌ Consolidation failed: {str(e)}")

    # Download consolidated file
    if st.session_state.consolidated and os.path.exists(st.session_state.consolidated):
        st.markdown("### Download Report")

        with open(st.session_state.consolidated, "rb") as f:
            excel_data = f.read()

        st.download_button(
            "📥 Download Consolidated Excel",
            excel_data,
            file_name="GST_consolidated.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

        # Preview the consolidated data
        st.markdown("### Preview")
        try:
            xls = pd.ExcelFile(st.session_state.consolidated)
            preview_tabs = st.tabs(xls.sheet_names)
            for ptab, sheet_name in zip(preview_tabs, xls.sheet_names):
                with ptab:
                    df = pd.read_excel(xls, sheet_name)
                    df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
                    st.markdown(f"**{sheet_name}** — {len(df)} rows")
                    try:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    except Exception:
                        st.table(df)
        except Exception as e:
            st.warning(f"Could not preview: {e}")


# ═══════════════════════════════════════════════
#  ROUTER
# ═══════════════════════════════════════════════
page = st.session_state.page
if page == "Upload":
    render_upload_page()
elif page == "Processing":
    render_processing_page()
elif page == "Preview":
    render_preview_page()
elif page == "Consolidation":
    render_consolidation_page()
