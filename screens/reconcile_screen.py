"""Streamlit screen for reconciling existing GSTR-1 and GSTR-3B extractions."""

import os
import tempfile
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from reconcilers.gstr1_vs_3b import (
    _period_label,
    g1_mapped,
    g3b_table_3_1,
    period_key,
    reconcile,
    build_reco_excel,
)


def _label(extraction: Dict[str, Any]) -> str:
    metadata = extraction.get("metadata", {})
    return (
        f"{extraction.get('filename', 'Unknown file')} - "
        f"{metadata.get('period', '?')} {metadata.get('year', '')}"
    )


def _highlight(row: pd.Series) -> List[str]:
    color = "" if row.get("Status") == "Match" else "background-color: #fde2e2"
    return [color] * len(row)


def _period_maps(extractions: List[Dict[str, Any]]) -> Dict[Any, Dict[str, Dict[str, Any]]]:
    periods: Dict[Any, Dict[str, Dict[str, Any]]] = {}
    for extraction in extractions:
        key = period_key(extraction.get("metadata", {}))
        if key is None:
            continue
        form = "gstr1" if "GSTR-1" in extraction.get("form_type", "") else "gstr3b"
        periods.setdefault(key, {})[form] = extraction
    return periods


def render(extractions: List[Dict[str, Any]], app_version: str) -> None:
    st.markdown(
        '<div class="card"><h2 class="reco-title">🔎 Reconcile GSTR-1 vs GSTR-3B</h2>'
        '<p class="reco-help">Compare extracted returns for the same filing periods.</p>'
        f'<p class="reco-help"><strong>GSTR Utilities version {app_version}</strong></p></div>',
        unsafe_allow_html=True,
    )

    gstr1 = [extraction for extraction in extractions if "GSTR-1" in extraction.get("form_type", "")]
    gstr3b = [extraction for extraction in extractions if "GSTR-3B" in extraction.get("form_type", "")]
    if not gstr1 or not gstr3b:
        st.info("Process at least one GSTR-1 and one GSTR-3B return before starting reconciliation.")
        return

    options_col, control_col = st.columns(2)
    with options_col:
        tolerance = st.number_input(
            "Tolerance (₹)", min_value=0.0, value=1.0, step=1.0,
            help="Differences at or below this amount are treated as matches.",
        )
    with control_col:
        show_matching = st.checkbox("Show matching rows too", value=False)

    gstr1_index = st.selectbox(
        "GSTR-1 returns available",
        range(len(gstr1)),
        format_func=lambda index: _label(gstr1[index]),
    )
    gstr3b_index = st.selectbox(
        "GSTR-3B returns available",
        range(len(gstr3b)),
        format_func=lambda index: _label(gstr3b[index]),
    )
    st.caption("The selectors help review individual returns. Reconcile uses all processed returns by period.")

    if st.button("▶ Reconcile", type="primary"):
        with st.spinner("Reconciling extracted returns..."):
            dataframe, issues = reconcile(gstr1, gstr3b, tol=tolerance)
        st.session_state["reco_result"] = (dataframe, issues, tolerance)

    result = st.session_state.get("reco_result")
    if result is None:
        st.info("Review the selected returns, then press Reconcile.")
        return

    dataframe, issues, used_tolerance = result
    if dataframe.empty:
        st.warning("No matching GSTR-1 / GSTR-3B periods were found.")
    else:
        differences = dataframe[dataframe["Status"] != "Match"]
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        metric_col1.metric("Periods reconciled", dataframe["Period"].nunique())
        metric_col2.metric("Rows with differences", len(differences))
        metric_col3.metric(
            "Total absolute difference (₹)",
            f"{differences['Difference'].abs().sum():,.2f}",
        )

        period_options = ["All"] + sorted(dataframe["Period"].unique())
        selected_period = st.selectbox("Period", period_options)
        view = dataframe if show_matching else differences
        if selected_period != "All":
            view = view[view["Period"] == selected_period]
        st.dataframe(view.style.apply(_highlight, axis=1), use_container_width=True)

        with tempfile.TemporaryDirectory() as directory:
            output_path = os.path.join(directory, "gstr1_vs_3b_reconciliation.xlsx")
            build_reco_excel(dataframe, issues, output_path, tol=used_tolerance)
            with open(output_path, "rb") as report:
                st.download_button(
                    "📥 Download reconciliation (Excel)",
                    report.read(),
                    file_name="gstr1_vs_3b_reconciliation.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    with st.expander("Issues and warnings", expanded=bool(issues)):
        for message in issues or ["No issues found."]:
            st.write(message)

    with st.expander("Extracted values (for checking against the PDF)"):
        for key, pair in sorted(_period_maps(extractions).items()):
            st.markdown(f"**{_period_label(key)}**")
            value_col1, value_col2 = st.columns(2)
            with value_col1:
                st.markdown(
                    '<div class="reco-panel"><strong>GSTR-1 mapped values</strong></div>',
                    unsafe_allow_html=True,
                )
                if pair.get("gstr1"):
                    st.dataframe(pd.DataFrame.from_dict(g1_mapped(pair["gstr1"]), orient="index"))
                else:
                    st.info("No GSTR-1 return for this period.")
            with value_col2:
                st.markdown(
                    '<div class="reco-panel"><strong>GSTR-3B Table 3.1 values</strong></div>',
                    unsafe_allow_html=True,
                )
                if pair.get("gstr3b"):
                    st.dataframe(pd.DataFrame.from_dict(g3b_table_3_1(pair["gstr3b"]), orient="index"))
                else:
                    st.info("No GSTR-3B return for this period.")
