import pandas as pd

from reconcilers.gstr1_vs_3b import g1_mapped, period_key, reconcile


GSTIN = "29ABCDE1234F1Z5"


def _g1(period="April", year="2024-25", gstin=GSTIN, sections=None):
    return {
        "metadata": {"gstin": gstin, "period": period, "year": year},
        "sections": sections or {
            "4A": {
                "rows": [{
                    "Description": "Total",
                    "Value (₹)": "1,000,000.00",
                    "Integrated Tax (₹)": "0.00",
                    "Central Tax (₹)": "90,000.00",
                    "State/UT Tax (₹)": "90,000.00",
                    "Cess (₹)": "0.00",
                }]
            }
        },
        "form_type": "GSTR-1 (Outward Supplies)",
        "filename": "gstr1.pdf",
    }


def _g3b(period="April", year="2024-25", gstin=GSTIN):
    return {
        "metadata": {"gstin": gstin, "period": period, "year": year},
        "sections": {
            "3.1": {
                "rows": [{
                    "Nature of Supplies": "(a) Outward taxable supplies (other than zero rated, nil rated and exempted)",
                    "Total Taxable value": "1,000,000.00",
                    "Integrated Tax": "0.00",
                    "Central Tax": "90,000.00",
                    "State/UT Tax": "90,000.00",
                    "Cess": "0.00",
                }]
            }
        },
        "form_type": "GSTR-3B (Monthly Return)",
        "filename": "gstr3b.pdf",
    }


def test_perfect_match():
    dataframe, issues = reconcile([_g1()], [_g3b()])

    assert not issues
    assert not dataframe.empty
    assert set(dataframe["Status"]) == {"Match"}


def test_one_head_mismatch():
    g3b = _g3b()
    g3b["sections"]["3.1"]["rows"][0]["Central Tax"] = "89,000.00"

    dataframe, _ = reconcile([_g1()], [g3b])

    mismatch = dataframe[dataframe["Head"] == "CGST"]
    assert mismatch.iloc[0]["Difference"] == 1000.0
    assert mismatch.iloc[0]["Status"] == "3B lower than GSTR-1"


def test_credit_note_is_subtracted():
    credit_note = {
        "9B-CDNR": {
            "rows": [{
                "Description": "Credit Note",
                "Document Type": "Credit Note",
                "Value (₹)": "100.00",
                "Integrated Tax (₹)": "10.00",
                "Central Tax (₹)": "5.00",
                "State/UT Tax (₹)": "5.00",
                "Cess (₹)": "0.00",
            }]
        }
    }

    mapped = g1_mapped(_g1(sections=credit_note))

    assert mapped["3.1(a)"]["taxable"] == -100.0
    assert mapped["3.1(a)"]["igst"] == -10.0


def test_missing_counterpart_period_is_reported():
    dataframe, issues = reconcile(
        [_g1(period="April")],
        [_g3b(period="May")],
    )

    assert isinstance(dataframe, pd.DataFrame)
    assert dataframe.empty
    assert any("GSTR-1 uploaded but no GSTR-3B" in issue for issue in issues)
    assert any("GSTR-3B uploaded but no GSTR-1" in issue for issue in issues)


def test_gstin_mismatch_is_reported():
    dataframe, issues = reconcile(
        [_g1()],
        [_g3b(gstin="27ABCDE1234F1Z5")],
    )

    assert not dataframe.empty
    assert any("GSTIN mismatch" in issue for issue in issues)


def test_calendar_year_and_financial_year_pair_for_same_month():
    assert period_key({"period": "January", "year": "2025"}) == period_key(
        {"period": "January", "year": "2024-25"}
    )
