---
title: GSTR3B
emoji: 📊
colorFrom: blue
colorTo: green
sdk: docker
sdk_version: "3.12"
app_file: Dockerfile
pinned: false
short_description: GSTR-3B PDF to Excel Converter with GSTR-1 Reconciliation
---

# GSTR-3B PDF to Excel Converter

## Reconcile GSTR-1 vs GSTR-3B

After processing returns, open **Perform Reco** from the sidebar to compare
GSTR-1 and GSTR-3B returns by filing period. The screen reports differences in
taxable value, IGST, CGST, SGST/UTGST, and cess, with an editable rupee
tolerance and an Excel download.

The reconciliation currently covers GSTR-1 outward-supply sections against
GSTR-3B Table 3.1. It does not reconcile GSTR-3B Table 3.1.1, Table 4 ITC,
GSTR-2B, or books. Review the extracted-values panel against the source PDFs
before acting on a difference because PDF table extraction can vary by portal
format.