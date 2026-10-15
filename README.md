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

[![Live Demo](https://img.shields.io/badge/Live%20Demo-Open%20App-00D9A5?style=for-the-badge&logo=streamlit&logoColor=white)](https://skilledsite.onrender.com)
[![Render Status](https://img.shields.io/endpoint?url=https://render-status-badge.vercel.app/api/status/skilledsite&style=for-the-badge)](https://dashboard.render.com/web/skilledsite)
[![GitHub Actions](https://img.shields.io/github/actions/workflow/status/wolfrec007/gstr3b/keep-alive.yml?branch=main&label=Keep%20Alive&style=for-the-badge&logo=githubactions)](https://github.com/wolfrec007/gstr3b/actions/workflows/keep-alive.yml)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.57-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io)
[![Reco Feature](https://img.shields.io/badge/Reco-GSTR--1%20vs%203B-FF6B35?style=for-the-badge&logo=databricks&logoColor=white)](https://skilledsite.onrender.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

---

## 🚀 Live Demo

**Try it now:** [https://skilledsite.onrender.com](https://skilledsite.onrender.com)

> **Note:** Free tier spins down after 15 min inactivity. First load may take 30-60s. A GitHub Actions cron job pings every 10 min to keep it warm.

---

## 📋 Features

| Feature | Description |
|---------|-------------|
| **GSTR-3B Extraction** | Section-aware table extraction (3.1, 3.1.1, 3.1.2, 4, 5, 5.1, 6, Breakup) |
| **GSTR-1 Extraction** | All sections (4A–15, amendments, B2B, B2CL, exports, SEZ, deemed exports) |
| **Dual Engine** | `pdfplumber` (fast, pure Python) or `tabula-py` (accurate, needs Java) |
| **Yearly Consolidation** | Merge monthly returns into single Excel with month-wise sheets |
| **🔍 GSTR-1 vs 3B Reconciliation (Reco)** | **NEW: Compare outward supplies (GSTR-1) vs Table 3.1 (GSTR-3B) by filing period — configurable rupee tolerance, side-by-side diff view, Excel download** |
| **Multi-format Export** | Excel (multi-sheet), CSV (zipped), per-file or consolidated |

---

### 🔍 Reconciliation (Reco) — Key Capabilities

| Capability | Details |
|------------|---------|
| **Match by Period** | Auto-pairs GSTR-1 & GSTR-3B extractions for same GSTIN + filing period |
| **Compare Sections** | GSTR-1 outward supply sections (4A, 4B, 5, 6A–6C, 7, 8) vs GSTR-3B Table 3.1 |
| **Metrics Compared** | Taxable Value, IGST, CGST, SGST/UTGST, Cess |
| **Tolerance** | Configurable rupee threshold (default ₹1) — mark as "Match" or "Mismatch" |
| **Output** | Side-by-side comparison table + summary + downloadable Excel report |
| **Access** | Via **Perform Reco** page in sidebar (after processing both form types) |

---

## 🎯 Quick Start

### Option 1: Use Live Demo (Easiest)
[**Open App →**](https://gstr3b-converter.onrender.com) — No installation needed.

### Option 2: Run Locally
```bash
# Clone
git clone https://github.com/wolfrec007/gstr3b.git
cd gstr3b

# Install system deps (Java for tabula)
# Ubuntu/Debian: sudo apt install default-jdk
# macOS: brew install openjdk
# Windows: Download from oracle.com/java

# Python deps
pip install -r requirements.txt

# Run
streamlit run app.py
```

### Option 3: Docker
```bash
docker build -t gstr3b .
docker run -p 7860:7860 gstr3b
# Open http://localhost:7860
```

---

## 📖 Usage Guide

### 1. Upload PDFs
- Go to **Upload** page
- Select form type: **GSTR-3B** or **GSTR-1**
- Choose engine: `pdfplumber` (default) or `tabula`
- Upload one or more PDFs → Click **Upload**

### 2. Process
- Go to **Processing** page
- Click **Start Extraction**
- Wait for completion (green checkmarks)

### 3. Preview & Export
- **Preview** page: View tables by section, export single file as Excel/CSV
- **Consolidation** page: Merge multiple months → Download consolidated Excel

### 4. Reconcile (GSTR-1 vs 3B) — **Reco Feature**
- **Prerequisite:** Process at least one GSTR-1 and one GSTR-3B PDF for the **same GSTIN + filing period**
- Go to **Perform Reco** page (sidebar)
- Select matching period pairs from the dropdown
- Adjust **tolerance** (₹) if needed — rows within tolerance = "Match"
- Review **side-by-side comparison**: GSTR-1 value vs GSTR-3B value vs Difference
- Click **Download Excel Report** for detailed diff with summary sheet
- **Covers:** GSTR-1 sections 4A, 4B, 5, 6A, 6B, 6C, 7, 8 vs GSTR-3B Table 3.1

---

## 🏗️ Architecture

```
gstr3b/
├── app.py                    # Main Streamlit app (entry point)
├── requirements.txt          # Python dependencies
├── packages.txt              # System packages (default-jdk)
├── Dockerfile                # Container definition for Render/HF Spaces
├── render.yaml               # Render Blueprint config
├── .streamlit/config.toml    # Streamlit server config
├── .github/workflows/
│   └── keep-alive.yml        # Cron job to prevent spin-down
├── extractors/
│   ├── gstr3b_extractor.py   # GSTR-3B PDF parsing logic
│   └── gstr1_extractor.py    # GSTR-1 PDF parsing logic
├── reconcilers/
│   └── gstr1_vs_3b.py        # Reconciliation engine
├── screens/
│   └── reconcile_screen.py   # Reconciliation UI
└── exports/                  # Generated Excel files (ephemeral)
```

---

## ⚙️ Configuration

### Extraction Engine
| Engine | Pros | Cons | Best For |
|--------|------|------|----------|
| `pdfplumber` | Fast, no Java, pure Python | Less accurate on complex tables | Quick processing, GSTR-1 |
| `tabula-py` | Better table detection | Needs Java, slower startup | Complex GSTR-3B tables |

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `7860` | Server port (Render injects this) |
| `STREAMLIT_SERVER_HEADLESS` | `true` | Run without browser |
| `STREAMLIT_SERVER_ENABLE_CORS` | `false` | Disable CORS for embedding |

---

## 🔧 Deployment

### Render (Current — Free Tier + Cron)
- **Blueprint:** `render.yaml` → One-click deploy
- **Cron:** GitHub Actions pings every 10 min
- **URL:** `https://skilledsite.onrender.com`

### Hugging Face Spaces (Requires PRO)
```bash
hf repos create <user>/gstr3b --type space --space-sdk docker
hf upload <user>/gstr3b . --repo-type space
```

### Streamlit Community Cloud (Free, Sleeps)
1. Fork this repo
2. Go to share.streamlit.io → New App → Select fork
3. Add `default-jdk` in Advanced Settings → Apt Packages

### Other Platforms
- **Railway:** $5/mo, always-on, native Docker
- **Fly.io:** Free allowance, always-on
- **Google Cloud Run:** Pay-per-use, scales to zero

---

## 🤝 Contributing

1. Fork the repo
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push: `git push origin feature/amazing-feature`
5. Open a Pull Request

---

## 📝 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [pdfplumber](https://github.com/jsvine/pdfplumber) — PDF table extraction
- [tabula-py](https://github.com/chezou/tabula-py) — Java-based table extraction
- [Streamlit](https://streamlit.io) — Web app framework
- [Render](https://render.com) — Free hosting with Docker support

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/wolfrec007/gstr3b/issues)
- **Discussions:** [GitHub Discussions](https://github.com/wolfrec007/gstr3b/discussions)
- **Live Demo:** [skilledsite.onrender.com](https://skilledsite.onrender.com)

---

<div align="center">
  <strong>Made with ❤️ for Indian GST Compliance</strong><br>
  <sub>GSTR-3B • GSTR-1 • Reconciliation • PDF → Excel</sub>
</div>