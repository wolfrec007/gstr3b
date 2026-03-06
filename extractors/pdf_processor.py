import pdfplumber
import tabula
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class PDFProcessor:
    """Process PDF files and extract text and tables.
    
    V2: Uses tabula-py (lattice + stream) as primary extraction engine
    with pdfplumber as fallback for metadata and text.
    """
    
    def __init__(self):
        pass
    
    def get_page_count(self, pdf_path: str) -> int:
        """Get total number of pages in PDF"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                return len(pdf.pages)
        except Exception as e:
            logger.error(f"Error getting page count: {e}")
            return 0
    
    def extract_text(self, pdf_path: str, page_number: int = None) -> str:
        """Extract text from PDF page or entire document"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_number is not None:
                    if page_number < len(pdf.pages):
                        return pdf.pages[page_number].extract_text() or ""
                    return ""
                
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text += page_text + "\n"
                return text
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
            return ""
    
    def extract_tables_from_page(self, pdf_path: str, page_number: int) -> List[List[List[str]]]:
        """Extract tables from a specific page using tabula (1-indexed for tabula)"""
        try:
            # tabula uses 1-indexed pages
            dfs = tabula.read_pdf(
                pdf_path,
                pages=str(page_number + 1),
                multiple_tables=True,
                lattice=True,  # Try lattice mode first (bordered tables)
                silent=True,
            )
            
            # If lattice found nothing, try stream mode
            if not dfs or all(df.empty for df in dfs):
                dfs = tabula.read_pdf(
                    pdf_path,
                    pages=str(page_number + 1),
                    multiple_tables=True,
                    stream=True,
                    silent=True,
                )
            
            # Convert DataFrames to list-of-lists format (matching pdfplumber output)
            tables = []
            for df in dfs:
                if df.empty:
                    continue
                # Build table: headers as first row, then data rows
                table = [list(df.columns)]
                for _, row in df.iterrows():
                    table.append([str(v) if pd.notna(v) else "" for v in row.values])
                tables.append(table)
            
            return tables
        except Exception as e:
            logger.warning(f"Tabula failed on page {page_number}, falling back to pdfplumber: {e}")
            return self._pdfplumber_extract_page(pdf_path, page_number)
    
    def _pdfplumber_extract_page(self, pdf_path: str, page_number: int) -> List[List[List[str]]]:
        """Fallback: extract tables using pdfplumber"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_number >= len(pdf.pages):
                    return []
                page = pdf.pages[page_number]
                tables = page.extract_tables()
                return tables or []
        except Exception as e:
            logger.error(f"pdfplumber fallback also failed on page {page_number}: {e}")
            return []
    
    def extract_all_tables(self, pdf_path: str) -> Dict[int, List[List[List[str]]]]:
        """Extract all tables from all pages using tabula"""
        try:
            page_count = self.get_page_count(pdf_path)
            if page_count == 0:
                return {}
            
            # Try tabula lattice mode on all pages at once
            dfs = tabula.read_pdf(
                pdf_path,
                pages="all",
                multiple_tables=True,
                lattice=True,
                silent=True,
            )
            
            # If lattice found nothing, try stream mode
            if not dfs or all(df.empty for df in dfs):
                dfs = tabula.read_pdf(
                    pdf_path,
                    pages="all",
                    multiple_tables=True,
                    stream=True,
                    silent=True,
                )
            
            if not dfs:
                logger.info("Tabula found no tables, falling back to pdfplumber")
                return self._pdfplumber_extract_all(pdf_path)
            
            # tabula doesn't always tell us which page a table came from,
            # so we try per-page extraction for accurate page mapping
            all_tables = {}
            for page_num in range(page_count):
                page_tables = self._tabula_extract_page(pdf_path, page_num)
                if page_tables:
                    all_tables[page_num] = page_tables
            
            # If per-page found nothing but bulk did, just put all on page 0
            if not all_tables and dfs:
                tables_as_lists = []
                for df in dfs:
                    if df.empty:
                        continue
                    table = [list(df.columns)]
                    for _, row in df.iterrows():
                        table.append([str(v) if pd.notna(v) else "" for v in row.values])
                    tables_as_lists.append(table)
                if tables_as_lists:
                    all_tables[0] = tables_as_lists
            
            return all_tables
            
        except Exception as e:
            logger.warning(f"Tabula extract_all failed, falling back to pdfplumber: {e}")
            return self._pdfplumber_extract_all(pdf_path)
    
    def _tabula_extract_page(self, pdf_path: str, page_number: int) -> List[List[List[str]]]:
        """Extract tables from a single page using tabula"""
        try:
            dfs = tabula.read_pdf(
                pdf_path,
                pages=str(page_number + 1),
                multiple_tables=True,
                lattice=True,
                silent=True,
            )
            
            if not dfs or all(df.empty for df in dfs):
                dfs = tabula.read_pdf(
                    pdf_path,
                    pages=str(page_number + 1),
                    multiple_tables=True,
                    stream=True,
                    silent=True,
                )
            
            if not dfs:
                return []
            
            tables = []
            for df in dfs:
                if df.empty:
                    continue
                table = [list(df.columns)]
                for _, row in df.iterrows():
                    table.append([str(v) if pd.notna(v) else "" for v in row.values])
                tables.append(table)
            
            return tables
        except Exception:
            return []
    
    def _pdfplumber_extract_all(self, pdf_path: str) -> Dict[int, List[List[List[str]]]]:
        """Fallback: extract all tables using pdfplumber"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_tables = {}
                for page_num, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if tables:
                        all_tables[page_num] = tables
                return all_tables
        except Exception as e:
            logger.error(f"pdfplumber fallback also failed: {e}")
            return {}
    
    def get_pdf_metadata(self, pdf_path: str) -> Dict[str, Any]:
        """Extract PDF metadata"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                return {
                    "total_pages": len(pdf.pages),
                    "metadata": pdf.metadata or {},
                    "page_dimensions": [
                        {"width": page.width, "height": page.height}
                        for page in pdf.pages[:5]  # First 5 pages
                    ]
                }
        except Exception as e:
            logger.error(f"Error getting PDF metadata: {e}")
            return {"total_pages": 0, "metadata": {}, "page_dimensions": []}
