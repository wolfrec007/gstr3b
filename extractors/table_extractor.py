from typing import Dict, Any, List
import logging
from .pdf_processor import PDFProcessor
import re

logger = logging.getLogger(__name__)

class TableExtractor:
    """Extract and structure tables from GSTR PDFs"""
    
    # GSTR form configurations
    GSTR_CONFIGS = {
        "gstr_1": {
            "name": "GSTR-1 (Outward Supplies)",
            "expected_sections": ["B2B", "B2C", "Credit/Debit Notes", "Exports"],
            "key_fields": ["GSTIN", "Invoice Number", "Invoice Date", "Taxable Value", "IGST", "CGST", "SGST"]
        },
        "gstr_2a": {
            "name": "GSTR-2A (Inward Supplies)",
            "expected_sections": ["B2B", "Import of Goods", "ISD Credit"],
            "key_fields": ["GSTIN", "Invoice Number", "Invoice Date", "Taxable Value", "IGST", "CGST", "SGST"]
        },
        "gstr_2b": {
            "name": "GSTR-2B (Auto-drafted ITC Statement)",
            "expected_sections": ["B2B", "Import of Goods", "IGST Paid"],
            "key_fields": ["GSTIN", "Invoice Number", "Invoice Date", "Taxable Value", "Eligible ITC"]
        },
        "gstr_3b": {
            "name": "GSTR-3B (Summary Return)",
            "expected_sections": ["Outward Supplies", "Inward Supplies", "ITC", "Tax Payment"],
            "key_fields": ["Taxable Value", "IGST", "CGST", "SGST", "Cess"]
        }
    }
    
    def __init__(self):
        self.pdf_processor = PDFProcessor()
    
    def extract_from_pdf(self, pdf_path: str, form_type: str = "gstr_1") -> Dict[str, Any]:
        """Extract tables from GSTR PDF"""
        try:
            # Get PDF metadata
            metadata = self.pdf_processor.get_pdf_metadata(pdf_path)
            total_pages = metadata['total_pages']
            
            # Extract all tables
            all_tables = self.pdf_processor.extract_all_tables(pdf_path)
            
            # Process and structure tables
            structured_tables = []
            
            for page_num, page_tables in all_tables.items():
                for table_idx, table in enumerate(page_tables):
                    if not table or len(table) < 2:  # Skip empty or single-row tables
                        continue
                    
                    structured_table = self._structure_table(table, form_type)
                    if structured_table:
                        structured_tables.append({
                            "page": page_num + 1,
                            "table_index": table_idx,
                            "name": structured_table.get("name", f"Table {table_idx + 1}"),
                            "columns": structured_table['columns'],
                            "rows": structured_table['rows'],
                            "row_count": len(structured_table['rows'])
                        })
            
            return {
                "total_pages": total_pages,
                "tables": structured_tables,
                "metadata": {
                    "form_type": form_type,
                    "form_name": self.GSTR_CONFIGS.get(form_type, {}).get("name", "Unknown"),
                    "total_tables": len(structured_tables)
                }
            }
        
        except Exception as e:
            logger.error(f"Error extracting from PDF: {e}")
            raise
    
    def _structure_table(self, table: List[List[str]], form_type: str) -> Dict[str, Any]:
        """Structure raw table data into columns and rows"""
        if not table or len(table) < 2:
            return None
        
        # Clean up table data
        cleaned_table = []
        for row in table:
            cleaned_row = [cell.strip() if cell else "" for cell in row]
            cleaned_table.append(cleaned_row)
        
        # First row is typically headers
        headers = cleaned_table[0]
        
        # Filter out empty headers and create clean column names
        columns = []
        valid_column_indices = []
        
        for idx, header in enumerate(headers):
            if header:  # Only include non-empty headers
                columns.append(header)
                valid_column_indices.append(idx)
        
        # If no valid columns found, return None
        if not columns:
            return None
        
        # Extract data rows
        rows = []
        for row in cleaned_table[1:]:
            # Skip empty rows
            if not any(cell for cell in row):
                continue
            
            # Create row dict with only valid columns
            row_dict = {}
            for col_idx, header in zip(valid_column_indices, columns):
                if col_idx < len(row):
                    row_dict[header] = row[col_idx]
                else:
                    row_dict[header] = ""
            
            rows.append(row_dict)
        
        # Detect table name based on content
        table_name = self._detect_table_name(columns, rows, form_type)
        
        return {
            "name": table_name,
            "columns": columns,
            "rows": rows
        }
    
    def _detect_table_name(self, columns: List[str], rows: List[Dict], form_type: str) -> str:
        """Detect table name based on column headers and content"""
        # Convert columns to lowercase for matching
        columns_lower = [col.lower() for col in columns]
        
        # B2B table detection
        if any(keyword in ' '.join(columns_lower) for keyword in ['gstin', 'invoice number', 'invoice date']):
            return "B2B Invoices"
        
        # B2C table detection
        if any(keyword in ' '.join(columns_lower) for keyword in ['place of supply', 'rate', 'taxable value']):
            return "B2C Supplies"
        
        # Credit/Debit notes
        if any(keyword in ' '.join(columns_lower) for keyword in ['note number', 'note date', 'debit', 'credit']):
            return "Credit/Debit Notes"
        
        # Exports
        if any(keyword in ' '.join(columns_lower) for keyword in ['export', 'shipping bill']):
            return "Export Invoices"
        
        # Tax summary
        if any(keyword in ' '.join(columns_lower) for keyword in ['tax', 'igst', 'cgst', 'sgst', 'cess']):
            return "Tax Summary"
        
        return "Data Table"
    
    def validate_extraction(self, extraction_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate extracted data"""
        validation_result = {
            "is_valid": True,
            "errors": [],
            "warnings": []
        }
        
        if extraction_data['total_pages'] == 0:
            validation_result['is_valid'] = False
            validation_result['errors'].append("No pages found in PDF")
        
        if len(extraction_data['tables']) == 0:
            validation_result['warnings'].append("No tables extracted from PDF")
        
        # Validate each table has data
        for table in extraction_data['tables']:
            if table['row_count'] == 0:
                validation_result['warnings'].append(
                    f"Table '{table['name']}' on page {table['page']} has no data rows"
                )
        
        return validation_result
