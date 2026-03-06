import csv
from pathlib import Path
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

class CSVExporter:
    """Export extraction data to CSV format"""
    
    def __init__(self):
        pass
    
    def export(self, extraction_data: Dict[str, Any], output_path: Path) -> Path:
        """Export extraction to CSV file (combines all tables)"""
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = None
                
                for table in extraction_data.get('tables', []):
                    columns = table.get('columns', [])
                    rows = table.get('rows', [])
                    
                    if not columns:
                        continue
                    
                    # Write table name as separator
                    if writer is None:
                        writer = csv.DictWriter(csvfile, fieldnames=columns)
                    
                    csvfile.write(f"\n# {table['name']} (Page {table['page']})\n")
                    writer.writeheader()
                    
                    # Write rows
                    for row in rows:
                        writer.writerow(row)
                    
                    csvfile.write("\n")  # Empty line between tables
            
            logger.info(f"CSV file exported to {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            raise
    
    def export_consolidated(self, consolidated_data: Dict[str, Any], output_path: Path) -> Path:
        """Export consolidated yearly data to CSV"""
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                # Write summary header
                csvfile.write(f"# Yearly Consolidated Report\n")
                csvfile.write(f"# Year: {consolidated_data.get('year', '')}\n")
                csvfile.write(f"# Form Type: {consolidated_data.get('form_type', '')}\n")
                csvfile.write(f"# Total Records: {consolidated_data.get('total_records', 0)}\n\n")
                
                monthly_data = consolidated_data.get('monthly_data', {})
                writer = None
                
                for month, records in monthly_data.items():
                    if not records:
                        continue
                    
                    csvfile.write(f"\n# {month}\n")
                    
                    if isinstance(records[0], dict):
                        columns = list(records[0].keys())
                        writer = csv.DictWriter(csvfile, fieldnames=columns)
                        writer.writeheader()
                        
                        for record in records:
                            writer.writerow(record)
                    
                    csvfile.write("\n")
            
            logger.info(f"Consolidated CSV file exported to {output_path}")
            return output_path
        
        except Exception as e:
            logger.error(f"Error exporting consolidated data to CSV: {e}")
            raise
