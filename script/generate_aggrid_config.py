#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze all database tables and generate AG Grid column width configurations.
Calculates column widths based on:
1. Header text length (capped at 6 characters for width calculation)
2. Actual data length from database samples
Formula: width = max(data_width, min(header_length, 6)) * 13px + 20px padding
"""

import sys
import os
# scripts文件夹在项目根目录下,向上一级即可
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from instock.core import tablestructure
from instock.lib import database
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_table_columns():
    """Extract all table definitions with their columns and Chinese headers."""
    tables = {}
    
    # Get all TABLE_ constants from tablestructure module
    for attr_name in dir(tablestructure):
        if attr_name.startswith('TABLE_CN_'):
            table_def = getattr(tablestructure, attr_name)
            if isinstance(table_def, dict) and 'name' in table_def and 'columns' in table_def:
                table_name = table_def['name']
                table_cn = table_def['cn']
                columns = table_def['columns']
                
                tables[table_name] = {
                    'cn': table_cn,
                    'columns': {}
                }
                
                for col_key, col_def in columns.items():
                    header_cn = col_def.get('cn', col_key)
                    header_length = len(header_cn)
                    
                    tables[table_name]['columns'][col_key] = {
                        'header_cn': header_cn,
                        'header_length': header_length,
                        'type': col_def.get('type'),
                        'size': col_def.get('size', 70)
                    }
    
    return tables


def sample_data_lengths(table_name, columns, sample_size=100):
    """Sample actual data from database to determine maximum field lengths."""
    try:
        db = database.get_db()
        if not db:
            logger.warning(f"Cannot connect to database for table {table_name}")
            return {}
        
        # Get column names
        col_names = list(columns.keys())
        if not col_names:
            return {}
        
        # Build query - select all columns with LIMIT
        cols_str = ', '.join([f"`{col}`" for col in col_names])
        query = f"SELECT {cols_str} FROM `{table_name}` LIMIT {sample_size}"
        
        results = db.query(query)
        if not results:
            logger.info(f"No data found for table {table_name}")
            return {}
        
        # Calculate max length for each column
        max_lengths = {}
        for row in results:
            for col in col_names:
                value = getattr(row, col, None)
                if value is not None:
                    # Convert to string and measure length
                    str_value = str(value)
                    current_max = max_lengths.get(col, 0)
                    max_lengths[col] = max(current_max, len(str_value))
        
        db.close()
        return max_lengths
    
    except Exception as e:
        logger.error(f"Error sampling data for {table_name}: {e}")
        return {}


def calculate_column_width(header_length, data_length):
    """
    Calculate optimal column width.
    
    Args:
        header_length: Number of Chinese characters in header
        data_length: Maximum character length of data values
    
    Returns:
        Dictionary with min, max, and default widths in pixels
    """
    # For header width calculation, cap at 6 characters
    effective_header_length = min(header_length, 6)
    
    # Chinese character width at 13px font ~13-15px, use 14px average
    char_width = 14
    padding = 20  # Left + right padding
    
    # Calculate required widths
    header_width = effective_header_length * char_width + padding
    data_width = data_length * char_width + padding
    
    # Take the maximum
    base_width = max(header_width, data_width)
    
    # Add some buffer for safety
    min_width = int(base_width * 0.9)  # 90% of calculated
    max_width = int(base_width * 1.2)  # 120% of calculated
    default_width = base_width
    
    # Ensure minimum reasonable width
    min_width = max(min_width, 70)
    default_width = max(default_width, 80)
    max_width = max(max_width, 100)
    
    return {
        'min': min_width,
        'max': max_width,
        'default': default_width
    }


def generate_js_config(tables, data_samples):
    """Generate JavaScript column width configuration."""
    lines = []
    lines.append("            // Auto-generated column width configuration")
    lines.append("            // Formula: width = max(data_length, min(header_length, 6)) * 14px + 20px")
    lines.append("            const columnWidths = {")
    
    all_configs = {}
    
    for table_name, table_info in sorted(tables.items()):
        lines.append(f"                // {table_info['cn']} ({table_name})")
        
        for col_key, col_info in sorted(table_info['columns'].items()):
            header_cn = col_info['header_cn']
            header_length = col_info['header_length']
            
            # Get sampled data length
            sample_data = data_samples.get(table_name, {})
            data_length = sample_data.get(col_key, 0)
            
            # If no data sample, estimate based on type
            if data_length == 0:
                col_type = col_info['type']
                if hasattr(col_type, '__name__'):
                    type_name = col_type.__name__
                    if 'INT' in type_name or 'BIGINT' in type_name:
                        data_length = 12  # Large numbers
                    elif 'FLOAT' in type_name:
                        data_length = 8   # Decimal numbers
                    elif 'VARCHAR' in type_name:
                        # Extract VARCHAR size
                        if hasattr(col_type, 'length'):
                            data_length = min(col_type.length, 20)
                        else:
                            data_length = 10
                    elif 'DATE' in type_name or 'DATETIME' in type_name:
                        data_length = 10  # YYYY-MM-DD
                    else:
                        data_length = 10
            
            # Calculate width
            width_config = calculate_column_width(header_length, data_length)
            
            # Store for output
            config_key = f"'{col_key}'"
            if config_key not in all_configs:
                all_configs[config_key] = width_config
                
                comment = f"// {header_cn}: 表头{header_length}字"
                if data_length > 0:
                    comment += f", 数据{data_length}字符"
                
                line = f"                {config_key}: {{ min: {width_config['min']}, max: {width_config['max']}, default: {width_config['default']} }}, {comment}"
                lines.append(line)
        
        lines.append("")
    
    lines.append("            };")
    
    return '\n'.join(lines)


def main():
    print("=" * 80)
    print("AG Grid Column Width Configuration Generator")
    print("=" * 80)
    
    # Step 1: Extract all table definitions
    print("\n📊 Step 1: Extracting table definitions...")
    tables = get_table_columns()
    print(f"   Found {len(tables)} tables")
    
    for table_name, table_info in sorted(tables.items()):
        col_count = len(table_info['columns'])
        print(f"   - {table_info['cn']} ({table_name}): {col_count} columns")
    
    # Step 2: Sample data from database
    print("\n🔍 Step 2: Sampling data from database...")
    data_samples = {}
    
    for table_name, table_info in tables.items():
        print(f"   Sampling {table_info['cn']}...")
        samples = sample_data_lengths(table_name, table_info['columns'], sample_size=50)
        data_samples[table_name] = samples
        
        # Show some stats
        if samples:
            avg_len = sum(samples.values()) / len(samples)
            max_len = max(samples.values())
            print(f"      → {len(samples)} fields sampled, avg length: {avg_len:.1f}, max: {max_len}")
    
    # Step 3: Generate JavaScript configuration
    print("\n⚙️  Step 3: Generating column width configuration...")
    js_config = generate_js_config(tables, data_samples)
    
    # Write to file (保存到scripts文件夹)
    output_file = os.path.join(os.path.dirname(__file__), 'column_width_config.txt')
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(js_config)
    
    print(f"\n✅ Configuration generated successfully!")
    print(f"   Output file: {output_file}")
    print(f"\n📋 Preview (first 30 lines):")
    print('\n'.join(js_config.split('\n')[:30]))
    print("\n   ... (see full file for complete configuration)")
    
    # Summary statistics
    print("\n📈 Summary:")
    total_cols = sum(len(t['columns']) for t in tables.values())
    print(f"   Total tables: {len(tables)}")
    print(f"   Total columns: {total_cols}")
    

if __name__ == '__main__':
    main()
