import pandas as pd
import numpy as np
import os
from merge_trade_reports import generate_quant_audit_reports

def test_quant_audit():
    # Create dummy data
    data = {
        '交易 #': [1, 2, 3, 4], # Numeric IDs that previously caused "Unknown"
        '日期/時間': pd.to_datetime(['2024-01-01 10:00', '2024-01-02 10:00', '2024-01-01 11:00', '2024-01-02 11:00']),
        '淨損益 %': ['10%', '-5%', '20%', '10%'],
        '回撤 %': ['1%', '5%', '2%', '3%'],
        '商品': ['US30', 'US30', 'XAUUSD', 'XAUUSD']
    }
    df = pd.DataFrame(data)
    
    output_path = "test_output.xlsx"
    
    # Run the report generator
    generate_quant_audit_reports(df, output_path)
    
    # Check if files exist
    corr_file = "test_output_Correlation_Matrix.csv"
    mae_file = "test_output_MAE_Optimization.csv"
    
    assert os.path.exists(corr_file), "Correlation matrix file not found"
    assert os.path.exists(mae_file), "MAE optimization report file not found"
    
    # Check contents
    corr_df = pd.read_csv(corr_file, index_col=0)
    assert 'US30' in corr_df.columns
    assert 'XAUUSD' in corr_df.columns
    
    mae_df = pd.read_csv(mae_file)
    assert 'Symbol' in mae_df.columns
    assert len(mae_df) >= 2
    assert 'US30' in mae_df['Symbol'].values
    assert 'XAUUSD' in mae_df['Symbol'].values
    
    # Cleanup
    os.remove(corr_file)
    os.remove(mae_file)
    print("Quant Audit Logic Upgrade OK")

if __name__ == "__main__":
    test_quant_audit()
