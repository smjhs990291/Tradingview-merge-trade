import tempfile
from pathlib import Path

import streamlit as st
import pandas as pd
import io

from merge_trade_reports import merge_and_export_to_bytes


st.set_page_config(page_title="交易紀錄合併與績效報表", layout="wide")

st.title("交易紀錄合併與績效報表")

st.markdown(
    """
上傳多個 Excel 交易報表（TradingView 匯出格式），系統會自動合併並輸出與原檔相同呈現的報表：

- `績效`
- `交易分析`
- `風險 績效比`
- `交易清單`
- `屬性`
"""
)

uploaded_files = st.file_uploader(
    "選取要合併的 Excel 檔案（可多選）",
    type=["xlsx"],
    accept_multiple_files=True,
)

output_name = st.text_input("輸出檔名", value="merged_report.xlsx")

if uploaded_files:
    st.write(f"已選取檔案數量：{len(uploaded_files)}")

if st.button("產生合併報表", type="primary", disabled=not uploaded_files):
    if not output_name.lower().endswith(".xlsx"):
        output_name = output_name + ".xlsx"

    with st.spinner("處理中..."):
        temp_paths = []
        try:
            for uf in uploaded_files:
                suffix = Path(uf.name).suffix
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uf.getbuffer())
                    temp_paths.append(tmp.name)

            # Core processing
            # Signature: bytes, perf, corr_matrix, mae_df, equity_curve, stats_all
            report_bytes, perf, corr_df, mae_df, equity_curve, stats_all = merge_and_export_to_bytes(temp_paths)

            # --- Strategy Dashboard ---
            st.divider()
            
            # 1. Main KPI Metrics
            m_cols = st.columns(5) # Changed to 5 columns
            net_profit_val = stats_all.get("net_profit", 0)
            net_profit_pct = stats_all.get("net_profit_pct", 0)
            win_rate = stats_all.get("win_rate", 0)
            profit_factor = stats_all.get("profit_factor", 0)
            avg_eff = stats_all.get("avg_pos_efficiency", 0)
            
            # Extract Max DD from perf dataframe (Now relative)
            try:
                max_dd_row = perf[perf["Unnamed: 0"] == "最大資產回撤"]
                if not max_dd_row.empty:
                    max_dd_val = max_dd_row["全部 USD"].values[0]
                    max_dd_pct = max_dd_row["全部 %"].values[0]
                else:
                    max_dd_val, max_dd_pct = 0, 0
            except:
                max_dd_val, max_dd_pct = 0, 0

            m_cols[0].metric("總淨利 (Net Profit)", f"{net_profit_val:,.2f} USD", f"{net_profit_pct:.2f}%")
            m_cols[1].metric("最大回撤 (Max DD%)", f"{abs(max_dd_val):,.2f} USD", f"{abs(max_dd_pct):.2f}%", delta_color="inverse")
            m_cols[2].metric("獲利因子 (PF)", f"{profit_factor:.3f}")
            m_cols[3].metric("勝率 (Win Rate)", f"{win_rate:.2f}%")
            m_cols[4].metric("持倉效率 (Efficiency)", f"{avg_eff:.4f}%")

            # 2. Equity Curve Chart (Plotly)
            import plotly.graph_objects as go
            
            st.subheader("策略資產曲線 (Strategy Equity Curve)")
            
            if not equity_curve.empty:
                # Compute Drawdown for shading
                equity_curve['peak'] = equity_curve['equity'].cummax()
                equity_curve['drawdown'] = (equity_curve['equity'] - equity_curve['peak'])
                
                fig = go.Figure()
                
                # Main Equity Area
                fig.add_trace(go.Scatter(
                    x=equity_curve["日期/時間"] if "日期/時間" in equity_curve.columns else equity_curve.index,
                    y=equity_curve["equity"],
                    mode='lines',
                    name='Equity',
                    fill='tozeroy',
                    fillcolor='rgba(38, 166, 154, 0.1)', 
                    line=dict(color='#26a69a', width=3),
                    hovertemplate='<b>Date</b>: %{x}<br><b>Equity</b>: %{y:,.2f} USD<extra></extra>'
                ))
                
                # Peak Line
                fig.add_trace(go.Scatter(
                    x=equity_curve["日期/時間"] if "日期/時間" in equity_curve.columns else equity_curve.index,
                    y=equity_curve["peak"],
                    mode='lines',
                    name='Peak',
                    line=dict(color='rgba(255, 255, 255, 0.2)', width=1, dash='dot'),
                    showlegend=False
                ))

                fig.update_layout(
                    template="plotly_dark",
                    hovermode="x unified",
                    height=500,
                    margin=dict(l=0, r=0, t=30, b=0),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(showgrid=True, gridcolor='#333333'),
                    yaxis=dict(showgrid=True, gridcolor='#333333', title="Equity (USD)"),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("查無資產曲線數據")

            # 3. Profit Structure
            st.subheader("獲利結構 (Performance Structure)")
            gross_profit = stats_all.get("gross_profit", 0)
            gross_loss = abs(stats_all.get("gross_loss", 0))
            
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                name='毛利 (Gross Profit)',
                y=['結構'],
                x=[gross_profit],
                orientation='h',
                marker=dict(color='#26a69a')
            ))
            fig_bar.add_trace(go.Bar(
                name='總損失 (Total Loss)',
                y=['結構'],
                x=[gross_loss],
                orientation='h',
                marker=dict(color='#ef5350')
            ))
            
            fig_bar.update_layout(
                barmode='stack',
                template="plotly_dark",
                height=180,
                margin=dict(l=0, r=0, t=30, b=0),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(title="USD"),
                showlegend=True
            )
            st.plotly_chart(fig_bar, use_container_width=True)

            st.divider()
            st.success("報表產生完成")
            
            # Use tabs for detail inspection
            tab1, tab2, tab3 = st.tabs(["📊 合併報表 (Excel)", "🔗 相關性分析", "🛡️ MAE 止損優化"])
            
            with tab1:
                st.subheader("主報表下載")
                st.download_button(
                    label="下載合併報表 (.xlsx)",
                    data=report_bytes,
                    file_name=output_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
                st.info("包含 績效、交易分析、風險績效比、交易清單、屬性 等分頁。")

            with tab2:
                st.subheader("投資組合相關性矩陣 (Pearson's r)")
                st.markdown("數值越接近 1 代表商品走勢越同步，建議維持在 **0.3 以下** 以達到避險效果。")
                
                # Dynamic coloring for the correlation matrix
                st.dataframe(
                    corr_df.style.background_gradient(cmap='RdYlGn_r', axis=None, vmin=-1, vmax=1),
                    use_container_width=True
                )
                
                corr_csv = corr_df.to_csv().encode('utf-8-sig')
                st.download_button(
                    label="下載相關性矩陣 (CSV)",
                    data=corr_csv,
                    file_name=output_name.replace(".xlsx", "_Correlation_Matrix.csv"),
                    mime="text/csv",
                    use_container_width=True
                )

            with tab3:
                st.subheader("MFE/MAE 微觀執行效率")
                st.markdown("分析獲利交易的回徹深度，協助優化初始止損點位。")
                
                st.dataframe(mae_df, use_container_width=True)
                
                mae_csv = mae_df.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="下載 MAE 優化報告 (CSV)",
                    data=mae_csv,
                    file_name=output_name.replace(".xlsx", "_MAE_Optimization.csv"),
                    mime="text/csv",
                    use_container_width=True
                )
                
                st.success("💡 建議：參考 90th Percentile MAE 來微調你的止損參數。")

        except Exception as e:
            st.error(f"發生錯誤: {e}")
            st.exception(e)
        finally:
            for p in temp_paths:
                try:
                    Path(p).unlink(missing_ok=True)
                except:
                    pass

st.divider()

with st.expander("進階說明"):
    st.markdown(
        """
- `已支付佣金` 會維持為 `0`（不計手續費）。
- 指標計算以 `交易清單` 中的 `出場` 交易為基礎。
- 繪圖數據來源於合併後的每日累積資產變化。
"""
    )
