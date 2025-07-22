"""
CallbackHandler_plotter.py

【功能說明】
------------------------------------------------------------
本檔案為 plotter 模組的回調處理器，負責處理 Dash 應用的回調函數。
包括參數篩選、圖表更新、數據過濾等互動功能。

【關聯流程與數據流】
------------------------------------------------------------
- 主流程：接收用戶輸入 → 處理數據 → 更新組件 → 返回結果
- 各模組間數據流明確，流程如下：

```mermaid
flowchart TD
    A[用戶交互] -->|觸發| B[回調函數]
    B -->|處理| C[數據過濾]
    C -->|生成| D[更新組件]
    D -->|返回| E[界面更新]
    E -->|顯示| F[用戶界面]
```

【主流程步驟與參數傳遞細節】
------------------------------------------------------------
- 由 BasePlotter 調用，負責設置和處理回調函數
- CallbackHandler 負責參數篩選、圖表更新、數據過濾
- **每次新增/修改回調函數、組件ID時，必須同步檢查本檔案與所有依賴模組**

【維護與擴充提醒】
------------------------------------------------------------
- 新增回調函數、組件ID時，請同步更新頂部註解與對應模組
- 若組件結構有變動，需同步更新 DashboardGenerator 的組件創建

【常見易錯點】
------------------------------------------------------------
- 回調函數命名衝突
- 組件ID不匹配
- 數據類型錯誤
- 回調依賴關係錯誤

【範例】
------------------------------------------------------------
- 基本使用：handler = CallbackHandler()
- 設置回調：handler.setup_callbacks(app, data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 BasePlotter 調用
- 依賴 DashboardGenerator 的組件ID
- 處理用戶交互並更新界面

【維護重點】
------------------------------------------------------------
- 新增/修改回調函數、組件ID時，務必同步更新本檔案與所有依賴模組
- 回調函數的輸入輸出組件ID需要特別注意一致性

【參考】
------------------------------------------------------------
- 詳細流程規範如有變動，請同步更新本註解與 README
- 其他模組如有依賴本檔案的行為，請於對應模組頂部註解標明
- Dash 回調函數設計請參考 Dash 官方文檔
"""

import logging
from typing import Dict, Any, List, Optional
from dash import Input, Output, State, callback_context, ALL
from dash.exceptions import PreventUpdate
import pandas as pd
import json
from dash import html, dcc, ctx

class CallbackHandler:
    """
    Dash 回調處理器
    
    負責處理 Dash 應用的回調函數，
    包括參數篩選、圖表更新、數據過濾等互動功能。
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        初始化回調處理器
        
        Args:
            logger: 日誌記錄器，預設為 None
        """
        self.logger = logger or logging.getLogger(__name__)
        self.data = None
        
    def setup_callbacks(self, app, data: Dict[str, Any]):
        # ⚠️ 以下 callback 為動態展開/收合 collapsible，支援多指標自動擴充，勿隨意更動！
        @app.callback(
            [Output({'type': 'entry_param_collapse', 'indicator': ALL}, 'is_open'),
             Output({'type': 'exit_param_collapse', 'indicator': ALL}, 'is_open')],
            [Input('entry_indicator_type_toggle', 'value'),
             Input('exit_indicator_type_toggle', 'value')],
            [State({'type': 'entry_param_collapse', 'indicator': ALL}, 'id'),
             State({'type': 'exit_param_collapse', 'indicator': ALL}, 'id')]
        )
        def toggle_collapse(entry_types, exit_types, entry_ids, exit_ids):
            entry_open = [id['indicator'] in entry_types for id in entry_ids]
            exit_open = [id['indicator'] in exit_types for id in exit_ids]
            return entry_open, exit_open
        # ⚠️ 全選按鈕 callback 只做全選，不做全不選，且只影響對應 indicator checklist，其他保持原狀。
        @app.callback(
            Output({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            Input({'type': 'entry_param_select_all', 'indicator': ALL}, 'n_clicks'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            prevent_initial_call=True
        )
        def entry_select_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered['indicator']
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_['indicator'] == indicator:
                    all_vals = [o['value'] for o in opts]
                    values.append(all_vals)
                else:
                    values.append(cur)
            return values
        @app.callback(
            Output({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            Input({'type': 'exit_param_select_all', 'indicator': ALL}, 'n_clicks'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            prevent_initial_call=True
        )
        def exit_select_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered['indicator']
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_['indicator'] == indicator:
                    all_vals = [o['value'] for o in opts]
                    values.append(all_vals)
                else:
                    values.append(cur)
            return values
        # === 功能性主 callback ===
        @app.callback(
            Output('equity_chart', 'figure'),
            Input('entry_indicator_type_toggle', 'value'),
            Input('exit_indicator_type_toggle', 'value'),
            Input({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            Input({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            Input({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            Input({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            Input({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            Input({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            Input('sorting_select', 'value'),
        )
        def update_equity_chart(entry_types, exit_types, entry_vals, exit_vals, entry_ids, exit_ids, entry_opts, exit_opts, sorting_value):
            parameters = data.get('parameters', [])
            equity_curves = data.get('equity_curves', {})
            bah_curves = data.get('bah_curves', {})  # <--- 新增這行，確保 BAH 曲線來源正確
            # checklist value 為空時自動補全為 options 全部 value
            entry_param_map = {}
            for val, id_, opts in zip(entry_vals, entry_ids, entry_opts):
                if id_['indicator'] in entry_types:
                    v = val if val else [o['value'] for o in opts]
                    entry_param_map.setdefault(id_['indicator'], {})[id_['param']] = v
            exit_param_map = {}
            for val, id_, opts in zip(exit_vals, exit_ids, exit_opts):
                if id_['indicator'] in exit_types:
                    v = val if val else [o['value'] for o in opts]
                    exit_param_map.setdefault(id_['indicator'], {})[id_['param']] = v
            backtest_ids = data.get('backtest_ids', [])
            metrics = data.get('metrics', {})
            filtered_ids = []
            for i, param in enumerate(parameters):
                bid = param.get('Backtest_id', backtest_ids[i] if i < len(backtest_ids) else str(i))
                entry_ok = False
                exit_ok = False
                for d in param.get('Entry_params', []):
                    if str(d.get('indicator_type')) in entry_types:
                        indicator = str(d.get('indicator_type'))
                        entry_ok = True
                        for k, v in d.items():
                            if k == 'indicator_type':
                                continue
                            if k in entry_param_map[indicator]:
                                checklist_vals = entry_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    entry_ok = False
                                    break
                        break
                for d in param.get('Exit_params', []):
                    if str(d.get('indicator_type')) in exit_types:
                        indicator = str(d.get('indicator_type'))
                        exit_ok = True
                        for k, v in d.items():
                            if k == 'indicator_type':
                                continue
                            if k in exit_param_map[indicator]:
                                checklist_vals = exit_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    exit_ok = False
                                    break
                        break
                if entry_ok and exit_ok:
                    filtered_ids.append(bid)
            # 只根據 sorting_value 排序，取前 20
            sort_map = {
                'Top20_Total_return': ('Total_return', True),
                'Top20_least_Max_drawdown': ('Max_drawdown', True),
                'Top20_Recovery_factor': ('Recovery_factor', True),
                'Top20_Sharpe': ('Sharpe', True),
                'Top20_Sortino': ('Sortino', True),
                'Top20_Calmar': ('Calmar', True),
                'Top20_Information_ratio': ('Information_ratio', True),
            }
            if sorting_value in sort_map:
                sort_field, descending = sort_map[sorting_value]
                filtered_ids = sorted(
                    filtered_ids,
                    key=lambda bid: float(metrics.get(bid, {}).get(sort_field, float('-inf' if descending else 'inf'))),
                    reverse=descending
                )[:20]
            import plotly.graph_objs as go
            fig = go.Figure()
            instrument_bah = {}
            for idx, bid in enumerate(filtered_ids):
                df = equity_curves.get(bid)
                if df is not None and not df.empty and 'Time' in df.columns and 'Equity_value' in df.columns:
                    fig.add_trace(go.Scatter(
                        x=pd.to_datetime(df['Time']),
                        y=df['Equity_value'],
                        mode='lines',
                        name=str(bid),
                        customdata=[bid]*len(df)
                    ))
                # 畫 BAH 曲線（每種 instrument 只畫一條）
                param = next((p for p in parameters if p.get('Backtest_id') == bid), None)
                if param:
                    instrument = param.get('Asset', None)
                    if instrument and instrument not in instrument_bah:
                        bah_df = bah_curves.get(bid)
                        if bah_df is not None and not bah_df.empty and 'Time' in bah_df.columns and 'BAH_Equity' in bah_df.columns:
                            fig.add_trace(go.Scatter(
                                x=pd.to_datetime(bah_df['Time']),
                                y=bah_df['BAH_Equity'],
                                mode='lines',
                                name=f"{instrument} (BAH)",
                                line=dict(dash='dot', color='#ecbc4f')
                            ))
                            instrument_bah[instrument] = True
            fig.update_layout(
                title="權益曲線比較",
                xaxis_title="時間",
                yaxis_title="權益值",
                template=None,
                height=1000,
                showlegend=True,
                plot_bgcolor="#181818",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=15),
                legend=dict(font=dict(color="#ecbc4f", size=13)),
                xaxis=dict(color="#f5f5f5", gridcolor="#444"),
                yaxis=dict(color="#f5f5f5", gridcolor="#444")
            )
            return fig 
        # === 選中策略詳情展示 callback ===
        @app.callback(
            Output('selected_details', 'children'),
            Input('equity_chart', 'clickData'),
            State('entry_indicator_type_toggle', 'value'),
            State('exit_indicator_type_toggle', 'value'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
        )
        def show_selected_details(clickData, entry_types, exit_types, entry_vals, exit_vals, entry_ids, exit_ids, entry_opts, exit_opts):
            # 找出目前 filtered_ids
            parameters = data.get('parameters', [])
            backtest_ids = data.get('backtest_ids', [])
            metrics = data.get('metrics', {})
            # checklist value 為空時自動補全為 options 全部 value
            entry_param_map = {}
            for val, id_, opts in zip(entry_vals, entry_ids, entry_opts):
                if id_['indicator'] in entry_types:
                    v = val if val else [o['value'] for o in opts]
                    entry_param_map.setdefault(id_['indicator'], {})[id_['param']] = v
            exit_param_map = {}
            for val, id_, opts in zip(exit_vals, exit_ids, exit_opts):
                if id_['indicator'] in exit_types:
                    v = val if val else [o['value'] for o in opts]
                    exit_param_map.setdefault(id_['indicator'], {})[id_['param']] = v
            filtered_ids = []
            for i, param in enumerate(parameters):
                bid = param.get('Backtest_id', backtest_ids[i] if i < len(backtest_ids) else str(i))
                entry_ok = False
                exit_ok = False
                for d in param.get('Entry_params', []):
                    if str(d.get('indicator_type')) in entry_types:
                        indicator = str(d.get('indicator_type'))
                        entry_ok = True
                        for k, v in d.items():
                            if k == 'indicator_type':
                                continue
                            if k in entry_param_map[indicator]:
                                checklist_vals = entry_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    entry_ok = False
                                    break
                        break
                for d in param.get('Exit_params', []):
                    if str(d.get('indicator_type')) in exit_types:
                        indicator = str(d.get('indicator_type'))
                        exit_ok = True
                        for k, v in d.items():
                            if k == 'indicator_type':
                                continue
                            if k in exit_param_map[indicator]:
                                checklist_vals = exit_param_map[indicator][k]
                                if str(v) not in [str(x) for x in checklist_vals]:
                                    exit_ok = False
                                    break
                        break
                if entry_ok and exit_ok:
                    filtered_ids.append(bid)
            # 根據 clickData 找出選中的 Backtest_id
            sel_bid = None
            if clickData and 'points' in clickData and len(clickData['points']) > 0:
                sel_bid = clickData['points'][0].get('customdata')
                if sel_bid not in metrics:
                    sel_bid = None
            if not sel_bid:
                return '請點選資金曲線以顯示詳情'
            meta = metrics.get(sel_bid, {})
            # Details 欄位
            details_fields = [
                'Backtest_id','Frequency','Asset','Strategy','Predictor','Entry_params','Exit_params',
                'Transaction_cost','Slippage_cost','Trade_delay','Trade_price','Data_start_time','Data_end_time'
            ]
            details_table = [
                html.Tr([html.Th(f), html.Td(str(meta.get(f, '')))]) for f in details_fields
            ]
            # Performance 欄位
            def fmt3(x, field=None):
                try:
                    if field in ['Trade_count', 'Max_consecutive_losses']:
                        return str(int(float(x)))
                    f = float(x)
                    return f'{f:.3f}'
                except Exception:
                    return str(x)
            perf_fields = [
                'Total_return','Annualized_return (CAGR)','Std','Annualized_std','Downside_risk','Annualized_downside_risk',
                'Max_drawdown','Average_drawdown','Recovery_factor','Sharpe','Sortino','Calmar','Information_ratio','Alpha','Beta',
                'Trade_count','Win_rate','Profit_factor','Avg_trade_return','Max_consecutive_losses','Exposure_time','Max_holding_period_ratio'
            ]
            bah_fields = [
                'BAH_Total_return','BAH_Annualized_return (CAGR)','BAH_Std','BAH_Annualized_std','BAH_Downside_risk','BAH_Annualized_downside_risk',
                'BAH_Max_drawdown','BAH_Average_drawdown','BAH_Recovery_factor','BAH_Sharpe','BAH_Sortino','BAH_Calmar'
            ]
            perf_table = [
                html.Tr([html.Th(f), html.Td(fmt3(meta.get(f, ''), f))]) for f in perf_fields
            ]
            bah_table = [
                html.Tr([html.Th(f), html.Td(fmt3(meta.get(f, ''), f))]) for f in bah_fields
            ]
            return html.Div([
                html.H5('Details'),
                html.Table(details_table, className='table table-sm table-bordered'),
                html.H5('Performance'),
                html.Div([
                    html.Div([
                        html.Table(perf_table, className='table table-sm table-bordered')
                    ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
                    html.Div([
                        html.Table(bah_table, className='table table-sm table-bordered')
                    ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '4%'}),
                ])
            ]) 
        from dash import html, dcc, ctx
        @app.callback(
            [Output('filter_list_store', 'data'), Output('active_filters', 'children')],
            [Input('filter_apply_btn', 'n_clicks'),
             Input({'type': 'remove_filter_btn', 'index': ALL}, 'n_clicks')],
            [State('filter_metric', 'value'),
             State('filter_op', 'value'),
             State('filter_value', 'value'),
             State('filter_list_store', 'data')],
            prevent_initial_call=True
        )
        def update_filter_list(apply_click, remove_clicks, metric, op, value, filter_list):
            triggered = ctx.triggered_id
            # 新增 filter
            if triggered == 'filter_apply_btn' and metric and op and value is not None:
                filter_list = filter_list or []
                filter_list.append({'metric': metric, 'op': op, 'value': value})
            # 移除 filter
            elif isinstance(triggered, dict) and triggered.get('type') == 'remove_filter_btn':
                idx = triggered.get('index')
                if filter_list and 0 <= idx < len(filter_list):
                    filter_list.pop(idx)
            # 生成顯示
            children = []
            for i, f in enumerate(filter_list):
                label = f"{f['metric']} {f['op']} {f['value']}"
                children.append(html.Div([
                    html.Span(label, style={"color": "#ecbc4f", "marginRight": "6px"}),
                    html.Button("x", id={"type": "remove_filter_btn", "index": i}, n_clicks=0, style={"background": "#8f1511", "color": "#fff", "border": "none", "borderRadius": "4px", "padding": "0 6px", "cursor": "pointer"})
                ], style={"background": "#232323", "border": "1.5px solid #8f1511", "borderRadius": "4px", "padding": "2px 8px", "display": "flex", "alignItems": "center"}))
            return filter_list, children 