"""
CallbackHandler_plotter.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 可視化平台的回調處理核心模組，負責處理 Dash 應用的回調函數，包括參數篩選、圖表更新、數據過濾等互動功能。

【流程與數據流】
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

【維護與擴充重點】
------------------------------------------------------------
- 新增回調函數、組件ID時，請同步更新頂部註解與對應模組
- 若組件結構有變動，需同步更新 DashboardGenerator 的組件創建
- 新增/修改回調函數、組件ID時，務必同步更新本檔案與所有依賴模組
- 回調函數的輸入輸出組件ID需要特別注意一致性

【常見易錯點】
------------------------------------------------------------
- 回調函數命名衝突
- 組件ID不匹配
- 數據類型錯誤
- 回調依賴關係錯誤

【錯誤處理】
------------------------------------------------------------
- 回調函數錯誤時提供詳細錯誤訊息
- 組件ID不匹配時提供診斷建議
- 數據處理失敗時提供備用方案

【範例】
------------------------------------------------------------
- 基本使用：handler = CallbackHandler()
- 設置回調：handler.setup_callbacks(app, data)

【與其他模組的關聯】
------------------------------------------------------------
- 被 BasePlotter 調用
- 依賴 DashboardGenerator 的組件ID
- 處理用戶交互並更新界面

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，支援基本回調處理
- v1.1: 新增動態回調支援
- v1.2: 新增多指標自動擴充回調

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
        # 頁面切換回調函數
        @app.callback(
            [Output("layout-asset-curve-with-panel", "style"),
             Output("layout-parameter-landscape-full", "style")],
            [Input("btn-asset-curve", "n_clicks"),
             Input("btn-parameter-landscape", "n_clicks")],
            prevent_initial_call=False
        )
        def switch_page(asset_curve_clicks, parameter_landscape_clicks):
            """切換頁面顯示"""
            if not ctx.triggered_id:
                # 初始狀態：顯示資產曲線頁面
                return {"display": "block"}, {"display": "none"}
            
            if ctx.triggered_id == "btn-asset-curve":
                return {"display": "block"}, {"display": "none"}
            elif ctx.triggered_id == "btn-parameter-landscape":
                return {"display": "none"}, {"display": "block"}
            
            # 預設顯示資產曲線頁面
            return {"display": "block"}, {"display": "none"}
        
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
        # ⚠️ 切換按鈕 callback 在全選和清空之間切換，只影響對應 indicator checklist，其他保持原狀。
        @app.callback(
            [Output({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
             Output({'type': 'entry_param_toggle_all', 'indicator': ALL}, 'children')],
            Input({'type': 'entry_param_toggle_all', 'indicator': ALL}, 'n_clicks'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            State({'type': 'entry_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            prevent_initial_call=True
        )
        def entry_toggle_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered['indicator']
            
            # 動態處理：為每個參數設置值
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_['indicator'] == indicator:
                    all_vals = [o['value'] for o in opts]
                    # 檢查是否已經全選
                    is_all_selected = cur and len(cur) == len(all_vals) and all(v in cur for v in all_vals)
                    if is_all_selected:
                        # 如果全選，則清空
                        values.append([])
                    else:
                        # 如果不是全選，則全選
                        values.append(all_vals)
                else:
                    values.append(cur)
            
            # 動態處理：為每個indicator按鈕設置文字
            # 獲取所有唯一的indicator類型
            unique_indicators = set(id_['indicator'] for id_ in ids)
            button_texts = []
            
            for unique_indicator in unique_indicators:
                if unique_indicator == indicator:
                    # 檢查觸發的indicator是否全選
                    indicator_values = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            indicator_values.extend(cur if cur else [])
                    
                    # 獲取該indicator的所有選項
                    all_options = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            all_options.extend([o['value'] for o in opts])
                    
                    # 檢查是否全選
                    is_all_selected = len(indicator_values) == len(all_options) and all(v in indicator_values for v in all_options)
                    button_texts.append("反選" if is_all_selected else "全選")
                else:
                    button_texts.append("全選")
            
            return values, button_texts
        @app.callback(
            [Output({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
             Output({'type': 'exit_param_toggle_all', 'indicator': ALL}, 'children')],
            Input({'type': 'exit_param_toggle_all', 'indicator': ALL}, 'n_clicks'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'id'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'options'),
            State({'type': 'exit_param_checklist', 'indicator': ALL, 'param': ALL}, 'value'),
            prevent_initial_call=True
        )
        def exit_toggle_all(n_clicks, ids, options, current_values):
            triggered = ctx.triggered_id
            if not triggered:
                raise PreventUpdate
            indicator = triggered['indicator']
            
            # 動態處理：為每個參數設置值
            values = []
            for id_, opts, cur in zip(ids, options, current_values):
                if id_['indicator'] == indicator:
                    all_vals = [o['value'] for o in opts]
                    # 檢查是否已經全選
                    is_all_selected = cur and len(cur) == len(all_vals) and all(v in cur for v in all_vals)
                    if is_all_selected:
                        # 如果全選，則清空
                        values.append([])
                    else:
                        # 如果不是全選，則全選
                        values.append(all_vals)
                else:
                    values.append(cur)
            
            # 動態處理：為每個indicator按鈕設置文字
            # 獲取所有唯一的indicator類型
            unique_indicators = set(id_['indicator'] for id_ in ids)
            button_texts = []
            
            for unique_indicator in unique_indicators:
                if unique_indicator == indicator:
                    # 檢查觸發的indicator是否全選
                    indicator_values = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            indicator_values.extend(cur if cur else [])
                    
                    # 獲取該indicator的所有選項
                    all_options = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            all_options.extend([o['value'] for o in opts])
                    
                    # 檢查是否全選
                    is_all_selected = len(indicator_values) == len(all_options) and all(v in indicator_values for v in all_options)
                    button_texts.append("反選" if is_all_selected else "全選")
                else:
                    button_texts.append("全選")
            
            return values, button_texts
            
            # 動態處理：為每個indicator按鈕設置文字
            # 獲取所有唯一的indicator類型
            unique_indicators = set(id_['indicator'] for id_ in ids)
            button_texts = []
            
            for unique_indicator in unique_indicators:
                if unique_indicator == indicator:
                    # 檢查觸發的indicator是否全選
                    indicator_values = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            indicator_values.extend(cur if cur else [])
                    
                    # 獲取該indicator的所有選項
                    all_options = []
                    for id_, opts, cur in zip(ids, options, current_values):
                        if id_['indicator'] == unique_indicator:
                            all_options.extend([o['value'] for o in opts])
                    
                    # 檢查是否全選
                    is_all_selected = len(indicator_values) == len(all_options) and all(v in indicator_values for v in all_options)
                    button_texts.append("反選" if is_all_selected else "全選")
                else:
                    button_texts.append("全選")
            
            return values, button_texts
        

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
                yaxis=dict(color="#f5f5f5", gridcolor="#444"),
                margin=dict(l=50, r=50, t=50, b=50)
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
                html.Table(details_table, className='table table-sm table-bordered details-table'),
                html.H5('Performance'),
                html.Div([
                    html.Div([
                        html.Table(perf_table, className='table table-sm table-bordered performance-table')
                    ], style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
                    html.Div([
                        html.Table(bah_table, className='table table-sm table-bordered performance-table')
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

    def create_2d_parameter_heatmap(self, analysis: Dict[str, Any], metric: str, data: Dict[str, Any]) -> html.Div:
        """
        創建2D參數高原熱力圖
        
        Args:
            analysis: 策略參數分析結果
            metric: 績效指標名稱
            data: 完整數據字典
            
        Returns:
            html.Div: 包含圖表的組件
        """
        try:
            import plotly.graph_objs as go
            import plotly.express as px
            import numpy as np
            import pandas as pd
            
            # 檢查是否有足夠的可變參數
            variable_params = analysis.get('variable_params', {})
            if len(variable_params) < 2:
                return html.P("需要至少2個可變參數才能生成圖表", className="text-warning")
            
            # 自動選擇前兩個可變參數作為X和Y軸
            param_names = list(variable_params.keys())
            x_axis = param_names[0]
            y_axis = param_names[1]
            
            # 獲取參數值列表
            x_values = variable_params[x_axis]
            y_values = variable_params[y_axis]
            
            # 創建績效指標矩陣
            performance_matrix = []
            valid_data_points = 0
            total_data_points = 0
            
            for y_val in y_values:
                row = []
                for x_val in x_values:
                    # 查找對應的績效指標值
                    performance_value = self._find_performance_value(analysis, x_axis, x_val, y_axis, y_val, metric, data)
                    row.append(performance_value)
                    
                    total_data_points += 1
                    if performance_value is not None and not np.isnan(performance_value):
                        valid_data_points += 1
                
                performance_matrix.append(row)
            
            # 轉換為numpy數組
            performance_array = np.array(performance_matrix)
            
            # 檢查是否有有效數據
            if valid_data_points == 0:
                return html.P(f"沒有找到有效的 {metric} 數據", className="text-warning")
            
            # 過濾掉完全為 nan 的行和列
            valid_rows = []
            valid_cols = []
            
            # 檢查每行是否有有效數據
            for i, row in enumerate(performance_matrix):
                if any(val is not None and not np.isnan(val) for val in row):
                    valid_rows.append(i)
            
            # 檢查每列是否有有效數據
            for j in range(len(performance_matrix[0])):
                if any(performance_matrix[i][j] is not None and not np.isnan(performance_matrix[i][j]) for i in range(len(performance_matrix))):
                    valid_cols.append(j)
            
            if not valid_rows or not valid_cols:
                return html.P(f"沒有找到有效的 {metric} 數據組合", className="text-warning")
            
            # 創建過濾後的數據
            filtered_x_values = [x_values[j] for j in valid_cols]
            filtered_y_values = [y_values[i] for i in valid_rows]
            filtered_matrix = [[performance_matrix[i][j] for j in valid_cols] for i in valid_rows]
            
            # 轉換為numpy數組
            filtered_array = np.array(filtered_matrix)
            
            # 創建熱力圖
            fig = go.Figure(data=go.Heatmap(
                z=filtered_array,
                x=filtered_x_values,
                y=filtered_y_values,
                colorscale=[
                    [0, 'rgba(0,0,0,0)'],      # 無色（透明）
                    [0.3, '#8f1511'],            # 副主題紅色
                    [1, '#dbac30']               # 主題金色
                ],
                text=[[f"{val:.2f}" if val is not None and not np.isnan(val) else "N/A" for val in row] for row in filtered_matrix],
                texttemplate="%{text}",
                textfont={"size": 12, "color": "white"},
                hoverongaps=False,
                hovertemplate=f"<b>{x_axis}</b>: %{{x}}<br>" +
                             f"<b>{y_axis}</b>: %{{y}}<br>" +
                             f"<b>{metric}</b>: %{{z:.2f}}<extra></extra>"
            ))
            
            # 更新布局
            fig.update_layout(
                title=f"{metric} 參數高原圖表 (X: {x_axis}, Y: {y_axis}) - {valid_data_points}/{total_data_points} 有效數據點",
                xaxis_title=x_axis,
                yaxis_title=y_axis,
                template=None,
                height=600,
                plot_bgcolor="#181818",
                paper_bgcolor="#181818",
                font=dict(color="#f5f5f5", size=14),
                xaxis=dict(
                    color="#ecbc4f",
                    gridcolor="#444",
                    tickfont=dict(color="#ecbc4f")
                ),
                yaxis=dict(
                    color="#ecbc4f",
                    gridcolor="#444",
                    tickfont=dict(color="#ecbc4f")
                ),
                title_font=dict(color="#ecbc4f", size=18)
            )
            
            # 創建圖表組件
            chart_component = dcc.Graph(
                id='parameter-heatmap',
                figure=fig,
                config={'displayModeBar': True, 'displaylogo': False}
            )
            
            return chart_component
            
        except Exception as e:
            return html.P(f"創建圖表失敗: {str(e)}", className="text-danger")
    
    def _find_performance_value(self, analysis: Dict[str, Any], x_axis: str, x_val: str, y_axis: str, y_val: str, metric: str, data: Dict[str, Any]) -> float:
        """
        查找特定參數組合的績效指標值
        
        Args:
            analysis: 策略參數分析結果
            x_axis: X軸參數名
            x_val: X軸參數值
            y_axis: Y軸參數值
            y_val: Y軸參數值
            metric: 績效指標名稱
            data: 完整數據字典
            
        Returns:
            float: 績效指標值，如果找不到則返回None
        """
        try:
            parameters = data.get('parameters', [])
            parameter_indices = analysis['parameter_indices']
            
            for idx in parameter_indices:
                param = parameters[idx]
                match = True
                
                # 檢查X軸參數
                if not self._check_param_match(param, x_axis, x_val):
                    match = False
                
                # 檢查Y軸參數
                if not self._check_param_match(param, y_axis, y_val):
                    match = False
                
                if match:
                    # 找到匹配的參數組合，返回績效指標值
                    return self._extract_metric_value(param, metric)
            
            return None
            
        except Exception as e:
            return None
    
    def _check_param_match(self, param: Dict[str, Any], param_name: str, param_value: str) -> bool:
        """檢查參數是否匹配"""
        try:
            # 檢查 Entry_params
            if 'Entry_params' in param:
                for entry_param in param['Entry_params']:
                    if entry_param.get(param_name) == param_value:
                        return True
            
            # 檢查 Exit_params
            if 'Exit_params' in param:
                for exit_param in param['Exit_params']:
                    if exit_param.get(param_name) == param_value:
                        return True
            
            return False
            
        except Exception:
            return False
    
    def _extract_metric_value(self, param: Dict[str, Any], metric: str) -> float:
        """提取績效指標值"""
        try:
            if metric == "Max_drawdown":
                metric_key = "Max_drawdown"
            else:
                metric_key = metric
            
            if metric_key in param:
                value = param[metric_key]
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            return None
            
        except Exception:
            return None 