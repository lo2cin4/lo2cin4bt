"""
參數解析工具模組

統一處理 plotter 模組中的參數解析邏輯，避免代碼重複。
"""

from typing import Any, Dict, List, Tuple


class ParameterParser:
    """參數解析器，統一處理各種參數解析邏輯"""

    @staticmethod
    def parse_all_parameters(parameters: list) -> dict:
        """
        動態展開所有 Entry_params/Exit_params，回傳 {參數名: [所有值]}，同一參數只列一次。

        Args:
            parameters: 參數列表

        Returns:
            dict: 參數名到值列表的映射
        """
        param_values = {}
        for param in parameters:
            for key in ["Entry_params", "Exit_params"]:
                if key in param and isinstance(param[key], list):
                    for d in param[key]:
                        if isinstance(d, dict):
                            for k, v in d.items():
                                if k not in param_values:
                                    param_values[k] = set()
                                param_values[k].add(str(v))

        # 轉成 list 並排序
        for k in param_values:
            param_values[k] = sorted(list(param_values[k]))
        return param_values

    @staticmethod
    def parse_entry_exit_parameters(
        parameters: list,
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        分別展開 Entry_params/Exit_params，回傳 (entry_param_values, exit_param_values)

        Args:
            parameters: 參數列表

        Returns:
            Tuple[Dict[str, List], Dict[str, List]]: Entry和Exit參數值字典
        """
        entry_param_values = {}
        exit_param_values = {}

        for param in parameters:
            if "Entry_params" in param and isinstance(param["Entry_params"], list):
                for d in param["Entry_params"]:
                    if isinstance(d, dict):
                        for k, v in d.items():
                            if k not in entry_param_values:
                                entry_param_values[k] = set()
                            entry_param_values[k].add(str(v))

            if "Exit_params" in param and isinstance(param["Exit_params"], list):
                for d in param["Exit_params"]:
                    if isinstance(d, dict):
                        for k, v in d.items():
                            if k not in exit_param_values:
                                exit_param_values[k] = set()
                            exit_param_values[k].add(str(v))

        # 轉成 list 並排序
        for k in entry_param_values:
            entry_param_values[k] = sorted(list(entry_param_values[k]))
        for k in exit_param_values:
            exit_param_values[k] = sorted(list(exit_param_values[k]))

        return entry_param_values, exit_param_values

    @staticmethod
    def parse_indicator_param_structure(
        parameters: list,
    ) -> Dict[str, Dict[str, Dict[str, List]]]:
        """
        統計所有 entry/exit 下 indicator_type 及其所有參數名與值

        Args:
            parameters: 參數列表

        Returns:
            Dict: 結構化的指標參數信息
        """
        result = {"entry": {}, "exit": {}}

        for param in parameters:
            for key, target in [("Entry_params", "entry"), ("Exit_params", "exit")]:
                if key in param and isinstance(param[key], list):
                    for d in param[key]:
                        if isinstance(d, dict):
                            indicator_type = str(d.get("indicator_type", "Unknown"))
                            if indicator_type not in result[target]:
                                result[target][indicator_type] = {}

                            for k, v in d.items():
                                if k == "indicator_type":
                                    continue
                                if k not in result[target][indicator_type]:
                                    result[target][indicator_type][k] = set()
                                result[target][indicator_type][k].add(str(v))

        # 轉成 list 並排序
        for target in result:
            for ind in result[target]:
                for k in result[target][ind]:
                    result[target][ind][k] = sorted(list(result[target][ind][k]))

        return result

    @staticmethod
    def identify_strategy_groups(parameters: list) -> Dict[str, Any]:
        """
        識別策略分組，基於 Entry_params 和 Exit_params 的 indicator_type + strat_idx 組合

        Args:
            parameters: 參數列表

        Returns:
            Dict[str, Any]: 策略分組信息
        """
        strategy_groups = {}

        for i, param in enumerate(parameters):
            entry_strategies = []
            exit_strategies = []

            # 提取 Entry 策略信息
            if "Entry_params" in param and isinstance(param["Entry_params"], list):
                for entry_param in param["Entry_params"]:
                    if isinstance(entry_param, dict):
                        indicator_type = entry_param.get("indicator_type", "Unknown")
                        strat_idx = entry_param.get("strat_idx", "Unknown")
                        strategy_name = f"{indicator_type}{strat_idx}"
                        entry_strategies.append(
                            {
                                "indicator_type": indicator_type,
                                "strat_idx": strat_idx,
                                "strategy_name": strategy_name,
                                "full_params": entry_param,
                            }
                        )

            # 提取 Exit 策略信息
            if "Exit_params" in param and isinstance(param["Exit_params"], list):
                for exit_param in param["Exit_params"]:
                    if isinstance(exit_param, dict):
                        indicator_type = exit_param.get("indicator_type", "Unknown")
                        strat_idx = exit_param.get("strat_idx", "Unknown")
                        strategy_name = f"{indicator_type}{strat_idx}"
                        exit_strategies.append(
                            {
                                "indicator_type": indicator_type,
                                "strat_idx": strat_idx,
                                "strategy_name": strategy_name,
                                "full_params": exit_param,
                            }
                        )

            # 創建策略組合鍵
            if entry_strategies and exit_strategies:
                # 排序策略名稱以確保一致性
                entry_names = sorted([s["strategy_name"] for s in entry_strategies])
                exit_names = sorted([s["strategy_name"] for s in exit_strategies])

                strategy_key = (
                    f"Entry_{'+'.join(entry_names)}_Exit_{'+'.join(exit_names)}"
                )

                if strategy_key not in strategy_groups:
                    strategy_groups[strategy_key] = {
                        "entry_strategies": entry_strategies,
                        "exit_strategies": exit_strategies,
                        "entry_names": entry_names,
                        "exit_names": exit_names,
                        "parameter_combinations": [],
                        "count": 0,
                        "display_name": f"Entry: {', '.join(entry_names)} | Exit: {', '.join(exit_names)}",
                    }

                # 添加參數組合
                strategy_groups[strategy_key]["parameter_combinations"].append(i)
                strategy_groups[strategy_key]["count"] += 1

        return strategy_groups

    @staticmethod
    def parse_parameter_value(value: Any) -> List[Any]:
        """
        解析參數值，識別並處理不同類型：
        1. 範圍值：如 "10:20:10" -> 解析為實際數值列表
        2. 逗號分隔值：如 "2,2.5,3" -> 解析為數值列表
        3. 單一數值：直接轉換為數值

        Args:
            value: 參數值

        Returns:
            List[Any]: 解析後的參數值列表
        """
        if isinstance(value, str):
            # 檢查是否為範圍值 (start:end:step)
            if ":" in value and value.count(":") == 2:
                try:
                    parts = value.split(":")
                    start, end, step = map(int, parts)
                    if start < end and step > 0:
                        # 生成範圍內的數值列表
                        return list(range(start, end + 1, step))
                except (ValueError, TypeError):
                    pass

            # 檢查是否為逗號分隔值
            if "," in value:
                try:
                    # 嘗試轉換為浮點數列表
                    return [float(x.strip()) for x in value.split(",")]
                except (ValueError, TypeError):
                    pass

            # 嘗試轉換為單一數值
            try:
                if "." in value:
                    return [float(value)]
                else:
                    return [int(value)]
            except (ValueError, TypeError):
                pass

        # 如果無法解析，返回原始值
        return [value]

    @staticmethod
    def analyze_strategy_parameters(
        parameters: list, strategy_key: str
    ) -> Dict[str, Any]:
        """
        分析選中策略的可變參數，用於生成2D參數高原圖表

        支持多指標策略和動態參數識別

        Args:
            parameters: 參數列表
            strategy_key: 選中的策略鍵

        Returns:
            Dict[str, Any]: 參數分析結果
        """
        # 首先識別策略分組
        strategy_groups = ParameterParser.identify_strategy_groups(parameters)

        if strategy_key not in strategy_groups:
            return {}

        strategy_info = strategy_groups[strategy_key]
        parameter_indices = strategy_info["parameter_combinations"]

        # 獲取該策略的所有參數組合
        strategy_parameters = [parameters[i] for i in parameter_indices]

        # 分析可變參數（支持多指標策略）
        variable_params = {}
        fixed_params = {}

        # 收集 Entry 和 Exit 參數的鍵和值
        entry_param_values = {}
        exit_param_values = {}

        for param in strategy_parameters:
            # 分析 Entry_params（支持多個Entry指標）
            if "Entry_params" in param:
                for entry_param in param["Entry_params"]:
                    # 獲取指標類型和索引，用於區分不同指標的參數
                    indicator_type = entry_param.get("indicator_type", "Unknown")
                    strat_idx = entry_param.get("strat_idx", "Unknown")
                    indicator_key = f"{indicator_type}{strat_idx}"

                    for key, value in entry_param.items():
                        if key not in ["indicator_type", "strat_idx"]:  # 排除固定字段
                            # 創建唯一的參數鍵，包含指標信息
                            entry_key = f"Entry_{indicator_key}_{key}"
                            if entry_key not in entry_param_values:
                                entry_param_values[entry_key] = set()

                            # 解析參數值
                            parsed_values = ParameterParser.parse_parameter_value(value)
                            for parsed_val in parsed_values:
                                entry_param_values[entry_key].add(parsed_val)

            # 分析 Exit_params（支持多個Exit指標）
            if "Exit_params" in param:
                for exit_param in param["Exit_params"]:
                    # 獲取指標類型和索引，用於區分不同指標的參數
                    indicator_type = exit_param.get("indicator_type", "Unknown")
                    strat_idx = exit_param.get("strat_idx", "Unknown")
                    indicator_key = f"{indicator_type}{strat_idx}"

                    for key, value in exit_param.items():
                        if key not in ["indicator_type", "strat_idx"]:  # 排除固定字段
                            # 創建唯一的參數鍵，包含指標信息
                            exit_key = f"Exit_{indicator_key}_{key}"
                            if exit_key not in exit_param_values:
                                exit_param_values[exit_key] = set()

                            # 解析參數值
                            parsed_values = ParameterParser.parse_parameter_value(value)
                            for parsed_val in parsed_values:
                                exit_param_values[exit_key].add(parsed_val)

        # 合併 Entry 和 Exit 參數
        all_param_values = {**entry_param_values, **exit_param_values}

        # 判斷參數是否可變（有多個值）
        for key in all_param_values:
            if len(all_param_values[key]) > 1:
                # 轉換為排序的數值列表
                try:
                    sorted_values = sorted(
                        all_param_values[key],
                        key=lambda x: (
                            float(x) if isinstance(x, (int, float, str)) else 0
                        ),
                    )
                    variable_params[key] = sorted_values
                except (ValueError, TypeError):
                    # 如果無法排序，使用原始順序
                    variable_params[key] = list(all_param_values[key])
            else:
                fixed_params[key] = list(all_param_values[key])[0]

        # 轉換為排序列表
        variable_param_list = sorted(list(variable_params.keys()))

        return {
            "strategy_key": strategy_key,
            "strategy_info": strategy_info,
            "variable_params": variable_params,
            "fixed_params": fixed_params,
            "variable_param_list": variable_param_list,
            "total_combinations": len(strategy_parameters),
            "parameter_indices": parameter_indices,
        }
