"""
SpecMonitor_backtester.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT 回測框架的「系統規格監控器」，負責：
- 智能CPU配置檢測與並行處理閾值計算
- 記憶體安全檢查與閾值管理
- 配置信息收集與系統資源監控
- 向量化性能監控與優化建議
- 跨平台系統適配與資源管理

【主要功能】
------------------------------------------------------------
- 根據系統硬體自動優化處理參數
- 防止記憶體溢出和系統崩潰
- 提供智能並行處理建議
- 實時監控系統資源使用
- 支援跨平台系統檢測與配置

【流程與數據流】
------------------------------------------------------------
- 由 VectorBacktestEngine 調用，提供系統資源監控
- 提供靜態方法供其他模組使用
- 支援自適應配置調整

```mermaid
flowchart TD
    A[VectorBacktestEngine] -->|調用| B[SpecMonitor]
    B -->|CPU檢測| C[get_optimal_core_count]
    B -->|記憶體檢測| D[check_memory_safety]
    B -->|配置收集| E[collect_config_info]
    B -->|資源監控| F[get_memory_usage]
    C & D & E & F -->|優化建議| G[BacktestEngine]
```

【監控功能】
------------------------------------------------------------
- CPU核心數檢測與並行處理優化
- 記憶體使用量監控與安全閾值管理
- 系統配置信息收集與顯示
- 向量化性能監控與警告
- 跨平台兼容性檢測

【維護與擴充重點】
------------------------------------------------------------
- 確保跨平台兼容性（Windows、Linux、macOS）
- 優化記憶體使用效率與安全閾值
- 提供準確的系統檢測與配置建議
- 支援新的硬體配置與系統環境
- 監控邏輯需要與向量化引擎配合

【常見易錯點】
------------------------------------------------------------
- 跨平台系統檢測不準確
- 記憶體閾值設置不當導致系統崩潰
- CPU核心數檢測錯誤影響並行性能
- 系統資源監控不及時
- 配置建議與實際需求不匹配

【錯誤處理】
------------------------------------------------------------
- 系統檢測失敗時提供默認配置
- 記憶體不足時提供優化建議
- 跨平台兼容性問題時提供備用方案
- 監控異常時提供診斷信息

【範例】
------------------------------------------------------------
- 獲取最優CPU核心數：cores, desc = SpecMonitor.get_optimal_core_count()
- 檢查記憶體安全性：status = SpecMonitor.check_memory_safety(n_tasks)
- 收集配置信息：config_info = SpecMonitor.collect_config_info(n_tasks)
- 獲取記憶體使用量：memory_used = SpecMonitor.get_memory_usage()
- 顯示向量化監控：SpecMonitor.display_vectorization_monitor(initial_memory, console)

【與其他模組的關聯】
------------------------------------------------------------
- 由 VectorBacktestEngine 調用，提供系統資源監控
- 與 Rich 模組配合提供美化顯示
- 支援 psutil 模組進行系統檢測
- 與其他模組共享系統配置信息

【版本與變更記錄】
------------------------------------------------------------
- v1.0: 初始版本，基本系統檢測功能
- v1.1: 新增記憶體安全檢查
- v1.2: 完善CPU配置優化
- v2.0: 整合向量化性能監控
- v2.1: 新增跨平台兼容性支援
- v2.2: 完善錯誤處理與優化建議

【參考】
------------------------------------------------------------
- psutil 官方文檔：https://psutil.readthedocs.io/
- Rich 官方文檔：https://rich.readthedocs.io/
- 系統資源監控最佳實踐
- 跨平台開發與兼容性設計
"""

import logging
import multiprocessing
from typing import Any, Dict, List, Optional, Tuple

from rich.console import Console
from rich.panel import Panel

# 嘗試導入 psutil
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


class SpecMonitor:
    """系統規格監控器 - 負責系統資源檢測和配置優化"""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("SpecMonitor")
        self.max_memory_mb = 1000  # 最大記憶體使用量（MB）

    @staticmethod
    def display_vectorization_monitor(initial_memory: float, console: Console) -> None:
        """顯示向量化性能監控面板"""
        config_panel_content = (
            f"🚀 開始向量化回測...\n📊 初始記憶體使用: {initial_memory:.1f} MB"
        )

        console.print(
            Panel(
                config_panel_content,
                title="[bold #8f1511]⚡ 向量化性能監控[/bold #8f1511]",
                border_style="#dbac30",
            )
        )

    @staticmethod
    def display_config_info(config_info: List[str], console: Console) -> None:
        """顯示智能配置信息面板"""
        if config_info:
            # 過濾掉空字符串和只包含空格的字符串
            filtered_config = []
            for item in config_info:
                if item.strip():
                    # 清理換行符，但保持每個項目獨立
                    cleaned_item = item.strip().replace("\n", " ")
                    filtered_config.append(cleaned_item)

            # 使用換行符連接，讓每個配置項目換行顯示
            config_text = "\n".join(filtered_config)
            config_explanation = """[bold #dbac30]說明：[/bold #dbac30]
• 智能配置檢測根據您的CPU核心數和記憶體大小自動優化處理參數
• 並行處理閾值確保在安全範圍內最大化利用多核處理能力
• 記憶體安全檢查防止系統資源不足導致程序異常終止
• 批次配置優化確保大量任務能夠高效且穩定地完成處理"""

            console.print(
                Panel(
                    config_text + "\n\n" + config_explanation,
                    title="[bold #8f1511]🔧 智能檢測電腦配置[/bold #8f1511]",
                    border_style="#dbac30",
                )
            )

    @staticmethod
    def display_memory_warning(memory_used: float, console: Console) -> None:
        """顯示記憶體警告面板"""
        memory_warning = f"⚠️ 記憶體使用過高: {memory_used:.1f} MB，強制垃圾回收"
        memory_explanation = """[bold #dbac30]說明：[/bold #dbac30]
• 記憶體使用超過安全閾值，系統自動執行垃圾回收以釋放記憶體
• 這是正常的保護機制，確保程序穩定運行
• 如果頻繁出現此警告，建議減少回測參數組合數量或關閉其他程序"""

        console.print(
            Panel(
                memory_warning + "\n\n" + memory_explanation,
                title="[bold #dbac30]💾 記憶體管理[/bold #dbac30]",
                border_style="#dbac30",
            )
        )

    @staticmethod
    def get_optimal_core_count() -> Tuple[int, str]:
        """智能檢測最佳CPU核心數 - 適應不同用戶配置"""
        total_cores = multiprocessing.cpu_count()

        # 嘗試檢測系統記憶體
        try:
            if PSUTIL_AVAILABLE:
                available_memory_gb = psutil.virtual_memory().available / (1024**3)
                total_memory_gb = psutil.virtual_memory().total / (1024**3)

                # 根據記憶體配置調整
                if total_memory_gb < 4.0:
                    # 低記憶體配置（<4GB）：保守使用
                    if total_cores <= 2:
                        optimal_cores = 1
                    else:
                        optimal_cores = min(2, total_cores - 1)
                    config_info = (
                        f"💾 低記憶體配置檢測: 總記憶體={total_memory_gb:.1f}GB, "
                        f"可用記憶體={available_memory_gb:.1f}GB，"
                        f"🔧 使用保守配置: {optimal_cores}/{total_cores} 核心"
                    )

                elif available_memory_gb < 2.0:
                    # 可用記憶體不足（<2GB）：進一步限制
                    optimal_cores = max(1, min(2, total_cores // 2))
                    config_info = f"⚠️ 可用記憶體不足: {available_memory_gb:.1f}GB，🔧 限制並行處理: {optimal_cores}/{total_cores} 核心"

                elif total_cores <= 2:
                    # 低配置CPU（≤2核）：單核處理
                    optimal_cores = 1
                    config_info = f"🖥️ CPU檢測: {total_cores} 核心，🔧 使用單核處理: {optimal_cores}/{total_cores} 核心"

                elif total_cores <= 4:
                    # 中等配置CPU（3-4核）：保留1個核心給系統
                    optimal_cores = total_cores - 1
                    config_info = f"🖥️ CPU檢測: {total_cores} 核心，🔧 保留系統核心: {optimal_cores}/{total_cores} 核心"

                elif total_cores <= 8:
                    # 高配置CPU（5-8核）：保留1-2個核心給系統
                    optimal_cores = min(total_cores - 1, 6)
                    config_info = f"🖥️ CPU檢測: {total_cores} 核心，🔧 效能配置: {optimal_cores}/{total_cores} 核心"

                else:
                    # 超高配置CPU（>8核）：最多使用8個核心
                    optimal_cores = min(total_cores - 2, 8)
                    config_info = f"🖥️ CPU檢測: {total_cores} 核心，🔧 效能配置: {optimal_cores}/{total_cores} 核心"

            else:
                # 無法檢測記憶體，使用保守配置
                if total_cores <= 2:
                    optimal_cores = 1
                elif total_cores <= 4:
                    optimal_cores = min(2, total_cores - 1)
                else:
                    optimal_cores = min(4, total_cores - 1)
                config_info = f"⚠️ 無法檢測記憶體，使用保守配置: {optimal_cores}/{total_cores} 核心"

        except ImportError:
            # 無法檢測記憶體，使用保守配置
            if total_cores <= 2:
                optimal_cores = 1
            elif total_cores <= 4:
                optimal_cores = min(2, total_cores - 1)
            else:
                optimal_cores = min(4, total_cores - 1)
            config_info = (
                f"⚠️ 無法檢測記憶體，使用保守配置: {optimal_cores}/{total_cores} 核心"
            )
        except Exception as e:
            # 其他錯誤，使用保守配置
            print(f"⚠️ 記憶體檢測出現異常: {e}，使用保守配置")
            if total_cores <= 2:
                optimal_cores = 1
            elif total_cores <= 4:
                optimal_cores = min(2, total_cores - 1)
            else:
                optimal_cores = min(4, total_cores - 1)
            config_info = (
                f"⚠️ 記憶體檢測異常，使用保守配置: {optimal_cores}/{total_cores} 核心"
            )

        return optimal_cores, config_info

    @staticmethod
    def get_serial_threshold() -> Tuple[int, str]:
        """
        根據CPU核心數和記憶體動態計算並行處理閾值

        Returns:
            Tuple[int, str]: (閾值, 配置說明)
        """
        total_cores = multiprocessing.cpu_count()

        try:
            if PSUTIL_AVAILABLE:
                total_memory_gb = psutil.virtual_memory().total / (1024**3)

                # 根據記憶體大小調整閾值
                if total_memory_gb >= 32:
                    # 32GB+ 高配置：可以處理更多任務
                    base_threshold = 50000
                    memory_multiplier = 1.5
                elif total_memory_gb >= 16:
                    # 16GB 中高配置
                    base_threshold = 30000
                    memory_multiplier = 1.2
                elif total_memory_gb >= 8:
                    # 8GB 中等配置
                    base_threshold = 15000
                    memory_multiplier = 1.0
                else:
                    # 低配置：保守處理
                    base_threshold = 8000
                    memory_multiplier = 0.8

                # 根據CPU核心數調整
                if total_cores >= 8:
                    core_multiplier = 1.5
                elif total_cores >= 4:
                    core_multiplier = 1.2
                else:
                    core_multiplier = 1.0

                threshold = int(base_threshold * memory_multiplier * core_multiplier)
                config_info = f"⚡ 智能閾值計算: 記憶體{total_memory_gb:.1f}GB, CPU{total_cores}核, 並行閾值={threshold}"

                return threshold, config_info

            else:
                # 無法檢測記憶體，使用保守配置
                if total_cores >= 4:
                    threshold = 15000
                else:
                    threshold = 8000
                config_info = f"⚠️ 無法檢測記憶體，使用保守配置: 並行閾值={threshold}"
                return threshold, config_info

        except ImportError:
            # 無法檢測記憶體，使用保守配置
            if total_cores >= 4:
                threshold = 15000
            else:
                threshold = 8000
            config_info = f"⚠️ 無法檢測記憶體，使用保守配置: 並行閾值={threshold}"
            return threshold, config_info
        except Exception as e:
            # 其他錯誤，使用保守配置
            print(f"⚠️ 閾值計算出現異常: {e}，使用保守配置")
            if total_cores >= 4:
                threshold = 15000
            else:
                threshold = 8000
            config_info = f"⚠️ 閾值計算異常，使用保守配置: 並行閾值={threshold}"
            return threshold, config_info

    @staticmethod
    def check_memory_safety(n_tasks: int) -> str:
        """檢查記憶體安全性，返回檢查結果信息"""
        try:
            if PSUTIL_AVAILABLE:
                available_memory_gb = psutil.virtual_memory().available / (1024**3)
                total_memory_gb = psutil.virtual_memory().total / (1024**3)

                # 估算記憶體需求（向量化處理更高效，每個任務約需要0.1-0.2MB）
                estimated_memory_mb = n_tasks * 0.15  # 更合理的估算
                estimated_memory_gb = estimated_memory_mb / 1024

                # 根據總記憶體動態調整警告閾值
                if total_memory_gb >= 32:
                    # 32GB+ 高配置：更寬鬆的閾值
                    warning_threshold = 0.8  # 80%
                    critical_threshold = 0.95  # 95%
                elif total_memory_gb >= 16:
                    # 16GB 中高配置
                    warning_threshold = 0.85  # 85%
                    critical_threshold = 0.95  # 95%
                else:
                    # 低配置：更嚴格的閾值
                    warning_threshold = 0.9  # 90%
                    critical_threshold = 0.95  # 95%

                # 返回記憶體檢查結果
                memory_info = f"💾 記憶體檢查: 估算需求 {estimated_memory_gb:.1f}GB，可用記憶體{available_memory_gb:.1f}GB"

                # 如果估算記憶體需求超過警告閾值，發出警告
                if estimated_memory_gb > available_memory_gb * warning_threshold:
                    print(
                        f"⚠️ 記憶體警告: 估算需求 {estimated_memory_gb:.1f}GB，可用記憶體 {available_memory_gb:.1f}GB"
                    )
                    print("⚠️ 建議減少任務數量或關閉其他程序")
                    memory_info += (
                        f" ⚠️ 記憶體警告: 超過 {warning_threshold * 100:.0f}% 閾值"
                    )

                    # 如果超過臨界閾值，強制使用串行處理
                    if estimated_memory_gb > available_memory_gb * critical_threshold:
                        print("🛑 記憶體嚴重不足，嘗試優化策略...")
                        memory_info += " 🛑 記憶體嚴重不足"

                        # 策略1: 強制垃圾回收
                        import gc

                        gc.collect()

                        # 重新檢查記憶體
                        available_memory_gb_after_gc = (
                            psutil.virtual_memory().available / (1024**3)
                        )
                        memory_freed = (
                            available_memory_gb_after_gc - available_memory_gb
                        )

                        print(f"🔄 垃圾回收完成，釋放 {memory_freed:.1f}GB 記憶體")
                        memory_info += f" 🔄 垃圾回收釋放 {memory_freed:.1f}GB"

                        # 如果垃圾回收後仍然不足，才強制串行處理
                        if (
                            estimated_memory_gb
                            > available_memory_gb_after_gc * critical_threshold
                        ):
                            print("🛑 記憶體仍然不足，強制使用串行處理")
                            memory_info += " 🛑 強制串行處理"
                            raise MemoryError(
                                "記憶體不足，建議減少任務數量或關閉其他程序"
                            )
                        else:
                            print("✅ 垃圾回收後記憶體充足，繼續並行處理")
                            memory_info += " ✅ 垃圾回收後充足"

                return memory_info

            else:
                # 無法檢測記憶體，跳過檢查
                return "⚠️ 無法檢測記憶體，跳過檢查"

        except ImportError:
            # 無法檢測記憶體，跳過檢查
            return "⚠️ 無法檢測記憶體，跳過檢查"
        except Exception as e:
            # 其他錯誤，記錄但不中斷
            print(f"⚠️ 記憶體檢查出現異常: {e}，跳過檢查")
            return f"⚠️ 記憶體檢查異常: {e}"

    @staticmethod
    def get_memory_thresholds() -> Dict[str, float]:
        """根據系統記憶體動態計算記憶體監控閾值"""
        try:
            if PSUTIL_AVAILABLE:
                total_memory_gb = psutil.virtual_memory().total / (1024**3)

                # 根據總記憶體動態調整閾值，基於實際記憶體容量的百分比
                if total_memory_gb >= 32:
                    # 32GB+ 高配置：更寬鬆的閾值
                    warning_mb = total_memory_gb * 1024 * 0.25  # 25% of total memory
                    critical_mb = total_memory_gb * 1024 * 0.50  # 50% of total memory
                    fatal_mb = total_memory_gb * 1024 * 0.75  # 75% of total memory
                elif total_memory_gb >= 16:
                    # 16GB 中高配置
                    warning_mb = total_memory_gb * 1024 * 0.30  # 30% of total memory
                    critical_mb = total_memory_gb * 1024 * 0.60  # 60% of total memory
                    fatal_mb = total_memory_gb * 1024 * 0.75  # 75% of total memory
                elif total_memory_gb >= 8:
                    # 8GB 中等配置
                    warning_mb = total_memory_gb * 1024 * 0.40  # 40% of total memory
                    critical_mb = total_memory_gb * 1024 * 0.65  # 65% of total memory
                    fatal_mb = total_memory_gb * 1024 * 0.75  # 75% of total memory
                else:
                    # 低配置：更嚴格的閾值
                    warning_mb = total_memory_gb * 1024 * 0.50  # 50% of total memory
                    critical_mb = total_memory_gb * 1024 * 0.70  # 70% of total memory
                    fatal_mb = total_memory_gb * 1024 * 0.75  # 75% of total memory

                return {
                    "warning": warning_mb,
                    "critical": critical_mb,
                    "fatal": fatal_mb,
                    "total_memory_gb": total_memory_gb,
                }

            else:
                # 無法檢測記憶體，使用保守配置
                return {
                    "warning": 1500,
                    "critical": 2500,
                    "fatal": 3500,
                    "total_memory_gb": 0,
                }

        except ImportError:
            # 無法檢測記憶體，使用保守配置
            return {
                "warning": 1500,
                "critical": 2500,
                "fatal": 3500,
                "total_memory_gb": 0,
            }
        except Exception as e:
            # 其他錯誤，使用保守配置
            print(f"⚠️ 記憶體閾值計算出現異常: {e}，使用保守配置")
            return {
                "warning": 1500,
                "critical": 2500,
                "fatal": 3500,
                "total_memory_gb": 0,
            }

    @staticmethod
    def collect_config_info(n_tasks: int) -> List[str]:
        """預先收集配置信息"""
        config_info = []

        try:
            # 智能任務數量閾值
            serial_threshold, threshold_info = SpecMonitor.get_serial_threshold()
            config_info.append(threshold_info)

            # 智能CPU配置檢測
            n_cores, core_info = SpecMonitor.get_optimal_core_count()
            config_info.append(core_info)

            # 記憶體安全檢查
            memory_check_result = SpecMonitor.check_memory_safety(n_tasks)
            if memory_check_result and memory_check_result.strip():
                config_info.append(memory_check_result)

        except Exception as e:
            config_info.append(f"⚠️ 配置信息收集失敗: {e}")

        return config_info

    @staticmethod
    def get_memory_usage() -> float:
        """獲取當前記憶體使用量（MB）"""
        try:
            if PSUTIL_AVAILABLE:
                process = psutil.Process()
                return process.memory_info().rss / 1024 / 1024
            else:
                return 0.0
        except ImportError:
            return 0.0
        except Exception as e:
            print(f"⚠️ 記憶體使用量檢測失敗: {e}")
            return 0.0

    @staticmethod
    def get_system_info() -> Dict[str, Any]:
        """獲取系統完整信息"""
        system_info: Dict[str, Any] = {
            "cpu_cores": multiprocessing.cpu_count(),
            "psutil_available": PSUTIL_AVAILABLE,
        }

        if PSUTIL_AVAILABLE:
            try:
                memory = psutil.virtual_memory()
                system_info.update(
                    {
                        "total_memory_gb": memory.total / (1024**3),
                        "available_memory_gb": memory.available / (1024**3),
                        "memory_percent": memory.percent,
                    }
                )
            except Exception as e:
                system_info["memory_error"] = str(e)

        return system_info
