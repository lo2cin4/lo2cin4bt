#!/usr/bin/env python3
"""
SwitchDataSource_autorunner.py

【功能說明】
------------------------------------------------------------
本模組為 Lo2cin4BT autorunner 數據源切換工具，提供快速切換配置文件中數據源的功能，
支援 Yahoo Finance、Binance、Coinbase API 和本地文件四種數據源。

【使用方法】
------------------------------------------------------------
python autorunner/SwitchDataSource_autorunner.py yfinance
python autorunner/SwitchDataSource_autorunner.py binance
python autorunner/SwitchDataSource_autorunner.py coinbase
python autorunner/SwitchDataSource_autorunner.py file

【維護與擴充重點】
------------------------------------------------------------
- 新增數據源時，請同步更新 valid_sources 列表和對應的配置處理邏輯
- 若配置文件結構有變動，需同步更新本模組的讀取和寫入邏輯
- 錯誤處理和用戶提示信息如有調整，請保持一致性

【常見易錯點】
------------------------------------------------------------
- 配置文件路徑錯誤會導致切換失敗
- 無效的數據源名稱會導致程序異常
- JSON 格式錯誤會導致配置文件損壞

【與其他模組的關聯】
------------------------------------------------------------
- 依賴 records/autorunner/config_template.json 配置文件
- 為 autorunner 系統提供數據源切換功能
- 與 DataLoader_autorunner.py 協同工作
"""

import json
import sys
from pathlib import Path


def switch_data_source(source: str) -> bool:
    """切換配置文件中的數據源"""
    config_path = Path("records/autorunner/config_template.json")

    if not config_path.exists():
        print("❌ 配置文件不存在: records/autorunner/config_template.json")
        return False

    # 讀取配置文件
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    # 驗證數據源
    valid_sources = ["yfinance", "binance", "coinbase", "file"]
    if source not in valid_sources:
        print(f"❌ 無效的數據源: {source}")
        print(f"✅ 有效的數據源: {', '.join(valid_sources)}")
        return False

    # 切換數據源
    config["dataloader"]["source"] = source

    # 保存配置文件
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"✅ 數據源已切換為: {source}")
    print(f"📁 配置文件: {config_path}")

    # 顯示對應的配置信息
    source_configs = {
        "yfinance": config["dataloader"].get("yfinance_config", {}),
        "binance": config["dataloader"].get("binance_config", {}),
        "coinbase": config["dataloader"].get("coinbase_config", {}),
        "file": config["dataloader"].get("file_config", {}),
    }

    print("🔧 當前配置:")
    for key, value in source_configs[source].items():
        print(f"   {key}: {value}")

    return True


def main() -> None:
    """主函數：切換數據源"""
    if len(sys.argv) != 2:
        print("使用方法: python switch_data_source.py <數據源>")
        print("數據源選項: yfinance, binance, coinbase, file")
        return

    source = sys.argv[1].lower()
    switch_data_source(source)


if __name__ == "__main__":
    main()
