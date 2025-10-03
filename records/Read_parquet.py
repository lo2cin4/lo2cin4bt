import glob
import json
import os
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

# 新增：將所有 batch_metadata 與主表格輸出為 txt


def export_metadata_and_table_to_txt(
    batch_metadata: List[Dict[str, Any]],
    df: pd.DataFrame,
    out_dir: str,
    base_name: str,
) -> None:
    meta_txt = os.path.join(out_dir, f"{base_name}_metadata.txt")
    df_txt = os.path.join(out_dir, f"{base_name}_main_table.txt")
    # 輸出 batch_metadata
    with open(meta_txt, "w", encoding="utf-8") as f:
        for i, meta in enumerate(batch_metadata, 1):
            f.write(f"--- 策略 {i} ---\n")
            for k, v in meta.items():
                f.write(f"{k}: {v}\n")
            f.write("-------------------------------\n")
    # 輸出主表格
    df.to_string(buf=open(df_txt, "w", encoding="utf-8"), index=False)
    print(f"[ReadParquet] 已輸出所有 batch_metadata 至: {meta_txt}")
    print(f"[ReadParquet] 已輸出主表格至: {df_txt}")


# 設置 pandas 顯示選項以展示所有數據
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# 指定要讀取的 parquet 資料夾
parquet_dir = r"D:\Program files All Drive\Python code\Github_Strategy\lo2cin4bt\records\metricstracker"  # 可依實際路徑調整

# 列出所有 parquet 檔案
parquet_files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
if not parquet_files:
    print(f"[ReadParquet][ERROR] 資料夾 {parquet_dir} 下找不到 parquet 檔案！")
    exit(1)

print("[ReadParquet] 可選擇的 parquet 檔案：")
for i, f in enumerate(parquet_files, 1):
    print(f"  {i}. {os.path.basename(f)}")

file_input = (
    input(
        "請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選，預設為[1]）："
    ).strip()
    or "1"
)
try:
    idx = int(file_input) - 1
    assert 0 <= idx < len(parquet_files)
except Exception:
    print("[ReadParquet][ERROR] 輸入無效，預設選擇第一個檔案。")
    idx = 0
parquet_path = parquet_files[idx]

# 讀取 parquet
print(f"[ReadParquet] 讀取檔案: {parquet_path}")
df = pd.read_parquet(parquet_path)
table = pq.read_table(parquet_path)
meta = table.schema.metadata or {}

# 解析 batch_metadata
if b"batch_metadata" in meta:
    batch_metadata = json.loads(meta[b"batch_metadata"].decode())
    first_meta = batch_metadata[0]
    first_Backtest_id = first_meta.get("Backtest_id") or first_meta.get("Backtest_id")
    # 對齊輸出 batch_metadata
    maxlen = max(len(str(k)) for k in first_meta.keys())
    print("--- 第一個 batch_metadata 對齊輸出 ---")
    # 先找出所有數值型 value 的最大長度
    num_maxlen = max(
        len(str(v)) for v in first_meta.values() if isinstance(v, (int, float))
    )
    for k, v in first_meta.items():
        if isinstance(v, (int, float)):
            print(f"{str(k).ljust(maxlen)} | {str(v).rjust(num_maxlen)}")
        else:
            print(f"{str(k).ljust(maxlen)} | {str(v)}")
    # 匹配主表格
    if "Backtest_id" in df.columns:
        df_first = df[df["Backtest_id"] == first_Backtest_id]
    else:
        print("[ReadParquet][WARNING] 主表格缺少 Backtest_id 欄位，無法對應。")
        df_first = df
    # 輸出 csv
    out_dir = os.path.dirname(parquet_path)
    meta_csv = os.path.join(out_dir, "first_metadata.csv")
    df_csv = os.path.join(out_dir, "first_main_table.csv")
    pd.DataFrame([first_meta]).to_csv(meta_csv, index=False)
    df_first.to_csv(df_csv, index=False)
    print(f"[ReadParquet] 已輸出: {meta_csv}, {df_csv}")
    # 新增：全部 batch_metadata 與主表格 txt 輸出
    base_name = os.path.splitext(os.path.basename(parquet_path))[0]
    export_metadata_and_table_to_txt(batch_metadata, df, out_dir, base_name)
else:
    print("[ReadParquet][ERROR] 找不到 batch_metadata。")


def list_parquet_files() -> List[str]:
    """
    列出指定路徑內所有 .parquet 檔案

    Returns:
        包含所有 parquet 檔案路徑的列表，如果無檔案則返回空列表
    """
    # 指定搜尋路徑
    search_dir = (
        r"D:\Program files All Drive\Python code\lo2cin4bt\records\metricstracker"
    )
    print(f"\n當前搜尋目錄: {search_dir}")

    # 檢查目錄是否存在
    if not os.path.exists(search_dir):
        print(f"❌ 目錄不存在: {search_dir}")
        return []

    # 搜尋 .parquet 檔案
    parquet_files = glob.glob(os.path.join(search_dir, "*.parquet"))

    if not parquet_files:
        print(f"❌ 在 {search_dir} 中未找到任何 .parquet 檔案")
        return []

    print(f"\n✅ 找到以下 {len(parquet_files)} 個 Parquet 檔案:")
    for i, file in enumerate(parquet_files, 1):
        file_size = os.path.getsize(file) / 1024  # 檔案大小 (KB)
        print(f"  {i}. {os.path.basename(file)} ({file_size:.1f} KB)")

    return parquet_files


def read_parquet_with_metadata(
    file_path: str,
) -> tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    讀取 Parquet 檔案並提取 metadata

    Args:
        file_path: Parquet 檔案路徑

    Returns:
        (DataFrame, metadata_dict) 或 (None, None) 如果讀取失敗
    """
    try:
        # 使用 pandas 讀取 Parquet 檔案
        df = pd.read_parquet(file_path)

        # 提取 metadata
        table = pq.read_table(file_path)
        metadata = table.schema.metadata

        # 將 bytes 轉換為字串
        metadata_dict = {}
        for key, value in metadata.items():
            try:
                metadata_dict[key.decode("utf-8")] = value.decode("utf-8")
            except (AttributeError, UnicodeDecodeError):
                metadata_dict[key.decode("utf-8")] = str(value)

        return df, metadata_dict

    except Exception as e:
        print(f"❌ 讀取 Parquet 檔案時發生錯誤: {e}")
        return None, {}


def _calculate_column_widths(batch_list: List[Dict[str, Any]]) -> tuple[int, int]:
    """計算顯示欄位所需的最大寬度"""
    all_keys: set[str] = set()
    for meta in batch_list:
        all_keys.update(meta.keys())
    maxlen = max(len(str(k)) for k in all_keys) if all_keys else 0

    num_maxlen = 0
    for meta in batch_list:
        for v in meta.values():
            if isinstance(v, (int, float)):
                num_maxlen = max(num_maxlen, len(str(v)))
    return maxlen, num_maxlen


def _display_strategy_metadata(
    batch_list: List[Dict[str, Any]], maxlen: int, num_maxlen: int
) -> None:
    """顯示每個策略的 metadata"""
    for i, meta in enumerate(batch_list, 1):
        print(f"--- 策略 {i} ---")
        for k, v in meta.items():
            if isinstance(v, (int, float)):
                print(f"{str(k).ljust(maxlen)} | {str(v).rjust(num_maxlen)}")
            else:
                print(f"{str(k).ljust(maxlen)} | {str(v)}")
        print("-------------------------------")


def _display_batch_metadata(batch: str) -> None:
    """顯示 batch_metadata 信息"""
    try:
        batch_list = json.loads(batch)
        print(f"\n=== batch_metadata（共 {len(batch_list)} 組策略） ===")
        maxlen, num_maxlen = _calculate_column_widths(batch_list)
        _display_strategy_metadata(batch_list, maxlen, num_maxlen)
    except Exception as e:
        print(f"❌ 解析 batch_metadata 失敗: {e}")


def display_metadata(metadata_dict: Dict[str, Any]) -> None:
    """
    顯示 metadata 信息（含 batch_metadata 展開）
    Args:
        metadata_dict: metadata 字典
    """
    if not metadata_dict:
        print("❌ 沒有找到 metadata")
        return

    print("\n=== Metadata（檔案層級） ===")
    for key, value in metadata_dict.items():
        if key == "batch_metadata":
            continue
        print(f"  {key}: {value}")

    # 額外展開 batch_metadata
    batch = metadata_dict.get("batch_metadata")
    if batch:
        _display_batch_metadata(batch)


def display_table_info(df: pd.DataFrame) -> None:
    """
    顯示主表格欄位與前 3 行
    Args:
        df: DataFrame
    """
    if df is None or df.empty:
        print("❌ 數據為空")
        return
    print("\n=== 主表格所有欄位 ===")
    print(list(df.columns))
    print("\n=== 主表格前 3 行 ===")
    print(df.head(3))


def main() -> None:
    """
    主函數：列出指定路徑中的 Parquet 檔案，讓用戶選擇並顯示 metadata 和主表格資訊
    """
    print("=== Parquet 檔案讀取器 ===")
    parquet_files = list_parquet_files()
    if not parquet_files:
        print("程式結束")
        return
    while True:
        try:
            choice = (
                input(
                    "\n請輸入要分析的檔案編號（可用逗號分隔多選，或輸入al/all全選，預設為[1]）："
                ).strip()
                or "1"
            )
            if choice.lower() == "q":
                print("程式結束")
                return
            choice_int = int(choice)
            if 1 <= choice_int <= len(parquet_files):
                selected_file = parquet_files[choice_int - 1]
                print(f"\n✅ 已選擇檔案: {selected_file}")
                break
            else:
                print(f"❌ 請輸入有效編號 (1-{len(parquet_files)})")
        except ValueError:
            print("❌ 請輸入數字或 'q' 退出")
    df, metadata = read_parquet_with_metadata(selected_file)
    if metadata:
        display_metadata(metadata)
    if df is not None:
        display_table_info(df)
        print("\n=== 讀取完成 ===")
        print(f"文件: {selected_file}")
        print(f"數據行數: {len(df)}")
        print(f"數據列數: {len(df.columns)}")
    else:
        print(f"❌ 無法讀取檔案: {selected_file}")


if __name__ == "__main__":
    main()
