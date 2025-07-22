import os
import glob

def list_parquet_files(directory):
    """
    掃描指定資料夾下所有parquet檔案，回傳檔案路徑list。
    """
    pattern = os.path.join(directory, '*.parquet')
    return sorted(glob.glob(pattern))

def show_parquet_files(files):
    """
    列出所有parquet檔案，顯示編號與檔名。
    """
    print("\n=== 可用Parquet檔案 ===")
    for idx, file in enumerate(files, 1):
        print(f"[{idx}] {os.path.basename(file)}")

def select_files(files, user_input):
    """
    根據用戶輸入的編號字串，回傳所選檔案的完整路徑list。
    user_input: 字串，如 '1,2' 或 'al'/'all'
    """
    user_input = user_input.strip().lower()
    if user_input in ("al", "all"):
        return files
    try:
        idxs = [int(x) for x in user_input.split(',') if x.strip().isdigit()]
        selected = [files[i-1] for i in idxs if 1 <= i <= len(files)]
        if selected:
            return selected
        else:
            print("❌ 請輸入有效編號！")
            return []
    except Exception:
        print("❌ 輸入格式錯誤，請重新輸入！")
        return [] 