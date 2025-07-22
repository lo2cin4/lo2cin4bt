

class BaseMetricTracker:
    """
    績效分析基底類
    """
    def __init__(self):
        pass

    def analyze(self, file_list):
        print("[BaseMetricTracker] 收到以下檔案進行分析：")
        for f in file_list:
            print(f"  - {f}")

    def load_data(self, file_path: str):
        """讀取 parquet 或其他格式的原始回測資料"""
        raise NotImplementedError

    def calculate_metrics(self, df):
        """計算所有績效指標，回傳 DataFrame"""
        raise NotImplementedError

    def export(self, df, output_path: str):
        """輸出標準化後的 DataFrame 或檔案"""
        raise NotImplementedError 