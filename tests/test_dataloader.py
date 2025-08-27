"""
Test suite for dataloader module

Tests the AbstractDataLoader base class and common functionality
that all data loaders should inherit.
"""

from typing import Optional, Tuple
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

# Import the classes we want to test
from dataloader.base_loader import AbstractDataLoader
from dataloader.binance_loader import BinanceLoader
from dataloader.coinbase_loader import CoinbaseLoader
from dataloader.file_loader import FileLoader
from dataloader.yfinance_loader import YahooFinanceLoader


class ConcreteDataLoader(AbstractDataLoader):
    """Concrete implementation for testing AbstractDataLoader"""

    def load(self) -> Tuple[Optional[pd.DataFrame], str]:
        """Simple implementation that returns test data"""
        data = pd.DataFrame(
            {
                "Time": pd.date_range("2020-01-01", periods=10),
                "Open": np.random.rand(10) * 100,
                "High": np.random.rand(10) * 100,
                "Low": np.random.rand(10) * 100,
                "Close": np.random.rand(10) * 100,
                "Volume": np.random.rand(10) * 1000000,
            }
        )
        return data, "1d"


class TestAbstractDataLoader:
    """Test the AbstractDataLoader base class functionality"""

    def setup_method(self) -> None:
        """Set up test fixtures"""
        self.loader = ConcreteDataLoader()

    def test_initialization(self) -> None:
        """Test that loader initializes with correct attributes"""
        assert hasattr(self.loader, "console")
        assert hasattr(self.loader, "panel_title")
        assert self.loader.panel_error_style == "#8f1511"
        assert self.loader.panel_success_style == "#dbac30"

    def test_show_error(self, capsys: pytest.CaptureFixture) -> None:
        """Test error message display"""
        self.loader.show_error("Test error message")
        captured = capsys.readouterr()
        assert "Test error message" in captured.out
        assert "❌" in captured.out

    def test_show_success(self, capsys: pytest.CaptureFixture) -> None:
        """Test success message display"""
        self.loader.show_success("Test success message")
        captured = capsys.readouterr()
        assert "Test success message" in captured.out

    def test_show_warning(self, capsys: pytest.CaptureFixture) -> None:
        """Test warning message display"""
        self.loader.show_warning("Test warning")
        captured = capsys.readouterr()
        assert "Test warning" in captured.out
        assert "⚠️" in captured.out

    def test_show_info(self, capsys: pytest.CaptureFixture) -> None:
        """Test info message display"""
        self.loader.show_info("Test info")
        captured = capsys.readouterr()
        assert "Test info" in captured.out

    @patch("builtins.input", return_value="test_input")
    def test_get_user_input_no_default(
        self, mock_input: Mock, capsys: pytest.CaptureFixture
    ) -> None:
        """Test user input without default value"""
        result = self.loader.get_user_input("Enter value")
        assert result == "test_input"
        captured = capsys.readouterr()
        assert "Enter value" in captured.out

    @patch("builtins.input", return_value="")
    def test_get_user_input_with_default(
        self, mock_input: Mock, capsys: pytest.CaptureFixture
    ) -> None:
        """Test user input with default value"""
        result = self.loader.get_user_input("Enter value", "default_value")
        assert result == "default_value"
        captured = capsys.readouterr()
        assert "預設 default_value" in captured.out

    @patch("builtins.input", side_effect=["2021-01-01", "2021-12-31"])
    def test_get_date_range(self, mock_input: Mock) -> None:
        """Test date range input"""
        start, end = self.loader.get_date_range()
        assert start == "2021-01-01"
        assert end == "2021-12-31"

    @patch("builtins.input", side_effect=["", ""])
    def test_get_date_range_defaults(self, mock_input: Mock) -> None:
        """Test date range with default values"""
        start, end = self.loader.get_date_range("2020-01-01", "2020-12-31")
        assert start == "2020-01-01"
        assert end == "2020-12-31"

    @patch("builtins.input", return_value="1h")
    def test_get_frequency(self, mock_input: Mock) -> None:
        """Test frequency input"""
        freq = self.loader.get_frequency()
        assert freq == "1h"

    @patch("builtins.input", return_value="")
    def test_get_frequency_default(self, mock_input: Mock) -> None:
        """Test frequency with default value"""
        freq = self.loader.get_frequency("1d")
        assert freq == "1d"

    def test_display_missing_values(self, capsys: pytest.CaptureFixture) -> None:
        """Test missing values display"""
        data = pd.DataFrame(
            {
                "Open": [1, 2, np.nan, 4, 5],
                "Close": [1, 2, 3, 4, 5],
                "Volume": [np.nan, np.nan, 3, 4, 5],
            }
        )
        self.loader.display_missing_values(data, ["Open", "Close", "Volume"])
        captured = capsys.readouterr()
        assert "Open 缺失值比例：20.00%" in captured.out
        assert "Close 缺失值比例：0.00%" in captured.out
        assert "Volume 缺失值比例：40.00%" in captured.out

    def test_standardize_columns(self) -> None:
        """Test column standardization"""
        data = pd.DataFrame(
            {
                "date": ["2020-01-01"],
                "open": [100],
                "high": [110],
                "low": [90],
                "close": [105],
                "volume": [1000000],
            }
        )
        result = self.loader.standardize_columns(data)
        assert "Time" in result.columns
        assert "Open" in result.columns
        assert "High" in result.columns
        assert "Low" in result.columns
        assert "Close" in result.columns
        assert "Volume" in result.columns

    def test_standardize_columns_variations(self) -> None:
        """Test column standardization with various naming conventions"""
        data = pd.DataFrame(
            {
                "timestamp": ["2020-01-01"],
                "o": [100],
                "h": [110],
                "l": [90],
                "c": [105],
                "vol": [1000000],
            }
        )
        result = self.loader.standardize_columns(data)
        assert "Time" in result.columns
        assert "Open" in result.columns
        assert "High" in result.columns
        assert "Low" in result.columns
        assert "Close" in result.columns
        assert "Volume" in result.columns

    def test_ensure_required_columns(self) -> None:
        """Test ensuring required columns exist"""
        data = pd.DataFrame(
            {
                "Time": pd.date_range("2020-01-01", periods=5),
                "Open": [100, 101, 102, 103, 104],
                "Close": [101, 102, 103, 104, 105],
            }
        )
        result = self.loader.ensure_required_columns(data)
        assert "Time" in result.columns
        assert "Open" in result.columns
        assert "High" in result.columns
        assert "Low" in result.columns
        assert "Close" in result.columns
        assert "Volume" in result.columns
        assert len(result.columns) == 6  # Only required columns

    def test_convert_numeric_columns(self) -> None:
        """Test numeric conversion"""
        data = pd.DataFrame(
            {
                "Open": ["100.5", "101.2", "102.3"],
                "High": ["110", "111", "112"],
                "Low": ["90", "91", "92"],
                "Close": ["105", "106", "107"],
                "Volume": ["1000000", "2000000", "3000000"],
            }
        )
        result = self.loader.convert_numeric_columns(data)
        assert pd.api.types.is_numeric_dtype(result["Open"])
        assert pd.api.types.is_numeric_dtype(result["High"])
        assert pd.api.types.is_numeric_dtype(result["Low"])
        assert pd.api.types.is_numeric_dtype(result["Close"])
        assert pd.api.types.is_numeric_dtype(result["Volume"])

    def test_convert_numeric_columns_with_errors(self) -> None:
        """Test numeric conversion with non-convertible values"""
        data = pd.DataFrame(
            {
                "Open": ["100.5", "invalid", "102.3"],
                "Close": ["105", "106", "not_a_number"],
            }
        )
        result = self.loader.convert_numeric_columns(data, ["Open", "Close"])
        assert pd.isna(result["Open"].iloc[1])  # 'invalid' becomes NaN
        assert pd.isna(result["Close"].iloc[2])  # 'not_a_number' becomes NaN

    def test_load_abstract_method(self) -> None:
        """Test that the abstract load method works in concrete implementation"""
        data, freq = self.loader.load()
        assert isinstance(data, pd.DataFrame)
        assert freq == "1d"
        assert "Time" in data.columns
        assert "Open" in data.columns
        assert "Close" in data.columns


class TestFileLoader:
    """Test the FileLoader class"""

    # Note: FileLoader tests are simplified as the loader has complex interactive flows
    # that are difficult to mock comprehensively. Integration tests would be more suitable.

    def test_file_loader_creation(self) -> None:
        """Test FileLoader can be instantiated"""
        loader = FileLoader()
        assert loader is not None
        assert hasattr(loader, "load")
        assert hasattr(loader, "show_error")
        assert hasattr(loader, "show_success")

    def test_file_loader_inheritance(self) -> None:
        """Test FileLoader inherits from AbstractDataLoader"""
        loader = FileLoader()
        assert isinstance(loader, AbstractDataLoader)


class TestBinanceLoader:
    """Test the BinanceLoader class"""

    @patch("dataloader.binance_loader.Client")
    @patch("builtins.input", side_effect=["BTCUSDT", "", "", ""])
    def test_binance_load(self, mock_input: Mock, mock_client: Mock) -> None:
        """Test Binance loader with mocked API"""
        # Mock Binance API response
        mock_klines = [
            [
                1609459200000,  # Open time
                "28923.63",  # Open
                "28955.00",  # High
                "28690.00",  # Low
                "28840.00",  # Close
                "1234.56",  # Volume
                1609462800000,  # Close time
                "35656789.12",  # Quote asset volume
                1000,  # Number of trades
                "617.28",  # Taker buy base asset volume
                "17828394.56",  # Taker buy quote asset volume
                "0",  # Ignore
            ]
            for _ in range(5)
        ]

        mock_client_instance = Mock()
        mock_client_instance.get_historical_klines.return_value = mock_klines
        mock_client.return_value = mock_client_instance

        loader = BinanceLoader()
        data, freq = loader.load()

        assert data is not None
        assert freq == "1d"
        assert len(data) == 5
        assert "Time" in data.columns
        assert "Open" in data.columns
        assert "Close" in data.columns

    @patch("dataloader.binance_loader.Client", side_effect=Exception("API Error"))
    @patch("dataloader.binance_loader.BinanceLoader.get_user_input")
    def test_binance_load_error(self, mock_input: Mock, mock_client: Mock) -> None:
        """Test Binance loader error handling"""
        mock_input.return_value = "BTCUSDT"
        loader = BinanceLoader()
        data, freq = loader.load()

        assert data is None
        assert freq == "BTCUSDT"  # Binance returns symbol on error


class TestYahooFinanceLoader:
    """Test the YahooFinanceLoader class"""

    @patch("yfinance.download")
    @patch("dataloader.yfinance_loader.YahooFinanceLoader.get_frequency")
    @patch("dataloader.yfinance_loader.YahooFinanceLoader.get_date_range")
    @patch("dataloader.yfinance_loader.YahooFinanceLoader.get_user_input")
    def test_yfinance_load(
        self, mock_input: Mock, mock_dates: Mock, mock_freq: Mock, mock_download: Mock
    ) -> None:
        """Test Yahoo Finance loader"""
        # Mock yfinance data
        mock_data = pd.DataFrame(
            {
                "Open": [100, 101, 102, 103, 104],
                "High": [105, 106, 107, 108, 109],
                "Low": [95, 96, 97, 98, 99],
                "Close": [101, 102, 103, 104, 105],
                "Volume": [1000000, 1100000, 1200000, 1300000, 1400000],
            },
            index=pd.date_range("2020-01-01", periods=5),
        )
        mock_download.return_value = mock_data

        # Mock user inputs
        mock_input.return_value = "AAPL"
        mock_dates.return_value = ("2021-01-01", "2021-01-05")
        mock_freq.return_value = "1d"

        loader = YahooFinanceLoader()
        data, freq = loader.load()

        assert data is not None
        assert freq == "1d"
        assert len(data) == 5
        assert "Time" in data.columns
        mock_download.assert_called_once()

    @patch("yfinance.download")
    @patch("dataloader.yfinance_loader.YahooFinanceLoader.get_user_input")
    def test_yfinance_load_empty(self, mock_input: Mock, mock_download: Mock) -> None:
        """Test Yahoo Finance loader with invalid symbol"""
        # Mock empty response
        mock_input.return_value = "INVALID_SYMBOL"
        mock_download.return_value = pd.DataFrame()

        loader = YahooFinanceLoader()
        data, freq = loader.load()

        assert data is None
        assert freq == "INVALID_SYMBOL"  # YahooFinance returns symbol on error


class TestCoinbaseLoader:
    """Test the CoinbaseLoader class"""

    def test_coinbase_loader_creation(self) -> None:
        """Test CoinbaseLoader can be instantiated"""
        loader = CoinbaseLoader()
        assert loader is not None
        assert hasattr(loader, "load")
        assert isinstance(loader, AbstractDataLoader)

    def test_coinbase_loader_inheritance(self) -> None:
        """Test CoinbaseLoader inherits from AbstractDataLoader"""
        assert issubclass(CoinbaseLoader, AbstractDataLoader)


class TestDataIntegrity:
    """Test data integrity across all loaders"""

    def test_all_loaders_inherit_abstract(self) -> None:
        """Test that all loaders inherit from AbstractDataLoader"""
        assert issubclass(FileLoader, AbstractDataLoader)
        assert issubclass(BinanceLoader, AbstractDataLoader)
        assert issubclass(YahooFinanceLoader, AbstractDataLoader)
        assert issubclass(CoinbaseLoader, AbstractDataLoader)

    def test_all_loaders_implement_load(self) -> None:
        """Test that all loaders implement the load method"""
        assert hasattr(FileLoader, "load")
        assert hasattr(BinanceLoader, "load")
        assert hasattr(YahooFinanceLoader, "load")
        assert hasattr(CoinbaseLoader, "load")

    def test_load_return_types(self) -> None:
        """Test that load methods have correct signature"""
        from inspect import signature

        for loader_class in [
            FileLoader,
            BinanceLoader,
            YahooFinanceLoader,
            CoinbaseLoader,
        ]:
            sig = signature(loader_class.load)
            # Check return annotation
            assert sig.return_annotation == Tuple[Optional[pd.DataFrame], str]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
