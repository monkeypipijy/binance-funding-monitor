"""
配置管理模塊
"""
import os
from datetime import datetime
from dotenv import load_dotenv

# 加載環境變量
load_dotenv()

class Config:
    """配置類"""
    
    # API 配置
    BINANCE_API_KEY = os.getenv('BINANCE_API_KEY', '')
    BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET', '')
    BINANCE_TESTNET = os.getenv('BINANCE_TESTNET', 'false').lower() == 'true'
    
    # 監控配置
    SYMBOLS = [
        'BTC/USDT:USDT',  # BTC 永續合約
        'ETH/USDT:USDT',  # ETH 永續合約
        'BNB/USDT:USDT',  # BNB 永續合約
        'ADA/USDT:USDT',  # ADA 永續合約
        'SOL/USDT:USDT',  # SOL 永續合約
        'DOGE/USDT:USDT', # DOGE 永續合約
        'XRP/USDT:USDT',  # XRP 永續合約
        'MATIC/USDT:USDT', # MATIC 永續合約
        'AVAX/USDT:USDT', # AVAX 永續合約
        'DOT/USDT:USDT',  # DOT 永續合約
    ]
    
    # 資金費率闾值設定
    FUNDING_RATE_THRESHOLDS = {
        'BTC': {
            'normal': {'min': 0.0, 'max': 0.0003},     # 0% ~ 0.03%
            'hot': {'min': 0.0005, 'max': 0.001},      # 0.05% ~ 0.10%
            'overheated': {'min': 0.001, 'max': 999}    # > 0.10%
        },
        'ETH': {
            'normal': {'min': 0.0, 'max': 0.0005},     # 0% ~ 0.05%
            'hot': {'min': 0.001, 'max': 0.0015},      # 0.10% ~ 0.15%
            'overheated': {'min': 0.002, 'max': 999}    # > 0.20%
        },
        'DEFAULT': {  # 其他小幣
            'normal': {'min': -0.005, 'max': 0.005},   # -0.5% ~ 0.5%
            'hot': {'min': 0.01, 'max': 0.015},        # 1% ~ 1.5%
            'overheated': {'min': 0.02, 'max': 999}     # > 2%
        }
    }
    
    # 未平倉合約闾值 (百萬美元)
    OI_THRESHOLDS = {
        'BTC': {'low': 1000, 'high': 5000},
        'ETH': {'low': 500, 'high': 2000}, 
        'DEFAULT': {'low': 50, 'high': 200}
    }
    
    # 調度配置
    SCHEDULE_5MIN = "*/5 * * * *"  # 杈5分鐘
    SCHEDULE_1HOUR = "0 * * * *"  # 每小時
    
    # 通知配置
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '')
    TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
    DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')
    
    # 日誌配置
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = 'funding_monitor.log'
    
    # 數據儲存
    DATA_DIR = 'data'
    CSV_FILE_PREFIX = 'funding_rate_'
    
    @classmethod
    def get_symbol_threshold(cls, symbol):
        """獲取幣種的資金費率闾值"""
        base_symbol = symbol.split('/')[0]
        return cls.FUNDING_RATE_THRESHOLDS.get(base_symbol, cls.FUNDING_RATE_THRESHOLDS['DEFAULT'])
    
    @classmethod
    def get_oi_threshold(cls, symbol):
        """獲取幣種的未平倉合約闾值"""
        base_symbol = symbol.split('/')[0]
        return cls.OI_THRESHOLDS.get(base_symbol, cls.OI_THRESHOLDS['DEFAULT'])
