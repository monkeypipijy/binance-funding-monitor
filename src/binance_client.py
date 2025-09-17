"""
Binance API 客戶端模塊
"""
import ccxt
import pandas as pd
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from config import Config

logger = logging.getLogger(__name__)

class BinanceClient:
    """Binance API 客戶端"""
    
    def __init__(self):
        """初始化 Binance 客戶端"""
        self.exchange = None
        self._initialize_client()
    
    def _initialize_client(self):
        """初始化 CCXT 客戶端"""
        try:
            options = {
                'enableRateLimit': True,
                'options': {
                    'defaultType': 'future',  # 使用期貨API
                }
            }
            
            # 如果有API密鑰，則添加
            if Config.BINANCE_API_KEY and Config.BINANCE_API_SECRET:
                options['apiKey'] = Config.BINANCE_API_KEY
                options['secret'] = Config.BINANCE_API_SECRET
            
            # 測試網配置
            if Config.BINANCE_TESTNET:
                options['sandbox'] = True
            
            self.exchange = ccxt.binance(options)
            logger.info("Binance 客戶端初始化成功")
            
        except Exception as e:
            logger.error(f"Binance 客戶端初始化失敗: {e}")
            raise
    
    def get_funding_rates(self, symbols: List[str]) -> pd.DataFrame:
        """獲取指定幣種的資金費率"""
        funding_data = []
        timestamp = datetime.now(timezone.utc)
        
        try:
            for symbol in symbols:
                try:
                    # 獲取資金費率歷史（最新一條）
                    funding_history = self.exchange.fetch_funding_rate_history(
                        symbol, limit=1
                    )
                    
                    if funding_history:
                        latest_funding = funding_history[-1]
                        
                        funding_data.append({
                            'symbol': symbol,
                            'base_currency': symbol.split('/')[0],
                            'funding_rate': latest_funding.get('fundingRate', 0),
                            'funding_time': latest_funding.get('datetime'),
                            'timestamp': timestamp,
                            'annualized_rate': latest_funding.get('fundingRate', 0) * 365 * 3,
                        })
                        
                except Exception as e:
                    logger.warning(f"獲取 {symbol} 資金費率失敗: {e}")
                    continue
            
            df = pd.DataFrame(funding_data)
            logger.info(f"成功獲取 {len(df)} 個幣種的資金費率")
            return df
            
        except Exception as e:
            logger.error(f"獲取資金費率時發生錯誤: {e}")
            return pd.DataFrame()
    
    def get_open_interest(self, symbols: List[str]) -> pd.DataFrame:
        """獲取指定幣種的未平倉合約數據"""
        oi_data = []
        timestamp = datetime.now(timezone.utc)
        
        try:
            for symbol in symbols:
                try:
                    # 獲取未平倉合約
                    oi_info = self.exchange.fetch_open_interest(symbol)
                    
                    # 獲取標記價格用於計算美元價值
                    ticker = self.exchange.fetch_ticker(symbol)
                    mark_price = ticker.get('last', 0)
                    
                    oi_value_usd = oi_info.get('openInterestAmount', 0) * mark_price
                    
                    oi_data.append({
                        'symbol': symbol,
                        'base_currency': symbol.split('/')[0],
                        'open_interest': oi_info.get('openInterestAmount', 0),
                        'open_interest_usd': oi_value_usd,
                        'mark_price': mark_price,
                        'timestamp': timestamp,
                    })
                    
                except Exception as e:
                    logger.warning(f"獲取 {symbol} 未平倉合約失敗: {e}")
                    continue
            
            df = pd.DataFrame(oi_data)
            logger.info(f"成功獲取 {len(df)} 個幣種的未平倉合約數據")
            return df
            
        except Exception as e:
            logger.error(f"獲取未平倉合約時發生錯誤: {e}")
            return pd.DataFrame()
    
    def get_price_data(self, symbols: List[str]) -> pd.DataFrame:
        """獲取價格數據"""
        price_data = []
        timestamp = datetime.now(timezone.utc)
        
        try:
            for symbol in symbols:
                try:
                    ticker = self.exchange.fetch_ticker(symbol)
                    
                    price_data.append({
                        'symbol': symbol,
                        'base_currency': symbol.split('/')[0],
                        'price': ticker.get('last', 0),
                        'change_24h': ticker.get('change', 0),
                        'change_24h_percent': ticker.get('percentage', 0),
                        'volume_24h': ticker.get('baseVolume', 0),
                        'timestamp': timestamp,
                    })
                    
                except Exception as e:
                    logger.warning(f"獲取 {symbol} 價格數據失敗: {e}")
                    continue
            
            df = pd.DataFrame(price_data)
            logger.info(f"成功獲取 {len(df)} 個幣種的價格數據")
            return df
            
        except Exception as e:
            logger.error(f"獲取價格數據時發生錯誤: {e}")
            return pd.DataFrame()
