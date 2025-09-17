"""
數據處理模塊
"""
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from config import Config

logger = logging.getLogger(__name__)

class DataProcessor:
    """數據處理器"""
    
    def analyze_funding_rate(self, funding_df: pd.DataFrame) -> Dict:
        """分析資金費率數據"""
        analysis_results = {}
        
        try:
            for _, row in funding_df.iterrows():
                symbol = row['symbol']
                base_currency = row['base_currency']
                funding_rate = row['funding_rate']
                annualized_rate = row['annualized_rate']
                
                # 獲取該幣種的閾值
                thresholds = Config.get_symbol_threshold(symbol)
                
                # 判斷資金費率狀態
                status = self._determine_funding_status(funding_rate, thresholds)
                
                # 計算風險等級
                risk_level = self._calculate_risk_level(funding_rate, thresholds)
                
                analysis_results[symbol] = {
                    'base_currency': base_currency,
                    'funding_rate': funding_rate,
                    'funding_rate_percent': funding_rate * 100,
                    'annualized_rate': annualized_rate,
                    'annualized_rate_percent': annualized_rate * 100,
                    'status': status,
                    'risk_level': risk_level,
                    'signal': self._generate_signal(funding_rate, status, risk_level),
                    'timestamp': row['timestamp']
                }
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"分析資金費率時發生錯誤: {e}")
            return {}
    
    def analyze_open_interest(self, oi_df: pd.DataFrame) -> Dict:
        """分析未平倉合約數據"""
        analysis_results = {}
        
        try:
            for _, row in oi_df.iterrows():
                symbol = row['symbol']
                base_currency = row['base_currency']
                oi_usd = row['open_interest_usd']
                
                # 獲取該幣種的OI閾值
                oi_thresholds = Config.get_oi_threshold(symbol)
                
                # 判斷OI狀態
                oi_status = self._determine_oi_status(oi_usd, oi_thresholds)
                
                analysis_results[symbol] = {
                    'base_currency': base_currency,
                    'open_interest': row['open_interest'],
                    'open_interest_usd': oi_usd,
                    'open_interest_usd_millions': oi_usd / 1_000_000,
                    'mark_price': row['mark_price'],
                    'oi_status': oi_status,
                    'timestamp': row['timestamp']
                }
            
            return analysis_results
            
        except Exception as e:
            logger.error(f"分析未平倉合約時發生錯誤: {e}")
            return {}
    
    def combine_analysis(self, funding_analysis: Dict, oi_analysis: Dict, 
                        price_data: pd.DataFrame) -> Dict:
        """結合分析"""
        combined_results = {}
        
        try:
            # 將價格數據轉為字典
            price_dict = {}
            for _, row in price_data.iterrows():
                price_dict[row['symbol']] = {
                    'price': row['price'],
                    'change_24h_percent': row['change_24h_percent'],
                    'volume_24h': row['volume_24h']
                }
            
            # 遍歷所有幣種
            all_symbols = set(funding_analysis.keys()) | set(oi_analysis.keys())
            
            for symbol in all_symbols:
                funding_data = funding_analysis.get(symbol, {})
                oi_data = oi_analysis.get(symbol, {})
                price_info = price_dict.get(symbol, {})
                
                combined_results[symbol] = {
                    'symbol': symbol,
                    'base_currency': funding_data.get('base_currency', symbol.split('/')[0]),
                    'funding_rate_percent': funding_data.get('funding_rate_percent', 0),
                    'funding_status': funding_data.get('status', 'unknown'),
                    'funding_risk_level': funding_data.get('risk_level', 0),
                    'oi_usd_millions': oi_data.get('open_interest_usd_millions', 0),
                    'oi_status': oi_data.get('oi_status', 'unknown'),
                    'price': price_info.get('price', 0),
                    'price_change_24h_percent': price_info.get('change_24h_percent', 0),
                    'combined_signal': self._generate_combined_signal(funding_data, oi_data, price_info),
                    'analysis_time': datetime.now(timezone.utc)
                }
            
            return combined_results
            
        except Exception as e:
            logger.error(f"綜合分析時發生錯誤: {e}")
            return {}
    
    def _determine_funding_status(self, funding_rate: float, thresholds: Dict) -> str:
        """判斷資金費率狀態"""
        abs_rate = abs(funding_rate)
        
        if abs_rate >= thresholds['overheated']['min']:
            return 'overheated'
        elif abs_rate >= thresholds['hot']['min']:
            return 'hot'
        else:
            return 'normal'
    
    def _calculate_risk_level(self, funding_rate: float, thresholds: Dict) -> int:
        """計算風險等級 (1-5)"""
        abs_rate = abs(funding_rate)
        
        if abs_rate >= thresholds['overheated']['min']:
            return 5
        elif abs_rate >= thresholds['hot']['min']:
            return 3
        else:
            return 1
    
    def _determine_oi_status(self, oi_usd: float, thresholds: Dict) -> str:
        """判斷未平倉合約狀態"""
        oi_millions = oi_usd / 1_000_000
        
        if oi_millions >= thresholds['high']:
            return 'high'
        elif oi_millions >= thresholds['low']:
            return 'medium'
        else:
            return 'low'
    
    def _generate_signal(self, funding_rate: float, status: str, risk_level: int) -> str:
        """生成交易信號"""
        if status == 'overheated':
            if funding_rate > 0:
                return '🔴 多頭擁擠，小心回調'
            else:
                return '🟢 空頭擠壓，可能反彈'
        elif status == 'hot':
            if funding_rate > 0:
                return '🟡 多頭偏熱，注意風險'
            else:
                return '🟡 空頭偏冷，關注機會'
        else:
            return '⚪ 資金費率正常'
    
    def _generate_combined_signal(self, funding_data: Dict, oi_data: Dict, 
                                price_data: Dict) -> str:
        """生成綜合信號"""
        signals = []
        
        # 資金費率信號
        if funding_data:
            funding_status = funding_data.get('status', 'normal')
            funding_rate = funding_data.get('funding_rate', 0)
            
            if funding_status == 'overheated':
                if funding_rate > 0:
                    signals.append("資金費率過熱(多頭擁擠)")
                else:
                    signals.append("資金費率過冷(空頭擁擠)")
        
        # 未平倉合約信號  
        if oi_data:
            oi_status = oi_data.get('oi_status', 'low')
            if oi_status == 'high':
                signals.append("未平倉合約偏高")
        
        # 價格變化信號
        if price_data:
            price_change = price_data.get('change_24h_percent', 0)
            if abs(price_change) > 5:
                direction = "上漲" if price_change > 0 else "下跌"
                signals.append(f"價格大幅{direction}({price_change:.2f}%)")
        
        return " | ".join(signals) if signals else "市場正常"
