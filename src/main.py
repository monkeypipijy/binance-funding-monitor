"""
Binance 資金費率監控系統 - 主程序
"""
import os
import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# 添加當前目錄到 Python 路徑
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config import Config
from binance_client import BinanceClient
from data_processor import DataProcessor
from notifier import Notifier

def setup_logging():
    """設置日誌"""
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

logger = logging.getLogger(__name__)

class FundingMonitor:
    """資金費率監控器"""
    
    def __init__(self):
        """初始化監控器"""
        self.binance_client = BinanceClient()
        self.data_processor = DataProcessor()
        self.notifier = Notifier()
        
        # 創建數據目錄
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        
        logger.info("資金費率監控器初始化完成")
    
    def run_monitoring_cycle(self, cycle_type: str = "5min"):
        """執行監控周期"""
        try:
            logger.info(f"開始執行 {cycle_type} 監控周期")
            
            # 1. 獲取資金費率數據
            logger.info("獲取資金費率數據...")
            funding_df = self.binance_client.get_funding_rates(Config.SYMBOLS)
            
            if funding_df.empty:
                logger.warning("未獲取到資金費率數據")
                return
            
            # 2. 獲取未平倉合約數據
            logger.info("獲取未平倉合約數據...")
            oi_df = self.binance_client.get_open_interest(Config.SYMBOLS)
            
            # 3. 獲取價格數據
            logger.info("獲取價格數據...")
            price_df = self.binance_client.get_price_data(Config.SYMBOLS)
            
            # 4. 數據分析
            logger.info("分析數據...")
            funding_analysis = self.data_processor.analyze_funding_rate(funding_df)
            oi_analysis = self.data_processor.analyze_open_interest(oi_df)
            combined_analysis = self.data_processor.combine_analysis(
                funding_analysis, oi_analysis, price_df
            )
            
            # 5. 保存數據
            self._save_data(funding_df, oi_df, price_df, combined_analysis, cycle_type)
            
            # 6. 檢查警報條件並發送通知
            alerts = self._check_alerts(combined_analysis)
            if alerts:
                self.notifier.send_alerts(alerts, cycle_type)
            
            # 7. 生成報告
            self._generate_report(combined_analysis, cycle_type)
            
            logger.info(f"{cycle_type} 監控周期完成")
            
        except Exception as e:
            logger.error(f"監控周期執行失敗: {e}")
            raise
    
    def _save_data(self, funding_df, oi_df, price_df, analysis, cycle_type):
        """保存數據到 CSV 文件"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if not funding_df.empty:
                funding_file = f"{Config.DATA_DIR}/funding_rate_{cycle_type}_{timestamp}.csv"
                funding_df.to_csv(funding_file, index=False, encoding='utf-8')
            
            if not oi_df.empty:
                oi_file = f"{Config.DATA_DIR}/open_interest_{cycle_type}_{timestamp}.csv"
                oi_df.to_csv(oi_file, index=False, encoding='utf-8')
            
            if not price_df.empty:
                price_file = f"{Config.DATA_DIR}/price_data_{cycle_type}_{timestamp}.csv"
                price_df.to_csv(price_file, index=False, encoding='utf-8')
            
            if analysis:
                analysis_df = pd.DataFrame.from_dict(analysis, orient='index')
                analysis_file = f"{Config.DATA_DIR}/analysis_{cycle_type}_{timestamp}.csv"
                analysis_df.to_csv(analysis_file, index=True, encoding='utf-8')
            
            logger.info(f"數據已保存到 {Config.DATA_DIR} 目錄")
            
        except Exception as e:
            logger.error(f"保存數據時發生錯誤: {e}")
    
    def _check_alerts(self, analysis: dict) -> list:
        """檢查警報條件"""
        alerts = []
        
        try:
            for symbol, data in analysis.items():
                funding_status = data['funding_status']
                oi_status = data['oi_status']
                
                if funding_status in ['overheated'] or (funding_status == 'overheated' and oi_status == 'high'):
                    alerts.append({
                        'symbol': symbol,
                        'base_currency': data['base_currency'],
                        'funding_rate_percent': data['funding_rate_percent'],
                        'status': funding_status,
                        'signal': data['combined_signal'],
                        'priority': 'high'
                    })
            
            return alerts
            
        except Exception as e:
            logger.error(f"檢查警報時發生錯誤: {e}")
            return []
    
    def _generate_report(self, analysis: dict, cycle_type: str):
        """生成監控報告"""
        try:
            if not analysis:
                return
            
            print(f"\n{'='*60}")
            print(f"📊 Binance 資金費率監控報告 ({cycle_type})")
            print(f"⏰ 時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            for symbol, data in analysis.items():
                base_currency = data['base_currency']
                funding_rate_percent = data['funding_rate_percent']
                funding_status = data['funding_status']
                oi_usd_millions = data['oi_usd_millions']
                price_change_24h = data['price_change_24h_percent']
                combined_signal = data['combined_signal']
                
                status_icon = {
                    'overheated': '🔥',
                    'hot': '🟡', 
                    'normal': '🟢'
                }.get(funding_status, '⚪')
                
                print(f"\n{status_icon} {base_currency}")
                print(f"   資金費率: {funding_rate_percent:+.4f}%")
                print(f"   未平倉合約: ${oi_usd_millions:.1f}M")
                print(f"   24h價格變化: {price_change_24h:+.2f}%")
                print(f"   綜合信號: {combined_signal}")
            
            print(f"\n{'='*60}")
            
        except Exception as e:
            logger.error(f"生成報告時發生錯誤: {e}")

def main():
    """主函數"""
    import argparse
    
    # 設置日誌
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Binance 資金費率監控系統')
    parser.add_argument('--cycle', choices=['5min', '1hour'], default='5min',
                       help='監控周期類型')
    args = parser.parse_args()
    
    try:
        monitor = FundingMonitor()
        monitor.run_monitoring_cycle(args.cycle)
        
    except Exception as e:
        logger.error(f"程序執行失敗: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
