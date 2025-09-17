"""
通知系統模塊
"""
import logging
import requests
from datetime import datetime
from typing import List, Dict
from config import Config

logger = logging.getLogger(__name__)

class Notifier:
    """通知器"""
    
    def __init__(self):
        """初始化通知器"""
        self.telegram_enabled = bool(Config.TELEGRAM_TOKEN and Config.TELEGRAM_CHAT_ID)
        self.discord_enabled = bool(Config.DISCORD_WEBHOOK_URL)
        
        if self.telegram_enabled:
            logger.info("Telegram 通知已啟用")
        if self.discord_enabled:
            logger.info("Discord 通知已啟用")
        
        if not (self.telegram_enabled or self.discord_enabled):
            logger.warning("未配置任何通知方式")
    
    def send_alerts(self, alerts: List[Dict], cycle_type: str):
        """發送警報"""
        if not alerts:
            return
        
        try:
            message = self._format_alert_message(alerts, cycle_type)
            
            if self.telegram_enabled:
                self._send_telegram_message(message)
            
            if self.discord_enabled:
                self._send_discord_message(message)
            
            logger.info(f"已發送 {len(alerts)} 個警報通知")
            
        except Exception as e:
            logger.error(f"發送警報時發生錯誤: {e}")
    
    def send_report(self, analysis: Dict, cycle_type: str):
        """發送定期報告"""
        try:
            message = self._format_report_message(analysis, cycle_type)
            
            if self.telegram_enabled:
                self._send_telegram_message(message)
            
            if self.discord_enabled:
                self._send_discord_message(message)
            
            logger.info(f"已發送 {cycle_type} 定期報告")
            
        except Exception as e:
            logger.error(f"發送報告時發生錯誤: {e}")
    
    def _format_alert_message(self, alerts: List[Dict], cycle_type: str) -> str:
        """格式化警報消息"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"🚨 **Binance 資金費率警報** ({cycle_type})\n"
        message += f"⏰ {timestamp}\n\n"
        
        for alert in alerts:
            symbol = alert['symbol']
            base_currency = alert['base_currency']
            funding_rate = alert.get('funding_rate_percent', 0)
            signal = alert.get('signal', '')
            
            message += f"🔴 **{base_currency}** ({symbol})\n"
            message += f"   資金費率: {funding_rate:+.4f}%\n"
            message += f"   {signal}\n\n"
        
        return message
    
    def _format_report_message(self, analysis: Dict, cycle_type: str) -> str:
        """格式化報告消息"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"📊 **Binance 資金費率報告** ({cycle_type})\n"
        message += f"⏰ {timestamp}\n\n"
        
        if not analysis:
            message += "無數據\n"
            return message
        
        # 統計信息
        total_symbols = len(analysis)
        overheated_count = sum(1 for data in analysis.values() 
                              if data.get('funding_status') == 'overheated')
        hot_count = sum(1 for data in analysis.values() 
                       if data.get('funding_status') == 'hot')
        
        message += f"📈 總計幣種: {total_symbols}\n"
        message += f"🔥 過熱: {overheated_count}\n"
        message += f"🟡 偏熱: {hot_count}\n\n"
        
        # 顯示前3個最極端的資金費率
        sorted_analysis = sorted(
            analysis.items(),
            key=lambda x: abs(x[1].get('funding_rate_percent', 0)),
            reverse=True
        )
        
        message += "**資金費率TOP 3:**\n"
        for symbol, data in sorted_analysis[:3]:
            base_currency = data['base_currency']
            funding_rate = data['funding_rate_percent']
            status = data['funding_status']
            
            status_icon = {
                'overheated': '🔥',
                'hot': '🟡',
                'normal': '🟢'
            }.get(status, '⚪')
            
            message += f"{status_icon} {base_currency}: {funding_rate:+.4f}%\n"
        
        return message
    
    def _send_telegram_message(self, message: str):
        """發送 Telegram 消息"""
        try:
            url = f"https://api.telegram.org/bot{Config.TELEGRAM_TOKEN}/sendMessage"
            data = {
                'chat_id': Config.TELEGRAM_CHAT_ID,
                'text': message,
                'parse_mode': 'Markdown'
            }
            
            response = requests.post(url, data=data, timeout=10)
            response.raise_for_status()
            
            logger.info("Telegram 消息發送成功")
            
        except Exception as e:
            logger.error(f"Telegram 消息發送失敗: {e}")
    
    def _send_discord_message(self, message: str):
        """發送 Discord 消息"""
        try:
            data = {
                'content': message
            }
            
            response = requests.post(Config.DISCORD_WEBHOOK_URL, json=data, timeout=10)
            response.raise_for_status()
            
            logger.info("Discord 消息發送成功")
            
        except Exception as e:
            logger.error(f"Discord 消息發送失敗: {e}")
