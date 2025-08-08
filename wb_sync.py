import os
import time
from datetime import datetime, timedelta
import requests
from supabase import create_client
import logging

# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WBSync:
    def __init__(self):
        self.wb_api_key = os.getenv("WB_API_KEY")
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY")
        )
        
    def create_report_task(self, date_from: str, date_to: str) -> str:
        """Создание задачи на отчет в WB"""
        url = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
        headers = {"Authorization": self.wb_api_key}
        params = {"dateFrom": date_from, "dateTo": date_to}
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()["data"]["taskId"]
    
    def check_task_status(self, task_id: str) -> str:
        """Проверка статуса задачи"""
        url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status"
        headers = {"Authorization": self.wb_api_key}
        
        while True:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            status = response.json()["data"]["status"]
            
            if status == "done":
                return status
            elif status == "error":
                raise Exception("Ошибка генерации отчета")
            
            logger.info(f"Статус: {status}, ожидание 10 сек...")
            time.sleep(10)
    
    def download_report(self, task_id: str) -> list:
        """Загрузка готового отчета"""
        url = f"https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download"
        headers = {"Authorization": self.wb_api_key}
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    
    def transform_data(self, data: list) -> list:
        """Трансформация данных для Supabase"""
        return [{
            "date": item["date"],
            "log_warehouse_coef": item["logWarehouseCoef"],
            "office_id": item["officeId"],
            "warehouse": item["warehouse"],
            "warehouse_coef": item["warehouseCoef"],
            "gi_id": item["giId"],
            "chrt_id": item["chrtId"],
            "size": item["size"],
            "barcode": item["barcode"],
            "subject": item["subject"],
            "brand": item["brand"],
            "vendor_code": item["vendorCode"],
            "nm_id": item["nmId"],
            "volume": item["volume"],
            "calc_type": item["calcType"],
            "warehouse_price": item["warehousePrice"],
            "barcodes_count": item["barcodesCount"],
            "pallet_place_code": item["palletPlaceCode"],
            "pallet_count": item["palletCount"],
            "original_date": item["originalDate"],
            "loyalty_discount": item["loyaltyDiscount"],
            "tariff_fix_date": item["tariffFixDate"],
            "tariff_lower_date": item["tariffLowerDate"]
        } for item in data]
    
    def load_data_period(self, date_from: str, date_to: str):
        """Обработка одного периода"""
        logger.info(f"Обработка периода: {date_from} - {date_to}")
        
        # 1. Создаем задачу
        task_id = self.create_report_task(date_from, date_to)
        logger.info(f"Создана задача: {task_id}")
        
        # 2. Ожидаем готовности
        self.check_task_status(task_id)
        
        # 3. Загружаем отчет
        report_data = self.download_report(task_id)
        logger.info(f"Получено записей: {len(report_data)}")
        
        # 4. Трансформируем и сохраняем
        if report_data:
            transformed = self.transform_data(report_data)
            self.supabase.table("wb_paid_storage").upsert(transformed).execute()
            logger.info(f"Сохранено записей: {len(transformed)}")
        
        # Соблюдаем лимит 1 запрос/минуту
        time.sleep(60)
    
    def initial_load(self):
        """Первоначальная загрузка за весь год"""
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1).date()
        end_date = datetime.now().date() - timedelta(days=1)
        
        current_date = start_date
        while current_date <= end_date:
            period_end = min(current_date + timedelta(days=7), end_date)
            self.load_data_period(
                current_date.isoformat(),
                period_end.isoformat()
            )
            current_date = period_end + timedelta(days=1)
    
    def daily_update(self):
        """Ежедневное обновление (последние 8 дней)"""
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=7)
        self.load_data_period(start_date.isoformat(), end_date.isoformat())

if __name__ == "__main__":
    sync = WBSync()
    
    # Для первоначальной загрузки:
    # sync.initial_load()
    
    # Для ежедневного обновления:
    sync.daily_update()
