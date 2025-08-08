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
        """Трансформация данных для Supabase с обработкой пустых дат"""
        transformed = []
        for item in data:
            try:
                record = {
                    "date": item["date"],
                    "log_warehouse_coef": item.get("logWarehouseCoef"),
                    "office_id": item.get("officeId"),
                    "warehouse": item.get("warehouse"),
                    "warehouse_coef": item.get("warehouseCoef"),
                    "gi_id": item.get("giId"),
                    "chrt_id": item.get("chrtId"),
                    "size": item.get("size"),
                    "barcode": item.get("barcode"),
                    "subject": item.get("subject"),
                    "brand": item.get("brand"),
                    "vendor_code": item.get("vendorCode"),
                    "nm_id": item.get("nmId"),
                    "volume": item.get("volume"),
                    "calc_type": item.get("calcType"),
                    "warehouse_price": item.get("warehousePrice"),
                    "barcodes_count": item.get("barcodesCount"),
                    "pallet_place_code": item.get("palletPlaceCode"),
                    "pallet_count": item.get("palletCount"),
                    "loyalty_discount": item.get("loyaltyDiscount"),
                    "original_date": item.get("originalDate") or None,
                    "tariff_fix_date": item.get("tariffFixDate") or None,
                    "tariff_lower_date": item.get("tariffLowerDate") or None
                }
                transformed.append(record)
            except Exception as e:
                logger.error(f"Ошибка обработки записи: {item}. Ошибка: {str(e)}")
                continue
        return transformed
    
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
            # Вставка пакетами по 500 записей
            batch_size = 500
            for i in range(0, len(transformed), batch_size):
                batch = transformed[i:i + batch_size]
                self.supabase.table("wb_paid_storage").upsert(batch).execute()
                logger.info(f"Сохранено записей: {len(batch)}")
                time.sleep(1)  # Небольшая пауза между пакетами
        
        # Соблюдаем лимит 1 запрос/минуту к WB API
        time.sleep(60)
    
    def initial_load(self):
        """Первоначальная загрузка за весь год"""
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1).date()
        end_date = datetime.now().date() - timedelta(days=1)
        
        current_date = start_date
        while current_date <= end_date:
            period_end = min(current_date + timedelta(days=7), end_date)
            try:
                self.load_data_period(
                    current_date.isoformat(),
                    period_end.isoformat()
                )
            except Exception as e:
                logger.error(f"Ошибка загрузки периода {current_date}: {str(e)}")
                time.sleep(120)  # Увеличенная пауза при ошибке
            current_date = period_end + timedelta(days=1)
    
    def daily_update(self):
        """Ежедневное обновление (последние 8 дней)"""
        end_date = datetime.now().date() - timedelta(days=1)
        start_date = end_date - timedelta(days=7)
        self.load_data_period(start_date.isoformat(), end_date.isoformat())

if __name__ == "__main__":
    sync = WBSync()
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial", action="store_true", help="Первоначальная загрузка за весь год")
    args = parser.parse_args()
    
    if args.initial:
        sync.initial_load()
    else:
        sync.daily_update()
