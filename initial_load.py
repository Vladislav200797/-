import os
from datetime import datetime, timedelta
from supabase import create_client
import requests

# Настройки
WB_API_KEY = os.getenv("WB_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def load_data(date_from, date_to):
    # Ваш код для загрузки данных из WB API
    # и сохранения в Supabase (аналогично предыдущим примерам)
    pass

if __name__ == "__main__":
    # Загрузка всего 2024 года по частям
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime.now().date() - timedelta(days=1)
    
    current_date = start_date
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=7), end_date)
        load_data(current_date, chunk_end)
        current_date = chunk_end + timedelta(days=1)