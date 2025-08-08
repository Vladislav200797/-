import os
from datetime import datetime, timedelta
from supabase import create_client

def update_last_days():
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    
    # Удаляем данные за последние 8 дней (на случай перерасчётов)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=8)
    
    supabase.table("wb_storage").delete().gte("report_date", start_date.isoformat()).execute()
    
    # Загружаем свежие данные
    load_data(start_date, end_date)

if __name__ == "__main__":
    update_last_days()