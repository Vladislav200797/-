import os
from supabase import create_client
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_last_days():
    try:
        supabase_url = os.getenv("SUPABASE_URL", "").strip()
        supabase_key = os.getenv("SUPABASE_KEY", "").strip()
        
        logger.info(f"Supabase URL: {'установлен' if supabase_url else 'не установлен'}")
        logger.info(f"Supabase Key: {'установлен' if supabase_key else 'не установлен'}")
        
        if not supabase_url.startswith("https://"):
            raise ValueError("URL должен начинаться с https://")
            
        supabase = create_client(supabase_url, supabase_key)
        logger.info("Успешное подключение к Supabase")
        
    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    update_last_days()
