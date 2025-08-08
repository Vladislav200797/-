import os
from supabase import create_client
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_supabase_url(url: str) -> bool:
    pattern = r'^https:\/\/[a-z0-9]+\.supabase\.co$'
    return re.match(pattern, url) is not None

def update_last_days():
    try:
        supabase_url = os.getenv("SUPABASE_URL", "").strip()
        supabase_key = os.getenv("SUPABASE_KEY", "").strip()
        
        logger.info(f"Полученный URL: '{supabase_url}'")
        
        if not validate_supabase_url(supabase_url):
            raise ValueError(
                f"Неверный формат Supabase URL. Ожидается: 'https://[идентификатор].supabase.co', "
                f"получено: '{supabase_url}'"
            )
            
        supabase = create_client(supabase_url, supabase_key)
        logger.info("✅ Успешное подключение к Supabase")
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    update_last_days()
