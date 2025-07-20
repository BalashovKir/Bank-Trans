import asyncio
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

# Добавляем путь к src в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent))

src_path = str(Path(__file__).parent)
if src_path not in sys.path:
    sys.path.append(src_path)

# Загружаем переменные окружения для email
load_dotenv(os.path.join(os.path.dirname(__file__), '.env.local'))

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

try:
    from models import Transaction
    from rule_engine import fraud_engine
    from db import get_session
    from crud import save_transaction
    try:
        from logger import logger
    except ImportError:
        import logging
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.warning("Loguru not found, using standard logging")
    from alert_sender import MailtrapAlertSender
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Текущий sys.path:", sys.path)
    raise

BOOTSTRAP_SERVERS = [
    'localhost:9092',   
    'localhost:19092',    
    'localhost:10092',
    'localhost:11092',
    '127.0.0.1:9092'      
]

async def process_transactions():
    # Инициализация системы оповещений
    alert_sender = MailtrapAlertSender()
    
    consumer = AIOKafkaConsumer(
        "transactions",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="mvp-consumer-group",
        request_timeout_ms=3000,
        session_timeout_ms=10000,
        heartbeat_interval_ms=3000
    )
    
    await consumer.start()
    logger.info("Успешно подключено к Kafka")
    logger.info("Подписка на тему: transactions")
    
    try:
        async for msg in consumer:
            try:
                tx_data = msg.value
                logger.info(f"Получена транзакция: {tx_data['id']}")
                
                # Конвертируем строку времени в datetime
                if 'timestamp' in tx_data:
                    tx_data['timestamp'] = datetime.fromisoformat(tx_data['timestamp'])
                
                tx = Transaction(**tx_data)
                result = fraud_engine.analyze(tx)

                # Временно отключено сохранение в БД
                """
                with get_session() as db:
                    save_transaction(
                        tx,
                        db,
                        is_suspicious=result["is_suspicious"],
                        alerts=result["alerts"],
                        risk_score=result["risk_score"]
                    )
                """

                if result["is_suspicious"]:
                    alert_message = (
                        f"\n🚨 Подозрительная транзакция [Риск: {result['risk_score']}%]\n"
                        f"ID: {tx.id}\n"
                        f"Сумма: {tx.amount} {tx.currency}\n"
                        f"Время: {tx.timestamp}\n"
                        f"Причины: {', '.join(result['alerts'])}"
                    )
                    logger.warning(alert_message)
                    
                    # Подготовка данных для алерта
                    tx_data_for_alert = {
                        'id': tx.id,
                        'amount': tx.amount,
                        'currency': tx.currency,
                        'timestamp': tx.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'ip': tx.ip if tx.ip else 'N/A'
                    }
                    
                    # Отправка email-оповещения
                    await alert_sender.send_alert(
                        tx_data_for_alert, 
                        result['alerts'], 
                        result['risk_score']
                    )
                    
            except json.JSONDecodeError as e:
                logger.error(f"Неверный JSON: {e} | Данные: {msg.value}")
            except Exception as e:
                logger.error(f"Ошибка обработки: {e}")
    except Exception as e:
        logger.critical(f"Ошибка подключения: {e}")
    finally:
        await consumer.stop()
        logger.info("Обработчик остановлен")

if __name__ == "__main__":
    asyncio.run(process_transactions())