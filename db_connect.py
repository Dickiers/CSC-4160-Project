import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_db_connection():
    """Create database connection using environment variables"""
    try:
        connection_string = "postgresql+pg8000://stock_admin:PASSWORD@stock-data-db.cp06c2mwi5vk.us-east-1.rds.amazonaws.com:5432/stock_db"
        engine = create_engine(connection_string)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection established")
        return True

    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        raise e
