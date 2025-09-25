from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def _get_engine(user: str,  password: str, port: str, database: str, host: str = "localhost",):
    url = f"postgresql+psycopg2://{user}:{password}@postgres:{port}/{database}"
    engine = create_engine(url)
    return engine