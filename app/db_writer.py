# db_writer.py
from sqlalchemy import create_engine, text
from io import StringIO
import pandas as pd

class DBWriter:
    def __init__(self, db_config: dict, table_name: str, view_name: str ):
        self.db_config = db_config
        self.table_name = table_name
        self.engine = create_engine(
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        self._ensure_log_table()
        self.view_name = view_name

    def _ensure_log_table(self):
        """Crée la table de log si elle n’existe pas"""
        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS imported_files (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT UNIQUE,
                    imported_at TIMESTAMP DEFAULT now()
                )
            """))
            conn.commit()

    def already_imported(self, file_name: str) -> bool:
        """Vérifie si le fichier a déjà été importé"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM imported_files WHERE file_name = :f"),
                {"f": file_name}
            ).fetchone()
            return result is not None

    def log_import(self, file_name: str):
        """Consigne qu’un fichier a été importé"""
        with self.engine.connect() as conn:
            conn.execute(
                text("INSERT INTO imported_files (file_name) VALUES (:f) ON CONFLICT DO NOTHING"),
                {"f": file_name}
            )
            conn.commit()

    def copy_dataframe(self, df: pd.DataFrame):
        """Insère un DataFrame en bulk via COPY"""
        conn = self.engine.raw_connection()
        cur = conn.cursor()

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cols = ",".join(df.columns)
        sql = f"COPY {self.table_name} ({cols}) FROM STDIN WITH CSV"
        cur.copy_expert(sql, buffer)

        conn.commit()
        cur.close()
        conn.close()

    def close(self):
        self.engine.dispose()
        
    def get_engine(self):
        return self.engine

    def get_view_name(self):
        return self.view_name
