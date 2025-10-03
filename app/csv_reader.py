# csv_reader.py
import pandas as pd
from charset_normalizer.api import from_path

class CSVReader:
    def __init__(self, filepath, chunksize=50000, include_comment=False, encoding=None):
        self.filepath = filepath
        self.chunksize = chunksize
        self.include_comment = include_comment
        self.encoding = encoding or self._detect_encoding()
        self.used_encoding = None  # encodage réellement utilisé

    def _detect_encoding(self):
        """
        Détecte automatiquement l’encodage du fichier en lisant un échantillon.
        """
        result = from_path(self.filepath).best()
        # result = charset_normalizer.from_path(self.filepath).best()
        if result:
            print(f"[INFO] Encodage détecté automatiquement : {result.encoding} (confiance {result.chaos})")
            return result.encoding
        else:
            print("[WARN] Impossible de détecter l’encodage, fallback en utf-8")
            return "utf-8"

    def _try_read(self, **kwargs):
        """
        Lecture avec l’encodage détecté. Si ça casse, fallback latin1/cp1252.
        """
        encodings_to_try = [self.encoding, "utf-8", "latin1", "cp1252"]
        last_error = None

        for enc in encodings_to_try:
            try:
                df = pd.read_csv(self.filepath, encoding=enc, **kwargs)
                if self.used_encoding is None:
                    self.used_encoding = enc
                    print(f"[INFO] Fichier lu avec encodage : {enc}")
                return df
            except UnicodeDecodeError as e:
                print(f"[WARN] Échec lecture avec encodage {enc}")
                last_error = e
                continue

        raise last_error

    def get_chunks(self):
        # Détecter colonnes si on veut exclure COMMENTAIRE
        usecols = None
        if not self.include_comment:
            header = self._try_read(nrows=0, engine="python")
            usecols = [c for c in header.columns if c.strip().upper() != "COMMENTAIRE"]

        return self._try_read(
            sep=",",
            quotechar='"',
            doublequote=True,
            escapechar="\\",
            engine="python",
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA", "NULL"],
            on_bad_lines="warn",
            usecols=usecols,
            chunksize=self.chunksize
        )
