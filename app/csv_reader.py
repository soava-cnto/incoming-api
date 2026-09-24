# csv_reader.py
"""
Lecteur CSV avec prétraitement centralisé.

Utilise CSVPreprocessor pour gérer les fichiers problématiques
(espaces insécables, guillemets mal formés, encodages variés).
"""

import pandas as pd
import logging
from app.utils.csv_preprocessor import CSVPreprocessor

logger = logging.getLogger(__name__)

class CSVReader:
    def __init__(self, filepath, chunksize=50000, include_comment=False, encoding=None):
        """
        Initialise le lecteur CSV.
        
        Args:
            filepath: Chemin du fichier CSV
            chunksize: Nombre de lignes par chunk (défaut 50000)
            include_comment: Si False, exclut la colonne COMMENTAIRE
            encoding: Encodage du fichier (défaut ISO-8859-1)
        """
        self.filepath = filepath
        self.chunksize = chunksize
        self.include_comment = include_comment
        self.encoding = encoding or "ISO-8859-1"

    # def _detect_encoding(self):
    #     """
    #     Détecte automatiquement l’encodage du fichier en lisant un échantillon.
    #     """
    #     result = from_path(self.filepath).best()
    #     # result = charset_normalizer.from_path(self.filepath).best()
    #     if result:
    #         print(f"[INFO] Encodage détecté automatiquement : {result.encoding} (confiance {result.chaos})")
    #         return result.encoding
    #     else:
    #         print("[WARN] Impossible de détecter l’encodage, fallback en utf-8")
    #         return "utf-8"

    # def _try_read(self, **kwargs):
    #     """
    #     Lecture avec l’encodage détecté. Si ça casse, fallback latin1/cp1252.
    #     """
    #     encodings_to_try = [self.encoding, "utf-8", "latin1", "cp1252"]
    #     last_error = None

    #     for enc in encodings_to_try:
    #         try:
    #             df = pd.read_csv(self.filepath, encoding=enc, **kwargs)
    #             if self.used_encoding is None:
    #                 self.used_encoding = enc
    #                 print(f"[INFO] Fichier lu avec encodage : {enc}")
    #             return df
    #         except UnicodeDecodeError as e:
    #             print(f"[WARN] Échec lecture avec encodage {enc}")
    #             last_error = e
    #             continue

    #     raise last_error
    
    def _try_read(self, **kwargs):
        """
        Lecture avec prétraitement complet du CSV (nettoyage brut + parsing + nettoyage DataF).
        Utilise CSVPreprocessor pour gérer les fichiers problématiques (VocalCom, etc.).
        """
        return CSVPreprocessor.read_csv_with_preprocessing(
            self.filepath,
            encoding=self.encoding,
            **kwargs
        )

    def get_chunks(self):
        """
        Génère des chunks du CSV avec prétraitement complet.
        Exclut optionnellement la colonne COMMENTAIRE si include_comment=False.
        """
        # Déterminer les colonnes à utiliser en lisant l'en-tête avec prétraitement
        usecols = None
        if not self.include_comment:
            try:
                # Lire juste l'en-tête avec prétraitement
                header_df = CSVPreprocessor.read_csv_with_preprocessing(
                    self.filepath,
                    encoding=self.encoding,
                    nrows=0
                )
                usecols = [
                    c for c in header_df.columns 
                    if c.strip().upper() != "COMMENTAIRE"
                ]
                logger.info(
                    f"[CSVReader] Colonne COMMENTAIRE exclue. "
                    f"Colonnes à traiter : {usecols}"
                )
            except Exception as e:
                logger.warning(
                    f"[CSVReader] Impossible de filtrer COMMENTAIRE : {e}, "
                    f"lecture complète"
                )

        # Lecture par chunks avec prétraitement
        return CSVPreprocessor.read_csv_in_chunks(
            self.filepath,
            encoding=self.encoding,
            chunksize=self.chunksize,
            usecols=usecols
        )
