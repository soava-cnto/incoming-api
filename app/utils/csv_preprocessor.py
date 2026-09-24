"""
Module centralisé pour le prétraitement des fichiers CSV.

Gère le nettoyage du contenu brut avant parsing et après parsing,
notamment pour les fichiers problématiques (VocalCom, etc.) contenant :
- Espaces insécables (U+00A0)
- Guillemets mal formés
- Encodages variés (UTF-16, ISO-8859-1, etc.)
"""

import io
import logging
import pandas as pd
from typing import Union, Optional, Tuple

logger = logging.getLogger(__name__)


class CSVPreprocessor:
    """
    Préprocesseur centralisé pour CSV problématiques.
    
    Applique un nettoyage en deux phases :
    1. Avant parsing : nettoyage du contenu brut (caractères mal formés)
    2. Après parsing : nettoyage des colonnes et valeurs
    """

    # Caractères problématiques à nettoyer avant parsing
    CHAR_REPLACEMENTS = {
        "\u00a0": " ",      # Espace insécable → espace classique
    }

    # Séquences problématiques à corriger avant parsing
    SEQUENCE_FIXES = {
        ' ""': '"',         # Séquence de fin de ligne mal formée
    }

    @staticmethod
    def preprocess_raw_content(raw_content: Union[str, bytes], encoding: str) -> str:
        """
        Prétraite le contenu brut d'un CSV avant parsing.
        
        Effectue un nettoyage caractère par caractère sur le contenu décodé
        pour gérer les espaces insécables et guillemets mal formés.
        
        Args:
            raw_content: Contenu brut (str ou bytes)
            encoding: Encodage du fichier
            
        Returns:
            Contenu nettoyé (str)
            
        Raises:
            ValueError: Si le contenu est vide après décodage
        """
        # Décoder si nécessaire
        if isinstance(raw_content, bytes):
            try:
                decoded = raw_content.decode(encoding, errors="ignore")
            except LookupError:
                logger.warning(
                    f"Encodage '{encoding}' inconnu, fallback sur UTF-8"
                )
                decoded = raw_content.decode("utf-8", errors="ignore")
        else:
            decoded = raw_content

        if not decoded or not decoded.strip():
            raise ValueError("Contenu CSV vide ou illisible après décodage")

        # Nettoyage ligne par ligne
        clean_lines = []
        for line in decoded.splitlines():
            # Remplacer les caractères problématiques
            for old_char, new_char in CSVPreprocessor.CHAR_REPLACEMENTS.items():
                line = line.replace(old_char, new_char)

            # Corriger les séquences problématiques
            for old_seq, new_seq in CSVPreprocessor.SEQUENCE_FIXES.items():
                line = line.replace(old_seq, new_seq)

            clean_lines.append(line)

        cleaned_content = "\n".join(clean_lines)
        logger.debug(
            f"Contenu brut nettoyé : {len(cleaned_content)} caractères"
        )
        return cleaned_content

    @staticmethod
    def read_csv_with_preprocessing(
        source: Union[str, bytes, io.StringIO],
        encoding: str = "ISO-8859-1",
        **pandas_kwargs
    ) -> pd.DataFrame:
        """
        Lit un CSV avec prétraitement complet (avant et après parsing).
        
        Enchaîne les étapes :
        1. Prétraitement du contenu brut
        2. Parsing du CSV
        3. Nettoyage des colonnes et valeurs
        
        Args:
            source: Fichier à lire (chemin, bytes, ou StringIO)
            encoding: Encodage du fichier
            **pandas_kwargs: Arguments supplémentaires pour pd.read_csv()
            
        Returns:
            DataFrame nettoyé
        """
        # Prétraitement du contenu brut
        if isinstance(source, str) and not isinstance(source, io.StringIO):
            # Si c'est un chemin fichier, lire et prétraiter
            try:
                with open(source, "rb") as f:
                    raw_content = f.read()
            except FileNotFoundError:
                logger.error(f"Fichier non trouvé : {source}")
                raise

            cleaned_content = CSVPreprocessor.preprocess_raw_content(
                raw_content, encoding
            )
            source_for_pandas = io.StringIO(cleaned_content)
        elif isinstance(source, bytes):
            # Si c'est du contenu brut
            cleaned_content = CSVPreprocessor.preprocess_raw_content(
                source, encoding
            )
            source_for_pandas = io.StringIO(cleaned_content)
        else:
            # Déjà un StringIO ou similaire
            source_for_pandas = source

        # Parsing du CSV avec le moteur Python (plus flexible)
        df = pd.read_csv(
            source_for_pandas,
            sep=",",
            engine="python",
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA", "NULL"],
            on_bad_lines="warn",
            **pandas_kwargs
        )

        # Nettoyage post-parsing
        df = CSVPreprocessor.clean_dataframe(df)

        return df

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Nettoie les colonnes et valeurs après parsing.
        
        Effectue :
        - Suppression des guillemets/espaces en début/fin de colonnes
        - Suppression des guillemets/espaces en début/fin de valeurs texte
        
        Args:
            df: DataFrame brut
            
        Returns:
            DataFrame nettoyé
        """
        df = df.copy()

        # Nettoyage des noms de colonnes
        df.columns = df.columns.str.strip('" ')

        # Nettoyage des valeurs texte
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].str.strip('" ')

        return df

    @staticmethod
    def read_csv_in_chunks(
        source: Union[str, bytes],
        encoding: str = "ISO-8859-1",
        chunksize: int = 5000,
        **pandas_kwargs
    ):
        """
        Générateur : lit un CSV avec prétraitement par chunks.
        
        Utile pour traiter les gros fichiers sans charger le fichier entier.
        
        Args:
            source: Fichier à lire (chemin ou bytes)
            encoding: Encodage du fichier
            chunksize: Nombre de lignes par chunk
            **pandas_kwargs: Arguments supplémentaires pour pd.read_csv()
            
        Yields:
            DataFrame nettoyé pour chaque chunk
        """
        # Prétraitement du contenu brut une seule fois
        if isinstance(source, str):
            try:
                with open(source, "rb") as f:
                    raw_content = f.read()
            except FileNotFoundError:
                logger.error(f"Fichier non trouvé : {source}")
                raise
        else:
            raw_content = source

        cleaned_content = CSVPreprocessor.preprocess_raw_content(
            raw_content, encoding
        )
        source_for_pandas = io.StringIO(cleaned_content)

        # Lecture par chunks
        reader = pd.read_csv(
            source_for_pandas,
            sep=",",
            engine="python",
            dtype=str,
            keep_default_na=False,
            na_values=["", "NA", "NULL"],
            on_bad_lines="warn",
            chunksize=chunksize,
            **pandas_kwargs
        )

        for chunk in reader:
            # Nettoyage post-parsing pour chaque chunk
            yield CSVPreprocessor.clean_dataframe(chunk)

    @staticmethod
    def detect_and_clean_encoding_issues(
        raw_content: bytes,
        attempted_encodings: Optional[list] = None
    ) -> Tuple[str, pd.DataFrame]:
        """
        Tentative de détection et nettoyage avec fallback sur encodages multiples.
        
        Args:
            raw_content: Contenu brut en bytes
            attempted_encodings: Liste d'encodages à essayer
            
        Returns:
            Tuple (encodage_utilisé, DataFrame nettoyé)
        """
        if attempted_encodings is None:
            attempted_encodings = [
                "utf-16",
                "utf-16-le",
                "utf-16-be",
                "ISO-8859-1",
                "cp1252",
                "utf-8",
            ]

        last_error = None
        for encoding in attempted_encodings:
            try:
                df = CSVPreprocessor.read_csv_with_preprocessing(
                    raw_content, encoding=encoding
                )
                logger.info(
                    f"CSV décodé et nettoyé avec succès : encodage {encoding}"
                )
                return encoding, df
            except Exception as e:
                logger.debug(
                    f"Impossible de déchiffrer avec {encoding} : {e}"
                )
                last_error = e
                continue

        # Aucun encodage ne fonctionne
        logger.error(
            f"Impossible de déchiffrer le CSV avec aucun encodage "
            f"(essayés : {attempted_encodings})"
        )
        raise last_error or ValueError(
            f"Impossible de déchiffrer le CSV "
            f"(encodages essayés : {attempted_encodings})"
        )
