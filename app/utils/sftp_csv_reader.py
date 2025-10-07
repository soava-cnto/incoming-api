import pandas as pd
import logging
from io import StringIO
from charset_normalizer import from_bytes

logger = logging.getLogger(__name__)

class SFTPCSVReader:
    def __init__(self, file_like, chunksize=50000, include_comment=False, encoding=None):
        """
        file_like: objet BytesIO ou fichier ouvert depuis SFTP
        """
        self.file_like = file_like
        self.chunksize = chunksize
        self.include_comment = include_comment

        # Détection automatique d'encodage si pas précisé
        self.encoding = encoding or self._detect_encoding()

        # Lire tout le contenu pour Pandas
        self.file_like.seek(0)
        self.str_io = StringIO(self.file_like.read().decode(self.encoding, errors="replace"))

    def _detect_encoding(self):
        self.file_like.seek(0)
        sample_bytes = self.file_like.read(10000)
        self.file_like.seek(0)
        result = from_bytes(sample_bytes).best()
        if result:
            logger.info(f"[SFTP] Encodage détecté : {result.encoding} (fiabilité {result.chaos})")
            return result.encoding
        else:
            logger.warning("[SFTP] Impossible de détecter l’encodage, fallback en utf-8")
            return "utf-8"

    def get_chunks(self):
        """
        Retourne un générateur de chunks Pandas, en excluant la colonne COMMENTAIRE.
        Les lignes “problématiques” sont sauvegardées dans un fichier CSV séparé.
        """
        # Détecter colonnes si on veut exclure COMMENTAIRE
        usecols = None
        if not self.include_comment:
            header = pd.read_csv(self.str_io, nrows=0, engine="python")
            usecols = [c for c in header.columns if c.strip().upper() != "COMMENTAIRE"]

        self.str_io.seek(0)
        bad_lines = []

        def on_bad_lines(bad_line):
            line_str = ",".join(bad_line) if isinstance(bad_line, list) else str(bad_line)
            logger.warning(f"[SFTP] Ligne corrompue détectée : {line_str}")
            bad_lines.append(line_str)
            return bad_line  # renvoyer pour l’inclure dans le chunk

        try:
            for chunk in pd.read_csv(
                self.str_io,
                chunksize=self.chunksize,
                dtype=str,
                keep_default_na=False,
                na_values=["", "NA", "NULL"],
                engine="python",
                quotechar='"',
                doublequote=True,
                escapechar="\\",
                usecols=usecols,
                on_bad_lines=on_bad_lines
            ):
                yield chunk
        finally:
            # sauvegarder lignes corrompues
            if bad_lines:
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                error_file = f"sftp_bad_lines_{ts}.csv"
                with open(error_file, "w", encoding="utf-8") as f:
                    for line in bad_lines:
                        f.write(line + "\n")
                logger.warning(f"[SFTP] Lignes corrompues sauvegardées dans {error_file}")
