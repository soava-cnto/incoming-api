# app/utils/sftp_client.py
import paramiko
from io import BytesIO
from charset_normalizer import from_bytes
import logging

logger = logging.getLogger("AUTO")


class SFTPClient:
    def __init__(self, config: dict):
        """
        Initialise la connexion SFTP.
        Exemple de config :
        {
            "host": "example.com",
            "port": 22,
            "user": "username",
            "password": "secret",
            "remote_dir": "/incoming/"
        }
        """
        self.config = config
        self.transport = None
        self.sftp = None
        self._connect()

    # -------------------------------------------------------------------------
    def _connect(self):
        """Établit la connexion SFTP avec gestion des erreurs."""
        try:
            self.transport = paramiko.Transport(
                (self.config["host"], self.config.get("port", 22))
            )
            self.transport.connect(
                username=self.config["user"],
                password=self.config["password"]
            )
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logger.info(f"[SFTP] Connexion réussie à {self.config['host']}")
        except Exception as e:
            logger.error(f"[SFTP] Échec de connexion : {e}")
            raise

    # -------------------------------------------------------------------------
    def list_files(self, remote_dir: str = None):
        """Liste les fichiers dans un dossier distant."""
        directory = remote_dir or self.config.get("remote_dir", ".")
        try:
            files = self.sftp.listdir(directory)
            logger.info(f"[SFTP] {len(files)} fichier(s) trouvé(s) dans {directory}")
            return files
        except Exception as e:
            logger.error(f"[SFTP] Impossible de lister {directory} : {e}")
            raise

    # -------------------------------------------------------------------------
    def read_file(self, remote_path: str) -> bytes:
        """
        Lit le contenu complet d’un fichier distant et le retourne en bytes.
        Ferme automatiquement le handle après lecture.
        """
        try:
            with self.sftp.open(remote_path, "rb") as remote_file:
                content = remote_file.read()
            logger.info(f"[SFTP] Lecture réussie du fichier {remote_path} ({len(content)} octets)")
            return content
        except FileNotFoundError:
            logger.error(f"[SFTP] Fichier introuvable : {remote_path}")
            raise
        except Exception as e:
            logger.error(f"[SFTP] Erreur lecture fichier {remote_path} : {e}")
            raise

    # -------------------------------------------------------------------------
    def detect_encoding(self, file_bytes: bytes, sample_size: int = 20000) -> str:
        """
        Détecte l’encodage probable du fichier distant.
        Utilise un échantillon des premiers bytes pour optimiser la performance.
        """
        if not isinstance(file_bytes, (bytes, bytearray)):
            raise TypeError("detect_encoding attend des bytes en entrée")

        try:
            sample = file_bytes[:sample_size]
            result = from_bytes(sample).best()

            if result and getattr(result, "encoding", None):
                logger.info(
                    f"[SFTP] Encodage détecté : {result.encoding} "
                    f"(fiabilité {getattr(result, 'chaos', 'N/A')})"
                )
                return result.encoding

            logger.warning("[SFTP] Encodage indéterminé → fallback sur utf-8")
            return "utf-8"

        except Exception as e:
            logger.warning(f"[SFTP] Erreur détection encodage ({e}) → utf-8 par défaut")
            return "utf-8"

    # -------------------------------------------------------------------------
    def open_file(self, remote_path: str):
        """
        Retourne un objet file-like (à fermer manuellement).
        À utiliser uniquement si tu veux streamer le contenu.
        """
        try:
            return self.sftp.open(remote_path, "rb")
        except Exception as e:
            logger.error(f"[SFTP] Erreur ouverture fichier {remote_path} : {e}")
            raise

    # -------------------------------------------------------------------------
    def close(self):
        """Ferme proprement la connexion SFTP."""
        try:
            if self.sftp:
                self.sftp.close()
        except Exception:
            pass
        try:
            if self.transport:
                self.transport.close()
        except Exception:
            pass
        logger.info("[SFTP] Connexion fermée proprement.")
