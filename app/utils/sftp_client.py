import paramiko

class SFTPClient:
    def __init__(self, config: dict):
        self.config = config
        self.transport = paramiko.Transport((config["host"], config.get("port", 22)))
        self.transport.connect(username=config["user"], password=config["password"])
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def open_file(self, remote_path: str):
        # Retourne le fichier en mode binaire pour détection d'encodage
        return self.sftp.open(remote_path, "rb")

    def list_files(self, remote_dir: str):
        return self.sftp.listdir(remote_dir)

    def close(self):
        self.sftp.close()
        self.transport.close()
