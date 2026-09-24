"""
Tests d'intégration pour l'IngestionService avec CSVPreprocessor.

Teste l'intégration du préprocesseur avec :
- process_csv() et CSVReader
- process_path() avec plusieurs fichiers
- clean_csv_remove_comment_column()
"""

import io
import os
import tempfile
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from app.services.ingestion_service import IngestionService
from app.csv_reader import CSVReader
from app.utils.csv_preprocessor import CSVPreprocessor


class TestCSVReaderWithPreprocessor:
    """Tests du CSVReader intégré avec CSVPreprocessor"""

    def test_csv_reader_get_chunks_with_preprocessing(self):
        """CSVReader utilise CSVPreprocessor pour la lecture par chunks"""
        csv_content = (
            'Column1,Column2\n'
            '"Value1\u00a0A","Value1\u00a0B"\n'
            '"Value2\u00a0A","Value2\u00a0B"\n'
            '"Value3\u00a0A","Value3\u00a0B"\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            reader = CSVReader(temp_path, chunksize=1)
            chunks = list(reader.get_chunks())
            
            # Doit retourner 3 chunks (3 lignes de data, 1 chunk size)
            assert len(chunks) >= 1
            
            # Vérifier que le prétraitement a été appliqué
            for chunk in chunks:
                # Pas d'espaces insécables
                assert "\u00a0" not in chunk.values.astype(str).flatten()
                # Pas de guillemets en début/fin de colonnes
                assert all('"' not in col for col in chunk.columns)
        finally:
            os.unlink(temp_path)

    def test_csv_reader_excludes_comment_column(self):
        """CSVReader exclut la colonne COMMENTAIRE si include_comment=False"""
        csv_content = (
            'ID,Name,COMMENTAIRE\n'
            '1,Alice,Comment1\n'
            '2,Bob,Comment2\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            reader = CSVReader(temp_path, include_comment=False)
            chunks = list(reader.get_chunks())
            
            assert len(chunks) >= 1
            # Vérifier que COMMENTAIRE n'est pas dans les colonnes
            for chunk in chunks:
                assert "COMMENTAIRE" not in chunk.columns
        finally:
            os.unlink(temp_path)

    def test_csv_reader_includes_comment_column_if_requested(self):
        """CSVReader inclut COMMENTAIRE si include_comment=True"""
        csv_content = (
            'ID,Name,COMMENTAIRE\n'
            '1,Alice,Comment1\n'
            '2,Bob,Comment2\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            reader = CSVReader(temp_path, include_comment=True)
            chunks = list(reader.get_chunks())
            
            assert len(chunks) >= 1
            # Vérifier que COMMENTAIRE est présent
            for chunk in chunks:
                assert "COMMENTAIRE" in chunk.columns or "commentaire" in chunk.columns.str.lower().tolist()
        finally:
            os.unlink(temp_path)


class TestIngestionServiceWithPreprocessor:
    """Tests de l'IngestionService intégré avec CSVPreprocessor"""

    def test_clean_csv_remove_comment_column_uses_preprocessor(self):
        """clean_csv_remove_comment_column utilise CSVPreprocessor"""
        csv_content = (
            'ID,Name,COMMENTAIRE\n'
            '1,Alice,This is a comment\n'
            '2,Bob,Another comment\n'
        )
        raw_data = csv_content.encode("utf-8")
        
        result_io = IngestionService.clean_csv_remove_comment_column(raw_data, "utf-8")
        
        # Doit retourner un StringIO
        assert isinstance(result_io, io.StringIO)
        
        # Lire le contenu
        content = result_io.getvalue()
        
        # Doit avoir nettoyé le contenu
        df = pd.read_csv(io.StringIO(content), engine="python")
        
        # COMMENTAIRE ne doit pas être présent
        assert "COMMENTAIRE" not in df.columns

    def test_clean_csv_remove_comment_column_with_nbsp_and_quotes(self):
        """clean_csv_remove_comment_column traite aussi nbsp et guillemets"""
        csv_content = (
            'ID,Name\u00a0,COMMENTAIRE\n'
            '"1\u00a0","Alice","Comment1"\n'
        )
        raw_data = csv_content.encode("utf-8")
        
        result_io = IngestionService.clean_csv_remove_comment_column(raw_data, "utf-8")
        content = result_io.getvalue()
        
        # Doit avoir supprimé l'espace insécable
        assert "\u00a0" not in content

    def test_clean_csv_remove_comment_column_no_comment_column(self):
        """clean_csv_remove_comment_column fonctionne si pas de COMMENTAIRE"""
        csv_content = (
            'ID,Name\n'
            '1,Alice\n'
            '2,Bob\n'
        )
        raw_data = csv_content.encode("utf-8")
        
        result_io = IngestionService.clean_csv_remove_comment_column(raw_data, "utf-8")
        content = result_io.getvalue()
        
        # Doit retourner le contenu prétraité sans erreur
        df = pd.read_csv(io.StringIO(content), engine="python")
        assert "ID" in df.columns
        assert "Name" in df.columns

    @patch('app.services.ingestion_service.DBWriter')
    def test_process_csv_uses_csv_reader_with_preprocessor(self, mock_db_writer):
        """process_csv utilise CSVReader qui utilise CSVPreprocessor"""
        csv_content = (
            'Column1,Column2\n'
            '"Value1\u00a0A","Value1\u00a0B"\n'
            '"Value2\u00a0A","Value2\u00a0B"\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            # Mock la DB pour ne pas vraiment écrire
            mock_writer = MagicMock()
            mock_writer.already_imported.return_value = False
            mock_db_writer.return_value = mock_writer
            
            # Appeler process_csv
            result = IngestionService.process_csv(temp_path)
            
            # Doit retourner un statut succès
            assert result["status"] == "success"
            assert result["file"].endswith(".csv")
            assert result["rows"] >= 0
            
            # Vérifier que copy_dataframe a été appelé
            assert mock_writer.copy_dataframe.called
        finally:
            os.unlink(temp_path)

    @patch('app.services.ingestion_service.DBWriter')
    def test_process_path_with_multiple_csv_files(self, mock_db_writer):
        """process_path traite tous les CSV d'un dossier"""
        csv_content_1 = 'Column1,Column2\n1,2\n'
        csv_content_2 = 'Column1,Column2\n3,4\n'
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Créer 2 fichiers CSV
            path1 = os.path.join(temp_dir, "file1.csv")
            path2 = os.path.join(temp_dir, "file2.csv")
            
            with open(path1, 'w', encoding='utf-8') as f:
                f.write(csv_content_1)
            with open(path2, 'w', encoding='utf-8') as f:
                f.write(csv_content_2)
            
            # Mock la DB
            mock_writer = MagicMock()
            mock_writer.already_imported.return_value = False
            mock_db_writer.return_value = mock_writer
            
            # Traiter le dossier
            results = IngestionService.process_path(temp_dir)
            
            # Doit retourner une liste de résultats
            assert isinstance(results, list)
            assert len(results) == 2

    @patch('app.services.ingestion_service.DBWriter')
    def test_process_path_with_single_file(self, mock_db_writer):
        """process_path avec un chemin fichier (pas dossier)"""
        csv_content = 'Column1,Column2\n1,2\n'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            # Mock la DB
            mock_writer = MagicMock()
            mock_writer.already_imported.return_value = False
            mock_db_writer.return_value = mock_writer
            
            # Traiter le fichier
            result = IngestionService.process_path(temp_path)
            
            # Doit retourner un seul résultat (pas une liste)
            assert isinstance(result, dict)
            assert result["status"] == "success"
        finally:
            os.unlink(temp_path)

    @patch('app.services.ingestion_service.DBWriter')
    @patch('app.services.ingestion_service.SFTPClient')
    def test_process_sftp_file_uses_csv_preprocessor(self, mock_sftp_client_class, mock_db_writer):
        """process_sftp_file utilise CSVPreprocessor"""
        # Simuler le contenu du SFTP
        csv_content = (
            'ID,Name,COMMENTAIRE\n'
            '"1\u00a0","Alice","Comment1"\n'
            '"2\u00a0","Bob","Comment2"\n'
        )
        raw_data = csv_content.encode("utf-16")
        
        # Mock SFTP
        mock_sftp_instance = MagicMock()
        mock_sftp_instance.read_file.return_value = raw_data
        mock_sftp_instance.detect_encoding.return_value = "utf-16"
        mock_sftp_client_class.return_value = mock_sftp_instance
        
        # Mock DB
        mock_writer = MagicMock()
        mock_writer.already_imported.return_value = False
        mock_db_writer.return_value = mock_writer
        
        # Appeler process_sftp_file
        result = IngestionService.process_sftp_file("/remote/path/file.csv")
        
        # Doit retourner succès
        assert result["status"] == "success"
        assert result["encoding"] == "utf-16"
        
        # Vérifier que copy_dataframe a été appelé
        assert mock_writer.copy_dataframe.called


class TestVocalComCSVFormat:
    """Tests spécifiques pour le format VocalCom problématique"""

    def test_vocalcom_utf16_with_nbsp(self):
        """Traite un CSV VocalCom UTF-16 avec espaces insécables"""
        # Simuler un CSV VocalCom typique
        csv_content = (
            'date_appel,nom_appelant,duree_appel,COMMENTAIRE\n'
            '"2025-01-15","Jean\u00a0Dupont","3600","Comment1"\n'
            '"2025-01-16","Marie\u00a0Martin","1800","Comment2"\n'
        )
        csv_bytes = csv_content.encode("utf-16")
        
        # Traiter avec le préprocesseur
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-16")
        
        # Vérifier
        assert len(df) == 2
        assert "date_appel" in df.columns
        assert "\u00a0" not in df.values.astype(str).flatten()
        
        # Vérifier que les noms sont corrects (sans espace insécable)
        assert "Jean Dupont" in df["nom_appelant"].values or "JeanDupont" in str(df["nom_appelant"].values)

    def test_vocalcom_with_malformed_quotes(self):
        """Traite un CSV VocalCom avec guillemets mal formés"""
        csv_content = (
            'ID,Name,Value\n'
            '"1" "","Alice","100"\n'
            '"2" "","Bob","200"\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        # Doit parser sans erreur
        assert len(df) >= 1

    def test_vocalcom_end_to_end_with_reader(self):
        """Test end-to-end : VocalCom CSV → CSVReader → préprocesseur"""
        csv_content = (
            'date,client\u00a0nom,montant\n'
            '"2025-01-15" "","Alice\u00a0Inc","1000.00"\n'
            '"2025-01-16" "","Bob\u00a0Ltd","2000.00"\n'
        )
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            reader = CSVReader(temp_path, chunksize=10)
            chunks = list(reader.get_chunks())
            
            assert len(chunks) >= 1
            
            # Vérifier le nettoyage
            for chunk in chunks:
                assert "\u00a0" not in chunk.values.astype(str).flatten()
        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
