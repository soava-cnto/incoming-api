"""
Tests pour CSVPreprocessor et le traitement des fichiers CSV problématiques.

Teste :
- Nettoyage des espaces insécables (U+00A0)
- Correction des guillemets mal formés
- Différents encodages (UTF-16, ISO-8859-1, etc.)
- Exclusion de la colonne COMMENTAIRE
- Compatibilité avec les CSV valides
"""

import io
import unittest
import pandas as pd
import tempfile
import os

from app.utils.csv_preprocessor import CSVPreprocessor
from app.csv_reader import CSVReader


class TestCSVPreprocessor(unittest.TestCase):
    """Tests du préprocesseur CSVPreprocessor"""

    def test_preprocess_nonbreaking_space(self):
        """Test : remplacement des espaces insécables (U+00A0)"""
        raw_content = "Nom,Valeur\nJean\u00a0Dupont,100\nMarie,200"
        expected = "Nom,Valeur\nJean Dupont,100\nMarie,200"
        
        result = CSVPreprocessor.preprocess_raw_content(raw_content, "utf-8")
        self.assertEqual(result, expected)

    def test_preprocess_malformed_quotes(self):
        """Test : correction de la séquence ' "" ' en fin de ligne"""
        raw_content = 'Nom,Valeur\n"Jean"" , 100\n"Marie"" , 200'
        
        result = CSVPreprocessor.preprocess_raw_content(raw_content, "utf-8")
        # La séquence ' ""' doit devenir '"'
        self.assertIn('"Jean"', result)
        # Vérifier que ' ""' a été remplacé
        self.assertNotIn(' ""', result)

    def test_clean_dataframe_columns(self):
        """Test : nettoyage des noms de colonnes (guillemets/espaces)"""
        df = pd.DataFrame({
            '"Nom"': ['Jean', 'Marie'],
            ' "Valeur" ': ['100', '200']
        })
        
        result = CSVPreprocessor.clean_dataframe(df)
        
        # Les noms de colonnes ne doivent pas avoir de guillemets/espaces en début/fin
        self.assertIn('Nom', result.columns)
        self.assertIn('Valeur', result.columns)

    def test_clean_dataframe_values(self):
        """Test : nettoyage des valeurs texte (guillemets/espaces)"""
        df = pd.DataFrame({
            'Nom': ['"Jean"', ' "Marie" '],
            'Valeur': ['100', '200']
        })
        
        result = CSVPreprocessor.clean_dataframe(df)
        
        # Les valeurs ne doivent pas avoir de guillemets/espaces en début/fin
        self.assertIn('Jean', result['Nom'].values)
        self.assertIn('Marie', result['Nom'].values)

    def test_read_csv_with_preprocessing_utf16(self):
        """Test : lecture d'un CSV UTF-16 avec prétraitement"""
        # Créer un fichier CSV UTF-16 avec espaces insécables
        content = "Nom,Valeur\nJean\u00a0Dupont,100\nMarie,200"
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-16', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            df = CSVPreprocessor.read_csv_with_preprocessing(
                temp_path,
                encoding="utf-16"
            )
            
            # Vérifier que les espaces insécables ont été remplacés
            self.assertEqual(len(df), 2)
            self.assertIn('Jean Dupont', df['Nom'].values)
            self.assertNotIn('Jean\u00a0Dupont', df['Nom'].values)
        finally:
            os.unlink(temp_path)

    def test_read_csv_in_chunks_with_preprocessing(self):
        """Test : lecture par chunks avec prétraitement"""
        # Créer un fichier CSV avec données
        content = "Nom,Valeur\n" + "\n".join([f"Personne{i},100" for i in range(10)])
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            chunks = list(CSVPreprocessor.read_csv_in_chunks(
                temp_path,
                encoding="utf-8",
                chunksize=3
            ))
            
            # Vérifier que les données sont bien partitionnées
            self.assertGreater(len(chunks), 1)
            total_rows = sum(len(chunk) for chunk in chunks)
            self.assertEqual(total_rows, 10)
        finally:
            os.unlink(temp_path)

    def test_read_csv_exclude_commentary_column(self):
        """Test : exclusion de la colonne COMMENTAIRE"""
        content = 'Nom,Valeur,COMMENTAIRE\n"Jean",100,"Pas de commentaire"\n"Marie",200,"Bon"'
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            df = CSVPreprocessor.read_csv_with_preprocessing(
                temp_path,
                encoding="utf-8",
                usecols=['Nom', 'Valeur']
            )
            
            # La colonne COMMENTAIRE ne doit pas être présente
            self.assertNotIn('COMMENTAIRE', df.columns)
            self.assertIn('Nom', df.columns)
            self.assertIn('Valeur', df.columns)
        finally:
            os.unlink(temp_path)

    def test_read_csv_valid_csv_still_works(self):
        """Test : les CSV valides doivent continuer à fonctionner"""
        content = "Nom,Valeur,Date\nJean,100,2026-01-01\nMarie,200,2026-01-02"
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-8', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            df = CSVPreprocessor.read_csv_with_preprocessing(
                temp_path,
                encoding="utf-8"
            )
            
            # Vérifier que les données sont correctement lues
            self.assertEqual(len(df), 2)
            self.assertEqual(df['Nom'].iloc[0], 'Jean')
            self.assertEqual(df['Valeur'].iloc[1], '200')
        finally:
            os.unlink(temp_path)

    def test_empty_file_raises_error(self):
        """Test : un fichier vide lève une erreur"""
        with self.assertRaises(ValueError):
            CSVPreprocessor.preprocess_raw_content("", "utf-8")

    def test_detect_and_clean_encoding_issues(self):
        """Test : détection et nettoyage avec fallback sur encodages multiples"""
        content = "Nom,Valeur\nJean\u00a0Dupont,100"
        
        # Tester avec UTF-16
        raw_utf16 = content.encode('utf-16')
        encoding, df = CSVPreprocessor.detect_and_clean_encoding_issues(raw_utf16)
        
        self.assertEqual(len(df), 1)
        self.assertIn('Jean Dupont', df['Nom'].values)


class TestCSVReader(unittest.TestCase):
    """Tests du CSVReader avec CSVPreprocessor"""

    def test_csv_reader_with_nonbreaking_spaces(self):
        """Test : CSVReader gère les espaces insécables"""
        content = "Nom,Valeur\nJean\u00a0Dupont,100\nMarie,200"
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='ISO-8859-1', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            reader = CSVReader(temp_path, chunksize=10)
            chunks = list(reader.get_chunks())
            
            # Vérifier que les données sont correctes
            self.assertEqual(len(chunks), 1)
            df = chunks[0]
            self.assertEqual(len(df), 2)
            # Les espaces insécables doivent avoir été remplacés
            self.assertIn('Jean Dupont', df['Nom'].values)
        finally:
            os.unlink(temp_path)

    def test_csv_reader_exclude_comment_column(self):
        """Test : CSVReader exclut la colonne COMMENTAIRE"""
        content = "Nom,Valeur,COMMENTAIRE\nJean,100,Commentaire1\nMarie,200,Commentaire2"
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='ISO-8859-1', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            reader = CSVReader(temp_path, chunksize=10, include_comment=False)
            chunks = list(reader.get_chunks())
            
            # Vérifier que la colonne COMMENTAIRE n'est pas présente
            df = chunks[0]
            self.assertNotIn('COMMENTAIRE', df.columns)
            self.assertIn('Nom', df.columns)
            self.assertIn('Valeur', df.columns)
        finally:
            os.unlink(temp_path)

    def test_csv_reader_include_comment_column(self):
        """Test : CSVReader peut inclure la colonne COMMENTAIRE si demandé"""
        content = "Nom,Valeur,COMMENTAIRE\nJean,100,Commentaire1\nMarie,200,Commentaire2"
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='ISO-8859-1', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            reader = CSVReader(temp_path, chunksize=10, include_comment=True)
            chunks = list(reader.get_chunks())
            
            # Vérifier que la colonne COMMENTAIRE est présente
            df = chunks[0]
            self.assertIn('COMMENTAIRE', df.columns)
        finally:
            os.unlink(temp_path)


class TestIntegrationVocalComFile(unittest.TestCase):
    """Tests d'intégration simulent un fichier VocalCom"""

    def test_vocalcom_like_file(self):
        """Test : simulation d'un fichier VocalCom avec problèmes typiques"""
        # Simule un fichier avec:
        # - Espaces insécables
        # - Guillemets mal formés
        # - UTF-16
        content = (
            'Date,Agent\u00a0Nom,Durée\n'
            '2026-01-01 "",100\n'
            '2026-01-02 "",200'
        )
        
        with tempfile.NamedTemporaryFile(
            mode='w', 
            encoding='utf-16', 
            suffix='.csv', 
            delete=False
        ) as f:
            f.write(content)
            temp_path = f.name

        try:
            df = CSVPreprocessor.read_csv_with_preprocessing(
                temp_path,
                encoding="utf-16"
            )
            
            # Les espaces insécables doivent avoir été remplacés
            self.assertIn('Agent Nom', df.columns)
            self.assertEqual(len(df), 2)
        finally:
            os.unlink(temp_path)


if __name__ == '__main__':
    unittest.main()
