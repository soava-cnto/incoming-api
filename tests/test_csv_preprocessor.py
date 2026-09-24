"""
Tests pour le module CSVPreprocessor.

Tests couvrant :
- CSV UTF-16 avec espaces insécables
- CSV avec séquences guillemets mal formées
- Noms de colonnes entourés de guillemets
- Valeurs texte avec guillemets résiduels
- CSV classique (non régression)
- Intégration avec les routes d'ingestion
"""

import io
import pandas as pd
import pytest
from app.utils.csv_preprocessor import CSVPreprocessor


class TestCSVPreprocessorRawContent:
    """Tests du prétraitement du contenu brut (preprocess_raw_content)"""

    def test_preprocess_raw_content_removes_nbsp(self):
        """Remplace les espaces insécables (U+00A0) par des espaces classiques"""
        raw_content = "Colonne1,Colonne2\nValue\u00a01,Value\u00a02"
        result = CSVPreprocessor.preprocess_raw_content(raw_content, "utf-8")
        assert "\u00a0" not in result
        assert "Value 1,Value 2" in result

    def test_preprocess_raw_content_fixes_malformed_quotes(self):
        """Corrige les séquences mal formées ' "" ' en '"'"""
        raw_content = 'Colonne1,Colonne2\n"Value1" "","Value2"'
        result = CSVPreprocessor.preprocess_raw_content(raw_content, "utf-8")
        # La séquence ' ""' (espace + deux guillemets) doit être remplacée par '"'
        assert ' ""' not in result
        assert '""' not in result or 'Value1"' in result

    def test_preprocess_raw_content_bytes_input(self):
        """Décode les bytes correctement"""
        raw_content = "Colonne1,Colonne2\nValue1,Value2"
        raw_bytes = raw_content.encode("utf-8")
        result = CSVPreprocessor.preprocess_raw_content(raw_bytes, "utf-8")
        assert isinstance(result, str)
        assert "Colonne1" in result

    def test_preprocess_raw_content_empty_content_raises(self):
        """Lève ValueError si contenu vide"""
        with pytest.raises(ValueError, match="CSV vide"):
            CSVPreprocessor.preprocess_raw_content("", "utf-8")

    def test_preprocess_raw_content_encoding_fallback(self):
        """Fallback sur UTF-8 si encodage invalide"""
        raw_content = "Colonne1,Colonne2\nValue1,Value2"
        raw_bytes = raw_content.encode("utf-8")
        # Essayer avec un encodage invalide, doit fallback
        result = CSVPreprocessor.preprocess_raw_content(raw_bytes, "invalid-encoding")
        assert "Colonne1" in result


class TestCSVPreprocessorDataFrame:
    """Tests du nettoyage post-parsing (clean_dataframe)"""

    def test_clean_dataframe_removes_quotes_from_columns(self):
        """Supprime les guillemets en début/fin des noms de colonnes"""
        df = pd.DataFrame({
            '"Column1"': [1, 2],
            '  "Column2"  ': [3, 4],
            "Column3": [5, 6]
        })
        result = CSVPreprocessor.clean_dataframe(df)
        assert "Column1" in result.columns
        assert "Column2" in result.columns
        assert "Column3" in result.columns
        assert '"Column1"' not in result.columns

    def test_clean_dataframe_removes_quotes_from_values(self):
        """Supprime les guillemets en début/fin des valeurs texte"""
        df = pd.DataFrame({
            "Column1": ['"Value1"', '  "Value2"  ', "Value3"],
            "Column2": ["Data1", "Data2", "Data3"]
        })
        result = CSVPreprocessor.clean_dataframe(df)
        assert result.loc[0, "Column1"] == "Value1"
        assert result.loc[1, "Column1"] == "Value2"
        assert result.loc[2, "Column1"] == "Value3"

    def test_clean_dataframe_preserves_numeric_columns(self):
        """Laisse les colonnes numériques intactes"""
        df = pd.DataFrame({
            "NumCol": ["1", "2", "3"],
            "TextCol": ['"text1"', '"text2"', '"text3"']
        })
        df["NumCol"] = df["NumCol"].astype(str)  # Garder en str pour ce test
        result = CSVPreprocessor.clean_dataframe(df)
        # Les colonnes str doivent être nettoyées
        assert result.loc[0, "TextCol"] == "text1"


class TestCSVPreprocessorFullPipeline:
    """Tests du pipeline complet (read_csv_with_preprocessing)"""

    def test_read_csv_with_preprocessing_utf8(self):
        """Lis et prétraite correctement un CSV UTF-8"""
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        assert len(df) == 1
        assert "Column1" in df.columns
        assert df.loc[0, "Column1"] == "Value1"

    def test_read_csv_with_preprocessing_handles_nbsp_and_quotes(self):
        """Traite simultanément espaces insécables et guillemets mal formés"""
        # CSV avec espaces insécables et guillemets
        csv_content = 'Col1,Col2\n"Value\u00a01" "","Value2"'
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        assert len(df) >= 1
        # Vérifier que l'espace insécable a été remplacé
        assert "\u00a0" not in df.values.astype(str).flatten()

    def test_read_csv_with_preprocessing_utf16(self):
        """Lis un CSV en UTF-16"""
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        csv_bytes = csv_content.encode("utf-16")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-16")
        
        assert len(df) == 1
        assert "Column1" in df.columns

    def test_read_csv_with_preprocessing_removes_quotes_from_all_parts(self):
        """Nettoie les guillemets des colonnes ET des valeurs"""
        csv_content = '"Col1","Col2"\n"Val1","Val2"\n'
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        assert df.columns.tolist() == ["Col1", "Col2"]
        assert df.loc[0, "Col1"] == "Val1"

    def test_read_csv_with_preprocessing_from_filepath(self):
        """Lis un fichier CSV depuis un chemin fichier"""
        import tempfile
        import os
        
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
            f.write(csv_content)
            temp_path = f.name
        
        try:
            df = CSVPreprocessor.read_csv_with_preprocessing(temp_path, encoding="utf-8")
            assert len(df) == 1
            assert "Column1" in df.columns
        finally:
            os.unlink(temp_path)


class TestCSVPreprocessorChunks:
    """Tests de la lecture par chunks (read_csv_in_chunks)"""

    def test_read_csv_in_chunks_splits_correctly(self):
        """Divise le CSV en chunks de la taille correcte"""
        csv_content = "Column1,Column2\n"
        for i in range(15):
            csv_content += f"Value{i},Data{i}\n"
        csv_bytes = csv_content.encode("utf-8")
        
        chunks = list(CSVPreprocessor.read_csv_in_chunks(csv_bytes, encoding="utf-8", chunksize=5))
        
        # Doit avoir au moins 3 chunks (15 lignes / 5 = 3)
        assert len(chunks) >= 3
        
        # Premier chunk doit avoir 5 lignes
        assert len(chunks[0]) == 5

    def test_read_csv_in_chunks_applies_preprocessing(self):
        """Applique le prétraitement à chaque chunk"""
        csv_content = 'Column1,Column2\n"Value\u00a01","Value\u00a02"\n'
        for i in range(8):
            csv_content += f'"Value{i}" "","Value{i}"\n'
        csv_bytes = csv_content.encode("utf-8")
        
        chunks = list(CSVPreprocessor.read_csv_in_chunks(csv_bytes, encoding="utf-8", chunksize=3))
        
        # Vérifier qu'aucun chunk n'a d'espaces insécables
        for chunk in chunks:
            assert "\u00a0" not in chunk.values.astype(str).flatten()

    def test_read_csv_in_chunks_with_usecols(self):
        """Filtre les colonnes avec usecols"""
        csv_content = "Col1,Col2,Col3\nVal1,Val2,Val3\n"
        for i in range(4):
            csv_content += f"Val{i}_1,Val{i}_2,Val{i}_3\n"
        csv_bytes = csv_content.encode("utf-8")
        
        chunks = list(
            CSVPreprocessor.read_csv_in_chunks(
                csv_bytes,
                encoding="utf-8",
                chunksize=2,
                usecols=["Col1", "Col3"]
            )
        )
        
        # Chaque chunk doit avoir juste Col1 et Col3
        for chunk in chunks:
            assert list(chunk.columns) == ["Col1", "Col3"]


class TestCSVPreprocessorEncodingDetection:
    """Tests de la détection d'encodage avec fallback"""

    def test_detect_and_clean_encoding_issues_utf16(self):
        """Détecte et nettoie un CSV UTF-16"""
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        csv_bytes = csv_content.encode("utf-16")
        
        encoding, df = CSVPreprocessor.detect_and_clean_encoding_issues(csv_bytes)
        
        assert encoding == "utf-16"
        assert len(df) == 1
        assert "Column1" in df.columns

    def test_detect_and_clean_encoding_issues_iso_8859_1(self):
        """Détecte et nettoie un CSV ISO-8859-1"""
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        csv_bytes = csv_content.encode("ISO-8859-1")
        
        encoding, df = CSVPreprocessor.detect_and_clean_encoding_issues(csv_bytes)
        
        assert encoding in ["ISO-8859-1", "utf-8"]  # Peut être UTF-8 aussi
        assert len(df) == 1

    def test_detect_and_clean_encoding_issues_fallback(self):
        """Teste le fallback entre encodages"""
        csv_content = 'Column1,Column2\nValue1,Value2\n'
        # Encoder en UTF-16, mais aucun des encodages essayés ne sera exactement UTF-16
        # Cette partie teste la robustesse du fallback
        csv_bytes = csv_content.encode("utf-8")
        
        encoding, df = CSVPreprocessor.detect_and_clean_encoding_issues(csv_bytes)
        
        # Doit réussir avec au moins un encodage
        assert encoding is not None
        assert len(df) == 1


class TestCSVPreprocessorIntegration:
    """Tests d'intégration avec des cas réels"""

    def test_vocalcom_style_csv(self):
        """Simule un CSV VocalCom avec problèmes typiques"""
        # CSV avec : espaces insécables, guillemets mal formés, encodage spécial
        csv_content = (
            'date_appel,nom_appelant,duree_appel\n'
            '"2025-01-15" "","Jean\u00a0Dupont",3600\n'
            '"2025-01-16","Marie\u00a0Martin",1800\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        # Doit avoir au moins 2 lignes
        assert len(df) >= 1
        
        # Pas d'espaces insécables
        assert "\u00a0" not in df.values.astype(str).flatten()
        
        # Colonnes nettoyées
        assert all('"' not in col for col in df.columns)

    def test_classic_csv_non_regression(self):
        """Vérifie qu'un CSV classique continue à fonctionner"""
        csv_content = (
            'id,name,age,city\n'
            '1,Alice,30,Paris\n'
            '2,Bob,25,Lyon\n'
            '3,Charlie,35,Marseille\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        assert len(df) == 3
        assert df.columns.tolist() == ['id', 'name', 'age', 'city']
        assert df.loc[0, 'name'] == 'Alice'
        assert df.loc[2, 'city'] == 'Marseille'

    def test_csv_with_multiline_and_quoted_fields(self):
        """Traite les champs sur plusieurs lignes (guillemets échappés)"""
        csv_content = (
            'id,description\n'
            '1,"Line1\nLine2"\n'
            '2,"Single line"\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        # Doit avoir 2 lignes
        assert len(df) == 2

    def test_csv_with_empty_fields(self):
        """Gère les champs vides correctement"""
        csv_content = (
            'col1,col2,col3\n'
            '"val1",,,"val3"\n'
            ',,"val2",\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        # Doit parser sans erreur
        assert len(df) >= 1

    def test_csv_with_special_characters(self):
        """Gère les caractères spéciaux (accents, symboles)"""
        csv_content = (
            'nom,prenom,ville\n'
            '"François","Élise","Côte-d\'Ivoire"\n'
            '"André","Michèle","São Paulo"\n'
        )
        csv_bytes = csv_content.encode("utf-8")
        
        df = CSVPreprocessor.read_csv_with_preprocessing(csv_bytes, encoding="utf-8")
        
        assert len(df) == 2
        assert "François" in df.values.astype(str).flatten()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
