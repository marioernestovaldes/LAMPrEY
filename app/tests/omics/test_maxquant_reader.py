import pandas as pd
from pathlib import Path as P
import pytest

from omics.proteomics import MaxquantReader
from omics.proteomics.maxquant.MaxquantReader import MaxquantParseError


PATH = P("tests/omics/data")


class TestMaxquantReader:
    def test__process_protein_groups_accepts_reduced_optional_schema(self):
        df = pd.DataFrame(
            {
                "Majority protein IDs": ["P1", "REV_P2"],
                "Fasta headers": ["header-1", "header-2"],
                "Score": [12.3, 7.5],
                "Intensity": [1000, 2000],
                "Reporter intensity corrected 1 sample": [11, 22],
            }
        )

        reader = MaxquantReader(remove_contaminants=True, remove_reverse=True)

        actual = reader.process_protein_groups(df)

        assert list(actual.columns) == [
            "Majority protein IDs",
            "Fasta headers",
            "Score",
            "Intensity",
            "Reporter intensity corrected 1",
        ]
        assert actual["Majority protein IDs"].tolist() == ["P1"]

    def test__process_protein_groups_raises_controlled_error_for_missing_required_columns(
        self,
    ):
        df = pd.DataFrame(
            {
                "Majority protein IDs": ["P1"],
                "Fasta headers": ["header-1"],
                "Intensity": [1000],
            }
        )

        reader = MaxquantReader()

        with pytest.raises(MaxquantParseError) as excinfo:
            reader.process_protein_groups(df)

        assert "missing required columns" in str(excinfo.value)
        assert "Score" in str(excinfo.value)

    def test__read_protein_groups_detects_comma_delimited_file(self, tmp_path):
        fn = tmp_path / "proteinGroups.txt"
        fn.write_text(
            "Majority protein IDs,Fasta headers,Score,Intensity,Reporter intensity corrected 1 sample\n"
            "P1,header-1,12.3,1000,11\n",
            encoding="utf-8",
        )

        reader = MaxquantReader()
        actual = reader.read(fn)

        assert isinstance(actual, pd.DataFrame)
        assert actual["Majority protein IDs"].tolist() == ["P1"]
        assert "Reporter intensity corrected 1" in actual.columns

    def test__read_tmt11_protein_groups_example1(self):
        fn = PATH / "maxquant" / "tmt11" / "example-1" / "proteinGroups.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame)

    def test__read_tmt11_allPeptides_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "allPeptides.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_peptides_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "peptides.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_evidence_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "evidence.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_msms_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "msms.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_mzRange_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "mzRange.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_parameters_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "parameters.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)

    def test__read_tmt11_protein_groups_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "proteinGroups.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame)

    def test__read_tmt11_summary_example0(self):
        fn = PATH / "maxquant" / "tmt11" / "example-0" / "summary.txt"
        reader = MaxquantReader()
        df = reader.read(fn)
        assert isinstance(df, pd.DataFrame), type(df)
