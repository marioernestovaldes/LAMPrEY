import pandas as pd
import logging
from pathlib import Path as P
import csv


MAXQUANT_STANDARDS = {
    "proteinGroups.txt": {
        "usecols": [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            28,
            29,
            30,
            31,
            55,
            57,
            58,
            59,
            60,
            61,
            62,
            63,
            64,
            65,
            66,
            67,
            68,
            69,
        ],
        "column_names": [
            "Protein IDs",
            "Majority protein IDs",
            "Peptide counts all",
            "Peptide counts (razor+unique)",
            "Peptide counts (unique)",
            "Fasta headers",
            "Number of proteins",
            "Peptides",
            "Razor + unique peptides",
            "Unique peptides",
            "Sequence coverage [%]",
            "Unique + razor sequence coverage [%]",
            "Unique sequence coverage [%]",
            "Mol. weight [kDa]",
            "Sequence length",
            "Sequence lengths",
            "Q-value",
            "Score",
            "Reporter intensity corrected 1",
            "Reporter intensity corrected 2",
            "Reporter intensity corrected 3",
            "Reporter intensity corrected 4",
            "Reporter intensity corrected 5",
            "Reporter intensity corrected 6",
            "Reporter intensity corrected 7",
            "Reporter intensity corrected 8",
            "Reporter intensity corrected 9",
            "Reporter intensity corrected 10",
            "Reporter intensity corrected 11",
            "Intensity",
            "MS/MS count",
            "Only identified by site",
            "Reverse",
            "Potential contaminant",
            "id",
            "Peptide IDs",
            "Peptide is razor",
            "Mod. peptide IDs",
            "Evidence IDs",
            "MS/MS IDs",
            "Best MS/MS",
            "Oxidation (M) site IDs",
            "Oxidation (M) site positions",
        ],
    }
}


class MaxquantParseError(ValueError):
    pass


class MaxquantReader:
    REQUIRED_PROTEIN_GROUP_COLUMNS = [
        "Majority protein IDs",
        "Fasta headers",
        "Score",
        "Intensity",
    ]

    OPTIONAL_PROTEIN_GROUP_COLUMNS = [
        "Number of proteins",
        "Peptides",
        "Razor + unique peptides",
        "Unique peptides",
        "Sequence coverage [%]",
        "Unique + razor sequence coverage [%]",
        "Unique sequence coverage [%]",
        "Mol. weight [kDa]",
        "Sequence length",
        "Sequence lengths",
        "Q-value",
        "MS/MS count",
        "Only identified by site",
        "Reverse",
        "Potential contaminant",
        "id",
        "Peptide IDs",
        "Peptide is razor",
        "Mod. peptide IDs",
        "Evidence IDs",
        "MS/MS IDs",
        "Best MS/MS",
        "Oxidation (M) site IDs",
        "Oxidation (M) site positions",
    ]

    def __init__(self, standardize=True, remove_contaminants=True, remove_reverse=True):
        self.standards = MAXQUANT_STANDARDS
        self.standardize = standardize
        self.remove_con = remove_contaminants
        self.remove_rev = remove_reverse

    @staticmethod
    def _detect_separator(fn, default="\t"):
        try:
            with open(fn, "r", encoding="utf-8", errors="ignore", newline="") as handle:
                sample = handle.read(8192)
        except OSError:
            return default
        if not sample:
            return default
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters="\t,;")
            return dialect.delimiter
        except csv.Error:
            header = sample.splitlines()[0] if sample.splitlines() else sample
            if header.count(",") > header.count("\t"):
                return ","
            if header.count(";") > header.count("\t"):
                return ";"
            return default

    def read(self, fn):
        assert P(fn).is_file(), fn
        name = P(fn).name
        sep = self._detect_separator(fn)

        try:
            df = pd.read_csv(fn, sep=sep, low_memory=False, na_filter=None)
        except MaxquantParseError:
            raise
        except Exception as e:
            logging.warning(f"MaxQuantReader: {e}")
            return None

        if name == "proteinGroups.txt":
            return self.process_protein_groups(df)

        return df

    def process_protein_groups(
        self,
        df,
    ):
        missing_required = [
            col for col in self.REQUIRED_PROTEIN_GROUP_COLUMNS if col not in df.columns
        ]
        if missing_required:
            missing = ", ".join(missing_required)
            raise MaxquantParseError(
                f"proteinGroups.txt is missing required columns: {missing}"
            )

        quant_cols = df.filter(regex="Reporter intensity corrected").columns.to_list()
        optional_cols = [
            col for col in self.OPTIONAL_PROTEIN_GROUP_COLUMNS if col in df.columns
        ]

        df = df[self.REQUIRED_PROTEIN_GROUP_COLUMNS + optional_cols + quant_cols].rename(
            columns={c: " ".join(i for i in c.split(" ")[:4]) for c in quant_cols}
        )

        if self.remove_con and "Potential contaminant" in df.columns:
            df = df[df["Potential contaminant"] != "+"]
        if self.remove_rev:
            df = df[~df["Majority protein IDs"].astype(str).str.contains("REV_", na=False)]

        return df
