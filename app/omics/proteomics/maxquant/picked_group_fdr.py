import json
import logging
import os
import re
import subprocess
import sys
import traceback
from functools import lru_cache
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

import pandas as pd

from omics.proteomics.maxquant.MqparParser import MqparParser


logger = logging.getLogger(__name__)

PICKED_GROUP_FDR_REQUIRED_PROTEIN_FDR = 1.0
PICKED_GROUP_FDR_REQUIRED_PEPTIDE_FDR = 1.0
PICKED_GROUP_FDR_FDR_CUTOFF = 0.01
PICKED_GROUP_FDR_LFQ_MIN_PEPTIDE_RATIOS = 1
PICKED_GROUP_FDR_MOKAPOT_MAX_WORKERS = 8

MQ_ENZYME_TO_PICKED_GROUP_FDR = {
    "Trypsin": "trypsin",
    "Trypsin/P": "trypsinp",
    "LysC": "lys-c",
    "Lys-C": "lys-c",
    "LysN": "lys-n",
    "Lys-N": "lys-n",
    "ArgC": "arg-c",
    "Arg-C": "arg-c",
    "AspN": "asp-n",
    "Asp-N": "asp-n",
    "GluC": "glu-c",
    "Glu-C": "glu-c",
    "Chymotrypsin": "chymotrypsin",
    "Chymotrypsin+": "chymotrypsin+",
    "No enzyme": "no_enzyme",
    "Unspecific": "no_enzyme",
}

PICKED_GROUP_FDR_MOKAPOT_FAILURE_PREFIX = (
    "Picked-group-FDR correction was not successful because Mokapot did not complete "
    "successfully. Picked-group-FDR requires a successful Mokapot rescoring step for this "
    "integration."
)

PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS = "proteinGroups.picked_group_fdr.txt"
PICKED_GROUP_FDR_MOKAPOT_PEPTIDES = "andromeda.mokapot.peptides.txt"


def _isoformat(value):
    if value is None:
        return None
    try:
        return value.isoformat()
    except AttributeError:
        return str(value)


def _result_sort_key(result):
    raw = getattr(result.raw_file, "logical_name", "") or getattr(result.raw_file, "name", "")
    return (str(raw), int(result.pk))


def _read_mqpar_settings(mqpar_path):
    parser = MqparParser(mqpar_path)
    enzyme_names = parser.findall_text("parameterGroups/parameterGroup/enzymes/string")
    enzyme_name = enzyme_names[0] if enzyme_names else None
    enzyme_mode = parser.find_text("parameterGroups/parameterGroup/enzymeMode")
    min_length = parser.find_float("minPepLen", default=7)
    max_missed_cleavages = parser.find_float(
        "parameterGroups/parameterGroup/maxMissedCleavages", default=2
    )
    protein_fdr = parser.find_float("proteinFdr")
    peptide_fdr = parser.find_float("peptideFdr")
    site_fdr = parser.find_float("siteFdr")

    digestion = "full"
    if enzyme_mode is not None and str(enzyme_mode) != "0":
        digestion = "unsupported"

    picked_group_enzyme = MQ_ENZYME_TO_PICKED_GROUP_FDR.get(enzyme_name)
    return {
        "protein_fdr": protein_fdr,
        "peptide_fdr": peptide_fdr,
        "site_fdr": site_fdr,
        "enzyme_name": enzyme_name,
        "enzyme_mode": enzyme_mode,
        "picked_group_enzyme": picked_group_enzyme,
        "min_length": int(min_length) if min_length is not None else 7,
        "max_missed_cleavages": (
            int(max_missed_cleavages) if max_missed_cleavages is not None else 2
        ),
        "digestion": digestion,
    }


def _validate_required_mqpar_fdr(mqpar_settings, settings_key, xml_name, required_value):
    actual_value = mqpar_settings.get(settings_key)
    if actual_value is None:
        return f"mqpar.xml is missing the {xml_name} setting."
    if abs(float(actual_value) - required_value) > 1e-9:
        return (
            f"mqpar.xml must set {xml_name} to {required_value:g} for picked-group-FDR, "
            f"but found {actual_value}."
        )
    return None


def format_picked_group_fdr_failure(exc):
    message = str(exc).strip() or exc.__class__.__name__
    lowered = message.lower()
    if "no decoy psms were detected" in lowered:
        detail = (
            "Mokapot reported that no decoy PSMs were detected in the generated PIN files."
        )
    elif "negative dimensions are not allowed" in lowered:
        detail = (
            "Mokapot failed during confidence assignment and PEP estimation "
            "(triqler/qvality reported 'negative dimensions are not allowed')."
        )
    elif "mokapot" in lowered:
        detail = f"Mokapot reported: {message}"
    else:
        detail = f"Underlying error: {message}"

    return (
        f"{PICKED_GROUP_FDR_MOKAPOT_FAILURE_PREFIX} {detail} "
        "See picked_group_fdr.err for the full traceback."
    )


def normalize_majority_protein_ids(value):
    if value is None:
        return frozenset()
    tokens = []
    for token in str(value).split(";"):
        cleaned = token.strip()
        if not cleaned:
            continue
        if cleaned.startswith("CON__"):
            continue
        tokens.append(cleaned)
    return frozenset(tokens)


def _pipeline_root_from_txt_path(txt_path):
    candidate = Path(txt_path)
    if candidate.is_file():
        candidate = candidate.parent
    candidate = candidate.resolve()

    for path in (candidate,) + tuple(candidate.parents):
        if (path / "config" / "mqpar.xml").is_file() and (path / "output").is_dir():
            return path
    return None


def _candidate_picked_group_fdr_runs(pipeline_root):
    picked_root = Path(pipeline_root) / "output" / "picked_group_fdr"
    if not picked_root.is_dir():
        return []
    return sorted(
        [path for path in picked_root.iterdir() if path.is_dir()],
        key=lambda path: path.name,
        reverse=True,
    )


def _load_completed_picked_group_fdr_manifest(run_dir):
    manifest_path = Path(run_dir) / "manifest.json"
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if manifest.get("status") != "completed":
        return None
    return manifest


def latest_successful_picked_group_fdr_file(txt_path):
    pipeline_root = _pipeline_root_from_txt_path(txt_path)
    if pipeline_root is None:
        return None

    for run_dir in _candidate_picked_group_fdr_runs(pipeline_root):
        filtered_path = run_dir / f"proteinGroups.fdr{PICKED_GROUP_FDR_FDR_CUTOFF*100:g}.txt"
        if not filtered_path.is_file():
            continue
        manifest = _load_completed_picked_group_fdr_manifest(run_dir)
        if manifest is None:
            continue
        return filtered_path
    return None


def latest_successful_picked_group_fdr_evidence_file(txt_path):
    pipeline_root = _pipeline_root_from_txt_path(txt_path)
    if pipeline_root is None:
        return None

    for run_dir in _candidate_picked_group_fdr_runs(pipeline_root):
        combined_evidence_path = run_dir / "evidence.txt"
        manifest = _load_completed_picked_group_fdr_manifest(run_dir)
        if manifest is None:
            continue
        artifact_path = (
            manifest.get("artifacts", {}).get("combined_evidence")
            or manifest.get("artifacts", {}).get("evidence")
        )
        if artifact_path and Path(artifact_path).is_file():
            return Path(artifact_path)
        if combined_evidence_path.is_file():
            return combined_evidence_path
    return None


def latest_successful_picked_group_fdr_peptides_file(txt_path):
    pipeline_root = _pipeline_root_from_txt_path(txt_path)
    if pipeline_root is None:
        return None

    for run_dir in _candidate_picked_group_fdr_runs(pipeline_root):
        peptides_path = run_dir / PICKED_GROUP_FDR_MOKAPOT_PEPTIDES
        if not peptides_path.is_file():
            continue
        manifest = _load_completed_picked_group_fdr_manifest(run_dir)
        if manifest is None:
            continue
        artifact_path = manifest.get("artifacts", {}).get("mokapot_peptides")
        if artifact_path and Path(artifact_path).is_file():
            return Path(artifact_path)
        return peptides_path
    return None


def picked_group_fdr_output_is_newer(txt_path, target_path):
    filtered_path = latest_successful_picked_group_fdr_file(txt_path)
    if filtered_path is None:
        return False
    try:
        return Path(filtered_path).stat().st_mtime > Path(target_path).stat().st_mtime
    except OSError:
        return False


@lru_cache(maxsize=64)
def _accepted_majority_protein_id_sets(filtered_path_str):
    filtered_path = Path(filtered_path_str)
    if not filtered_path.is_file():
        return frozenset()

    try:
        header, *rows = filtered_path.read_text(
            encoding="utf-8", errors="ignore"
        ).splitlines()
    except OSError:
        return frozenset()
    columns = header.lstrip("\ufeff").split("\t")
    try:
        majority_idx = columns.index("Majority protein IDs")
    except ValueError:
        return frozenset()

    accepted = set()
    for row in rows:
        parts = row.split("\t")
        if majority_idx >= len(parts):
            continue
        normalized = normalize_majority_protein_ids(parts[majority_idx])
        if normalized:
            accepted.add(normalized)
    return frozenset(accepted)


def normalize_peptide_sequence(value):
    if value is None:
        return ""
    peptide = str(value).strip()
    if not peptide:
        return ""

    # Mokapot writes Andromeda peptides with flanking residues, e.g. -.PEPTIDE.K.
    match = re.match(r"^[^.]*\.(.*)\.[^.]*$", peptide)
    if match:
        peptide = match.group(1)

    peptide = re.sub(r"[^A-Za-z]", "", peptide)
    return peptide.upper()


def _truthy_label(value):
    return str(value).strip().lower() in {"true", "1", "t", "target", "yes"}


def _has_non_contaminant_target_protein(value):
    if value is None:
        return True
    proteins = [token.strip() for token in re.split(r"[;,]", str(value)) if token.strip()]
    if not proteins:
        return True
    for protein in proteins:
        if protein.startswith("CON__") or protein.startswith("REV__"):
            continue
        return True
    return False


@lru_cache(maxsize=64)
def _accepted_peptide_sequences_from_mokapot(peptides_path_str):
    peptides_path = Path(peptides_path_str)
    if not peptides_path.is_file():
        return frozenset()

    try:
        df = _read_tsv_table(peptides_path)
    except Exception:
        return frozenset()
    if df.empty or "Peptide" not in df.columns:
        return frozenset()

    mask = pd.Series(True, index=df.index)
    if "Label" in df.columns:
        mask &= df["Label"].map(_truthy_label)
    if "mokapot PEP" in df.columns:
        pep = pd.to_numeric(df["mokapot PEP"], errors="coerce")
        mask &= pep <= PICKED_GROUP_FDR_FDR_CUTOFF
    if "Proteins" in df.columns:
        mask &= df["Proteins"].map(_has_non_contaminant_target_protein)

    accepted = {
        sequence
        for sequence in df.loc[mask, "Peptide"].map(normalize_peptide_sequence)
        if sequence
    }
    return frozenset(accepted)


def accepted_picked_group_fdr_peptide_sequences(txt_path):
    peptides_path = latest_successful_picked_group_fdr_peptides_file(txt_path)
    if peptides_path is None:
        return frozenset()
    return _accepted_peptide_sequences_from_mokapot(str(peptides_path))


def filter_peptides_with_picked_group_fdr(df, txt_path):
    if df is None or getattr(df, "empty", True):
        return df
    if "Sequence" not in df.columns:
        return df

    peptides_path = latest_successful_picked_group_fdr_peptides_file(txt_path)
    if peptides_path is None:
        return df

    accepted = _accepted_peptide_sequences_from_mokapot(str(peptides_path))
    if not accepted:
        return df.iloc[0:0].copy()

    normalized = df["Sequence"].map(normalize_peptide_sequence)
    mask = normalized.map(lambda sequence: bool(sequence) and sequence in accepted)
    return df[mask].copy()


def _read_tsv_table(path):
    return pd.read_csv(
        path,
        sep="\t",
        low_memory=False,
        na_filter=False,
        encoding="utf-8-sig",
    )


def _log_excerpt(path, max_lines=12):
    path = Path(path)
    if not path.is_file():
        return []
    try:
        lines = [
            line.strip()
            for line in path.read_text(encoding="utf-8", errors="ignore").splitlines()
            if line.strip()
        ]
    except OSError:
        return []
    if len(lines) <= max_lines:
        return lines
    head_count = max_lines // 2
    tail_count = max_lines - head_count - 1
    return lines[:head_count] + ["..."] + lines[-tail_count:]


def filter_protein_groups_with_picked_group_fdr(df, txt_path):
    if df is None or getattr(df, "empty", True):
        return df
    if "Majority protein IDs" not in df.columns:
        return df

    filtered_path = latest_successful_picked_group_fdr_file(txt_path)
    if filtered_path is None:
        return df

    accepted = _accepted_majority_protein_id_sets(str(filtered_path))
    if not accepted:
        logger.warning(
            "Picked Group FDR protein output is empty for %s; returning an empty protein-group frame.",
            filtered_path,
        )
        return df.iloc[0:0].copy()

    normalized_ids = df["Majority protein IDs"].map(normalize_majority_protein_ids)
    mask = normalized_ids.map(lambda value: bool(value) and value in accepted)
    return df[mask].copy()


def filtered_picked_group_fdr_evidence_for_result(txt_path):
    maxquant_dir = Path(txt_path)
    if maxquant_dir.is_file():
        maxquant_dir = maxquant_dir.parent

    combined_evidence_path = latest_successful_picked_group_fdr_evidence_file(maxquant_dir)
    if combined_evidence_path is None or not combined_evidence_path.is_file():
        return None

    local_evidence_path = maxquant_dir / "evidence.txt"
    suffixes = _experiment_suffixes_from_evidence(local_evidence_path)
    if not suffixes:
        return None
    experiment = suffixes[0]

    try:
        df = _read_tsv_table(combined_evidence_path)
    except Exception:
        return None
    if df.empty or "Experiment" not in df.columns:
        return None

    df = df[df["Experiment"].astype(str) == experiment].copy()
    if df.empty:
        return df

    if "PEP" in df.columns:
        pep = pd.to_numeric(df["PEP"], errors="coerce")
        df = df[pep <= PICKED_GROUP_FDR_FDR_CUTOFF].copy()
    if "Potential contaminant" in df.columns:
        df = df[df["Potential contaminant"] != "+"].copy()
    if "Reverse" in df.columns:
        df = df[df["Reverse"] != "+"].copy()
    return df


def _protein_group_id_sets_from_df(df):
    if df is None or getattr(df, "empty", True):
        return frozenset()
    if "Majority protein IDs" not in df.columns:
        return frozenset()
    return frozenset(
        value
        for value in df["Majority protein IDs"].map(normalize_majority_protein_ids)
        if value
    )


def _experiment_suffixes_from_protein_groups(df):
    suffixes = []
    patterns = [
        r"^Reporter intensity corrected \d+ (.+)$",
        r"^Reporter intensity count \d+ (.+)$",
        r"^Reporter intensity \d+ (.+)$",
        r"^Intensity (.+)$",
        r"^iBAQ (.+)$",
        r"^Unique peptides (.+)$",
        r"^Razor \+ unique peptides (.+)$",
        r"^Peptides (.+)$",
        r"^Identification type (.+)$",
        r"^Sequence coverage \[%\] (.+)$",
        r"^Sequence coverage (.+) \[%\]$",
    ]
    for column in df.columns:
        for pattern in patterns:
            match = re.match(pattern, str(column))
            if match:
                suffix = match.group(1)
                if suffix and suffix not in suffixes:
                    suffixes.append(suffix)
                break
    return suffixes


def _experiment_suffixes_from_evidence(evidence_path):
    if not evidence_path:
        return []
    evidence_path = Path(evidence_path)
    if not evidence_path.is_file():
        return []
    try:
        df = _read_tsv_table(evidence_path)
    except Exception:
        return []
    if "Experiment" not in df.columns:
        return []
    suffixes = []
    for value in df["Experiment"].dropna().astype(str):
        value = value.strip()
        if value and value not in suffixes:
            suffixes.append(value)
    return suffixes


def _sample_specific_column_suffix(column, suffixes):
    column = str(column)
    for suffix in suffixes:
        if column.endswith(f" {suffix}"):
            return suffix
        if column.startswith("Sequence coverage ") and column.endswith(f"{suffix} [%]"):
            return suffix
    return None


def _columns_for_result_suffix(global_columns, result_suffix, all_suffixes):
    selected = []
    for column in global_columns:
        suffix = _sample_specific_column_suffix(column, all_suffixes)
        if suffix is None or suffix == result_suffix:
            selected.append(column)
    return selected


def _pick_result_suffix(original_df, evidence_path, global_columns):
    candidates = []
    for suffix in _experiment_suffixes_from_protein_groups(original_df):
        if suffix not in candidates:
            candidates.append(suffix)
    for suffix in _experiment_suffixes_from_evidence(evidence_path):
        if suffix not in candidates:
            candidates.append(suffix)

    for suffix in candidates:
        if any(
            _sample_specific_column_suffix(column, [suffix]) == suffix
            for column in global_columns
        ):
            return suffix
    return candidates[0] if candidates else None


def write_per_result_picked_group_fdr_quant_files(
    included_results,
    filtered_protein_groups_path,
):
    filtered_protein_groups_path = Path(filtered_protein_groups_path)
    if not filtered_protein_groups_path.is_file():
        raise FileNotFoundError(filtered_protein_groups_path)

    global_df = _read_tsv_table(filtered_protein_groups_path)
    if "Majority protein IDs" not in global_df.columns:
        raise ValueError(
            f"{filtered_protein_groups_path} is missing the Majority protein IDs column."
        )

    all_suffixes = _experiment_suffixes_from_protein_groups(global_df)
    global_id_sets = global_df["Majority protein IDs"].map(normalize_majority_protein_ids)
    written = []
    skipped = []

    for item in included_results:
        maxquant_output_dir = Path(item["maxquant_output_dir"])
        original_path = maxquant_output_dir / "proteinGroups.txt"
        if not original_path.is_file():
            skipped.append({**item, "reason": "Missing original proteinGroups.txt."})
            continue

        try:
            original_df = _read_tsv_table(original_path)
        except Exception as exc:
            skipped.append(
                {**item, "reason": f"Could not read original proteinGroups.txt: {exc}"}
            )
            continue

        original_ids = _protein_group_id_sets_from_df(original_df)
        if not original_ids:
            skipped.append(
                {**item, "reason": "No original protein groups were available."}
            )
            continue

        suffix = _pick_result_suffix(
            original_df,
            item.get("evidence_path"),
            list(global_df.columns),
        )
        if not suffix:
            skipped.append(
                {**item, "reason": "Could not determine MaxQuant experiment suffix."}
            )
            continue

        columns = _columns_for_result_suffix(global_df.columns, suffix, all_suffixes)
        mask = global_id_sets.map(lambda value: bool(value) and value in original_ids)
        out_df = global_df.loc[mask, columns].copy()
        if out_df.empty:
            skipped.append(
                {**item, "reason": "No picked-group-FDR rows matched this result."}
            )
            continue

        output_path = maxquant_output_dir / PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS
        out_df.to_csv(output_path, sep="\t", index=False)
        written.append(
            {
                "result_id": item.get("result_id"),
                "raw_file": item.get("raw_file"),
                "experiment_suffix": suffix,
                "path": str(output_path),
                "n_rows": int(len(out_df)),
                "n_columns": int(len(out_df.columns)),
            }
        )

    return {
        "written": written,
        "skipped": skipped,
    }


def collect_pipeline_evidence_inputs(pipeline, result_ids=None):
    from maxquant.models import Result

    queryset = Result.objects.filter(raw_file__pipeline=pipeline).select_related(
        "raw_file", "raw_file__pipeline", "raw_file__pipeline__project"
    )
    if result_ids is not None:
        queryset = queryset.filter(pk__in=list(result_ids))

    included = []
    excluded = []
    for result in sorted(queryset, key=_result_sort_key):
        evidence_path = result.output_dir_maxquant / "evidence.txt"
        status = result.maxquant_status
        base = {
            "result_id": result.pk,
            "raw_file": str(result.raw_file.logical_name),
            "raw_file_id": result.raw_file.pk,
            "maxquant_status": status,
            "evidence_path": str(evidence_path),
            "maxquant_output_dir": str(result.output_dir_maxquant),
            "created": _isoformat(result.created),
        }
        if status != "done":
            excluded.append({**base, "reason": f"MaxQuant status is {status}."})
            continue
        if not evidence_path.is_file():
            excluded.append({**base, "reason": "Missing evidence.txt."})
            continue
        included.append(base)

    return {
        "included_results": included,
        "excluded_results": excluded,
    }


def validate_pipeline_for_picked_group_fdr(pipeline, result_ids=None):
    fasta_path = Path(pipeline.fasta_path)
    mqpar_path = Path(pipeline.mqpar_path)
    if not fasta_path.is_file():
        return {
            "status": "error",
            "message": f"Missing FASTA file: {fasta_path}",
        }
    if not mqpar_path.is_file():
        return {
            "status": "error",
            "message": f"Missing mqpar.xml file: {mqpar_path}",
        }

    try:
        mqpar_settings = _read_mqpar_settings(mqpar_path)
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Could not parse mqpar.xml: {exc}",
        }

    fdr_errors = []
    for settings_key, xml_name, required_value in (
        ("protein_fdr", "proteinFdr", PICKED_GROUP_FDR_REQUIRED_PROTEIN_FDR),
        ("peptide_fdr", "peptideFdr", PICKED_GROUP_FDR_REQUIRED_PEPTIDE_FDR),
    ):
        fdr_error = _validate_required_mqpar_fdr(
            mqpar_settings,
            settings_key,
            xml_name,
            required_value,
        )
        if fdr_error:
            fdr_errors.append(fdr_error)
    if fdr_errors:
        return {
            "status": "error",
            "message": " ".join(fdr_errors),
        }

    if mqpar_settings.get("digestion") == "unsupported":
        return {
            "status": "error",
            "message": (
                "mqpar.xml uses an unsupported enzymeMode for the current picked-group-FDR "
                "integration."
            ),
        }

    if not mqpar_settings.get("picked_group_enzyme"):
        return {
            "status": "error",
            "message": (
                "mqpar.xml uses an enzyme that is not mapped for picked-group-FDR: "
                f"{mqpar_settings.get('enzyme_name') or 'unknown'}."
            ),
        }

    run_manifest = collect_pipeline_evidence_inputs(pipeline, result_ids=result_ids)
    included = run_manifest["included_results"]
    excluded = run_manifest["excluded_results"]
    if len(included) == 0:
        return {
            "status": "error",
            "message": "No eligible MaxQuant runs with evidence.txt were found for this pipeline.",
            "included_results": included,
            "excluded_results": excluded,
            "mqpar_settings": mqpar_settings,
        }

    return {
        "status": "ok",
        "message": (
            f"{len(included)} run(s) eligible for picked-group-FDR; "
            f"{len(excluded)} run(s) excluded."
        ),
        "included_results": included,
        "excluded_results": excluded,
        "mqpar_settings": mqpar_settings,
        "fasta_path": str(fasta_path),
        "mqpar_path": str(mqpar_path),
    }


def run_picked_group_fdr(
    pipeline_identifier,
    selected_run_set,
    fasta_path,
    mqpar_path,
    evidence_paths,
    output_dir,
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = output_dir / "picked_group_fdr.out"
    stderr_path = output_dir / "picked_group_fdr.err"
    protein_groups_out = output_dir / "proteinGroups.txt"
    filtered_out = output_dir / f"proteinGroups.fdr{PICKED_GROUP_FDR_FDR_CUTOFF*100:g}.txt"
    copied_mqpar_path = output_dir / "mqpar.xml"
    copied_mqpar_path.write_text(Path(mqpar_path).read_text(encoding="utf-8"), encoding="utf-8")

    manifest_path = output_dir / "manifest.json"
    if not manifest_path.is_file():
        manifest_path.write_text(
            json.dumps(
                {
                    "pipeline_identifier": pipeline_identifier,
                    "selected_run_set": selected_run_set,
                    "fasta_path": str(fasta_path),
                    "mqpar_path": str(mqpar_path),
                    "evidence_paths": [str(path) for path in evidence_paths],
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    mqpar_settings = _read_mqpar_settings(mqpar_path)
    meta_path = output_dir / "meta.txt"
    combined_evidence_out = output_dir / "evidence.txt"
    andromeda_tab = output_dir / "andromeda.tab"
    mokapot_psms = output_dir / "andromeda.mokapot.psms.txt"
    mokapot_decoy_psms = output_dir / "andromeda.mokapot.decoy.psms.txt"
    mokapot_peptides = output_dir / PICKED_GROUP_FDR_MOKAPOT_PEPTIDES

    meta_path.write_text(
        "\n".join(str(path) for path in evidence_paths) + "\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    conda_prefix = env.get("CONDA_PREFIX")
    if conda_prefix:
        conda_lib = Path(conda_prefix) / "lib"
        if conda_lib.is_dir():
            existing_ld = env.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = (
                f"{conda_lib}:{existing_ld}" if existing_ld else str(conda_lib)
            )

    module_runner = [sys.executable, "-u", "-m"]
    digestion_args = [
        "--min-length",
        str(mqpar_settings["min_length"]),
        "--max-length",
        "60",
        "--cleavages",
        str(mqpar_settings["max_missed_cleavages"]),
        "--enzyme",
        mqpar_settings["picked_group_enzyme"],
        "--digestion",
        mqpar_settings["digestion"],
    ]
    commands = [
        module_runner
        + [
            "picked_group_fdr.pipeline.andromeda2pin",
            str(meta_path),
            "--outputTab",
            str(andromeda_tab),
            "--databases",
            str(fasta_path),
        ]
        + digestion_args,
        module_runner
        + [
            "picked_group_fdr.pipeline.run_mokapot",
            str(PICKED_GROUP_FDR_FDR_CUTOFF),
            str(PICKED_GROUP_FDR_FDR_CUTOFF),
            str(output_dir),
            str(PICKED_GROUP_FDR_MOKAPOT_MAX_WORKERS),
        ],
        module_runner
        + [
            "picked_group_fdr.pipeline.update_evidence_from_pout",
            "--mq_evidence",
        ]
        + [str(path) for path in evidence_paths]
        + [
            "--perc_results",
            str(mokapot_psms),
            str(mokapot_decoy_psms),
            "--mq_evidence_out",
            str(combined_evidence_out),
            "--pout_input_type",
            "andromeda",
        ],
        module_runner
        + [
            "picked_group_fdr",
            "--mq_evidence",
            str(combined_evidence_out),
            "--methods",
            "picked_protein_group_mq_input",
            "--do_quant",
            "--protein_groups_out",
            str(protein_groups_out),
            "--fasta",
            str(fasta_path),
        ]
        + digestion_args
        + [
            "--lfq_min_peptide_ratios",
            str(PICKED_GROUP_FDR_LFQ_MIN_PEPTIDE_RATIOS),
        ],
        module_runner
        + [
            "picked_group_fdr.pipeline.filter_fdr_maxquant",
            "--mq_protein_groups",
            str(protein_groups_out),
            "--mq_protein_groups_out",
            str(filtered_out),
            "--fdr_cutoff",
            str(PICKED_GROUP_FDR_FDR_CUTOFF),
        ],
    ]

    original_raise_exceptions = logging.raiseExceptions
    with stdout_path.open("w", encoding="utf-8") as stdout_handle, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr_handle, redirect_stdout(stdout_handle), redirect_stderr(stderr_handle):
        try:
            # Third-party pipeline modules install their own loggers and can emit
            # noisy "I/O operation on closed file" tracebacks during shutdown when
            # stderr has been redirected. Keep the underlying task behavior intact
            # while suppressing those internal logging diagnostics.
            logging.raiseExceptions = False
            print(f"Pipeline: {pipeline_identifier}")
            print(f"Evidence files: {len(evidence_paths)}")
            print(f"FASTA: {fasta_path}")
            print(f"mqpar.xml: {mqpar_path}")
            print(f"meta.txt: {meta_path}")
            print(f"combined evidence: {combined_evidence_out}")
            for command in commands:
                print("$ " + " ".join(command))
                stdout_handle.flush()
                stderr_handle.flush()
                subprocess.run(
                    command,
                    check=True,
                    cwd=output_dir,
                    env=env,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                )
            print("Picked-group-FDR completed successfully.")
        except SystemExit as exc:
            summary = format_picked_group_fdr_failure(exc)
            print(
                summary,
                file=stderr_handle,
            )
            raise RuntimeError(summary) from exc
        except Exception as exc:
            summary = format_picked_group_fdr_failure(exc)
            print(summary, file=stderr_handle)
            traceback.print_exc(file=stderr_handle)
            raise RuntimeError(summary) from exc
        finally:
            logging.raiseExceptions = original_raise_exceptions

    artifacts = {
        "meta": str(meta_path),
        "combined_evidence": str(combined_evidence_out),
        "protein_groups": str(protein_groups_out),
        "protein_groups_filtered": str(filtered_out),
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
        "mqpar_copy": str(copied_mqpar_path),
        "manifest": str(manifest_path),
    }
    if mokapot_peptides.exists():
        artifacts["mokapot_peptides"] = str(mokapot_peptides)
    missing = [path for path in artifacts.values() if not Path(path).exists()]
    if missing:
        raise RuntimeError(
            "Picked-group-FDR did not produce the expected artifacts: "
            + ", ".join(missing)
        )

    return {
        "status": "success",
        "artifacts": artifacts,
        "log_excerpt": {
            "stdout": _log_excerpt(stdout_path),
            "stderr": _log_excerpt(stderr_path, max_lines=8),
        },
    }
