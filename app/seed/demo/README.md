Demo bootstrap assets shipped with the repository.

Purpose:
- provide invariant first-run demo data for `bootstrap_demo`
- avoid depending on a local datalake path that does not exist on fresh installs

Contents:
- `config/`: pipeline-level `mqpar.xml` and `fasta.faa`
- `runs/demo_01..demo_03/maxquant/`: minimal MaxQuant outputs used by the demo UI
- `runs/demo_01..demo_03/rawtools/`: RawTools metrics and chromatograms
- `runs/demo_01..demo_03/rawtools_qc/`: RawTools QC outputs
- `manifest.json`: ordered list of seeded demo runs and their displayed raw file names

The bundle intentionally excludes large intermediate files that are not needed
for the seeded demo experience, such as RawTools `*_Matrix.txt`.
