# MaxQuant

> Cox, J. and Mann, M. MaxQuant enables high peptide identification rates, individualized p.p.b.-range mass accuracies and proteome-wide protein quantification. Nat Biotechnol, 2008, 26, pp 1367-72.
Note that the paper has a large supplement containing in-depth descriptions of algorithms.

MaxQuant is a proteomics platform for peptide identification, quantification, and downstream analysis of large mass-spectrometry datasets. LAMPrEY uses it to process individual Thermo Fisher `.raw` files inside a reproducible pipeline workflow.

The software supports label-free and isobaric workflows, and LAMPrEY extracts a standard quality-control table from the generated `summary.txt`, `proteinGroups.txt`, `peptides.txt`, `msmsScans.txt`, and `evidence.txt` outputs.

The QC layer currently captures metrics such as:

- acquisition summary values from `summary.txt`
- protein-group counts, contaminant counts, sequence coverage, and per-channel TMT missing values
- peptide counts, oxidation rates, and missed-cleavage distributions
- precursor and calibration metrics from `msmsScans.txt` and `evidence.txt`
- QC peptide and QC protein reporter-intensity summaries when the expected targets are present

LAMPrEY can also apply picked-group FDR correction to completed MaxQuant runs in a pipeline. This is started manually from the pipeline page in the Django admin panel, and successful runs produce corrected protein-group outputs for downstream QC and protein-group review. For the operator workflow, see [How to run picked-group FDR correction](how-to-run-picked-group-fdr.md).

To generate an input parameter file for LAMPrEY, create the configuration in a local MaxQuant installation with a single representative RAW file, then export the resulting `mqpar.xml` and upload it to the server as part of a pipeline setup.

Runtime selection is version-aware:

- MaxQuant versions earlier than `2.6` run through `mono`
- MaxQuant versions `2.6` and newer run through `dotnet`
- the runtime is selected automatically from the uploaded binary, but can be overridden with `MAXQUANT_RUNTIME=mono` or `MAXQUANT_RUNTIME=dotnet`

The repository also includes a bundled MaxQuant executable ZIP under `app/seed/defaults/maxquant/`. That asset is stored with Git LFS, so contributors and operators should install `git-lfs` before cloning the repository.
