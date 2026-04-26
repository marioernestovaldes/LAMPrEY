import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from django.contrib.admin.sites import AdminSite
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse

from maxquant.admin import PipelineAdmin
from maxquant.models import Pipeline, RawFile, Result
from maxquant.tasks import run_pipeline_picked_group_fdr
from omics.proteomics.maxquant.picked_group_fdr import (
    PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS,
    collect_pipeline_evidence_inputs,
    filter_protein_groups_with_picked_group_fdr,
    format_picked_group_fdr_failure,
    latest_successful_picked_group_fdr_evidence_file,
    latest_successful_picked_group_fdr_file,
    latest_successful_picked_group_fdr_peptides_file,
    run_picked_group_fdr,
    validate_pipeline_for_picked_group_fdr,
    write_per_result_picked_group_fdr_quant_files,
)
from project.models import Project
from user.models import User


class PickedGroupFDRHelperTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_superuser(
            email="picked-admin@example.com", password="pass1234"
        )
        self.project = Project.objects.create(
            name="Picked Project",
            description="Picked group FDR tests",
            created_by=self.user,
        )
        self.contents_fasta = b">sp|P00001|TEST_PROTEIN Test protein\nMPEPTIDESEQ\n"
        self.contents_mqpar_valid = (
            b'<?xml version="1.0" encoding="utf-8"?>\n'
            b"<MaxQuantParams>\n"
            b"  <minPepLen>7</minPepLen>\n"
            b"  <proteinFdr>1</proteinFdr>\n"
            b"  <peptideFdr>1</peptideFdr>\n"
            b"  <siteFdr>0.01</siteFdr>\n"
            b"  <parameterGroups>\n"
            b"    <parameterGroup>\n"
            b"      <maxMissedCleavages>2</maxMissedCleavages>\n"
            b"      <enzymeMode>0</enzymeMode>\n"
            b"      <enzymes><string>Trypsin/P</string></enzymes>\n"
            b"    </parameterGroup>\n"
            b"  </parameterGroups>\n"
            b"  <fastaFiles>\n"
            b"    <FastaFileInfo>\n"
            b"      <fastaFilePath>example.fasta</fastaFilePath>\n"
            b"    </FastaFileInfo>\n"
            b"  </fastaFiles>\n"
            b"  <filePaths><string>sample.raw</string></filePaths>\n"
            b"  <experiments><string>Sample 1</string></experiments>\n"
            b"</MaxQuantParams>\n"
        )
        self.contents_mqpar_invalid = self.contents_mqpar_valid.replace(
            b"<proteinFdr>1</proteinFdr>",
            b"<proteinFdr>0.01</proteinFdr>",
        )

    def _create_pipeline(self, name="picked-pipe", mqpar_bytes=None):
        return Pipeline.objects.create(
            name=name,
            project=self.project,
            created_by=self.user,
            fasta_file=SimpleUploadedFile("picked.fasta", self.contents_fasta),
            mqpar_file=SimpleUploadedFile(
                "picked.xml",
                mqpar_bytes or self.contents_mqpar_valid,
            ),
            rawtools_args="-q",
        )

    def _write_file(self, path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def _build_pipeline_like_layout(self, base_path):
        pipeline_root = base_path / "P1MQ1"
        maxquant_dir = pipeline_root / "output" / "u1_rf1_sample" / "maxquant"
        picked_root = pipeline_root / "output" / "picked_group_fdr"
        (pipeline_root / "config").mkdir(parents=True, exist_ok=True)
        (pipeline_root / "output").mkdir(parents=True, exist_ok=True)
        (pipeline_root / "config" / "mqpar.xml").write_text(
            "<MaxQuantParams/>",
            encoding="utf-8",
        )
        maxquant_dir.mkdir(parents=True, exist_ok=True)
        picked_root.mkdir(parents=True, exist_ok=True)
        return pipeline_root, maxquant_dir, picked_root

    @patch.object(Result, "run")
    def test_collect_pipeline_evidence_inputs_filters_missing_evidence(self, _mock_run):
        pipeline = self._create_pipeline()
        raw_1 = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample1.raw", b"..."),
            created_by=self.user,
        )
        raw_2 = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample2.raw", b"..."),
            created_by=self.user,
        )
        result_1 = Result.objects.get(raw_file=raw_1)
        result_2 = Result.objects.get(raw_file=raw_2)

        self._write_file(result_1.output_dir_maxquant / "maxquant.out", "Finish writing tables")
        self._write_file(result_1.output_dir_maxquant / "evidence.txt", "Sequence\tPEP\nA\t0.1\n")
        self._write_file(result_2.output_dir_maxquant / "maxquant.out", "Finish writing tables")

        manifest = collect_pipeline_evidence_inputs(pipeline)

        self.assertEqual([row["result_id"] for row in manifest["included_results"]], [result_1.pk])
        self.assertEqual([row["result_id"] for row in manifest["excluded_results"]], [result_2.pk])
        self.assertEqual(manifest["excluded_results"][0]["reason"], "Missing evidence.txt.")

    def test_validate_pipeline_for_picked_group_fdr_rejects_non_global_protein_fdr(self):
        pipeline = self._create_pipeline(
            name="invalid-picked-pipe",
            mqpar_bytes=self.contents_mqpar_invalid,
        )

        validation = validate_pipeline_for_picked_group_fdr(pipeline)

        self.assertEqual(validation["status"], "error")
        self.assertIn("proteinFdr to 1", validation["message"])

    def test_validate_pipeline_for_picked_group_fdr_rejects_non_global_peptide_fdr(self):
        pipeline = self._create_pipeline(
            name="invalid-peptide-picked-pipe",
            mqpar_bytes=self.contents_mqpar_valid.replace(
                b"<peptideFdr>1</peptideFdr>",
                b"<peptideFdr>0.01</peptideFdr>",
            ),
        )

        validation = validate_pipeline_for_picked_group_fdr(pipeline)

        self.assertEqual(validation["status"], "error")
        self.assertIn("peptideFdr to 1", validation["message"])

    def test_validate_pipeline_for_picked_group_fdr_reports_all_fdr_requirements(self):
        pipeline = self._create_pipeline(
            name="invalid-both-fdr-picked-pipe",
            mqpar_bytes=self.contents_mqpar_valid.replace(
                b"<proteinFdr>1</proteinFdr>",
                b"<proteinFdr>0.01</proteinFdr>",
            ).replace(
                b"<peptideFdr>1</peptideFdr>",
                b"<peptideFdr>0.01</peptideFdr>",
            ),
        )

        validation = validate_pipeline_for_picked_group_fdr(pipeline)

        self.assertEqual(validation["status"], "error")
        self.assertIn("proteinFdr to 1", validation["message"])
        self.assertIn("peptideFdr to 1", validation["message"])

    def test_format_picked_group_fdr_failure_handles_no_decoys(self):
        summary = format_picked_group_fdr_failure(
            ValueError("No decoy PSMs were detected.")
        )

        self.assertIn("FDR correction was not successful", summary)
        self.assertIn("Mokapot did not complete successfully", summary)
        self.assertIn("no decoy PSMs were detected", summary)

    def test_format_picked_group_fdr_failure_handles_qvality_failure(self):
        summary = format_picked_group_fdr_failure(
            ValueError("negative dimensions are not allowed")
        )

        self.assertIn("FDR correction was not successful", summary)
        self.assertIn("Mokapot did not complete successfully", summary)
        self.assertIn("negative dimensions are not allowed", summary)

    def test_latest_successful_picked_group_fdr_file_requires_completed_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _pipeline_root, maxquant_dir, picked_root = self._build_pipeline_like_layout(
                Path(tmp_dir)
            )
            newest_incomplete = picked_root / "20260425-120000"
            older_completed = picked_root / "20260424-120000"
            newest_incomplete.mkdir(parents=True, exist_ok=True)
            older_completed.mkdir(parents=True, exist_ok=True)
            (newest_incomplete / "proteinGroups.fdr1.txt").write_text(
                "Majority protein IDs\nP-new\n",
                encoding="utf-8",
            )
            (newest_incomplete / "manifest.json").write_text(
                json.dumps({"status": "running"}),
                encoding="utf-8",
            )
            (older_completed / "proteinGroups.fdr1.txt").write_text(
                "Majority protein IDs\nP-old\n",
                encoding="utf-8",
            )
            (older_completed / "manifest.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )

            actual = latest_successful_picked_group_fdr_file(maxquant_dir)

            self.assertEqual(actual, older_completed / "proteinGroups.fdr1.txt")

    def test_latest_successful_picked_group_fdr_evidence_file_ignores_missing_status(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _pipeline_root, maxquant_dir, picked_root = self._build_pipeline_like_layout(
                Path(tmp_dir)
            )
            newest_without_status = picked_root / "20260425-120000"
            older_completed = picked_root / "20260424-120000"
            newest_without_status.mkdir(parents=True, exist_ok=True)
            older_completed.mkdir(parents=True, exist_ok=True)
            (newest_without_status / "evidence.txt").write_text(
                "Experiment\tPEP\nnew\t0.1\n",
                encoding="utf-8",
            )
            (newest_without_status / "manifest.json").write_text(
                json.dumps({}),
                encoding="utf-8",
            )
            (older_completed / "evidence.txt").write_text(
                "Experiment\tPEP\nold\t0.1\n",
                encoding="utf-8",
            )
            (older_completed / "manifest.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )

            actual = latest_successful_picked_group_fdr_evidence_file(maxquant_dir)

            self.assertEqual(actual, older_completed / "evidence.txt")

    def test_latest_successful_picked_group_fdr_peptides_file_requires_completed_manifest(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _pipeline_root, maxquant_dir, picked_root = self._build_pipeline_like_layout(
                Path(tmp_dir)
            )
            newest_failed = picked_root / "20260425-120000"
            older_completed = picked_root / "20260424-120000"
            newest_failed.mkdir(parents=True, exist_ok=True)
            older_completed.mkdir(parents=True, exist_ok=True)
            (newest_failed / "andromeda.mokapot.peptides.txt").write_text(
                "Peptide\tmokapot PEP\n-.NEW.-\t0.001\n",
                encoding="utf-8",
            )
            (newest_failed / "manifest.json").write_text(
                json.dumps({"status": "failed"}),
                encoding="utf-8",
            )
            (older_completed / "andromeda.mokapot.peptides.txt").write_text(
                "Peptide\tmokapot PEP\n-.OLD.-\t0.001\n",
                encoding="utf-8",
            )
            (older_completed / "manifest.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )

            actual = latest_successful_picked_group_fdr_peptides_file(maxquant_dir)

            self.assertEqual(actual, older_completed / "andromeda.mokapot.peptides.txt")

    def test_filter_protein_groups_with_picked_group_fdr_returns_empty_frame_when_output_empty(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            _pipeline_root, maxquant_dir, picked_root = self._build_pipeline_like_layout(
                Path(tmp_dir)
            )
            run_dir = picked_root / "20260424-120000"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "proteinGroups.fdr1.txt").write_text(
                "Majority protein IDs\n",
                encoding="utf-8",
            )
            (run_dir / "manifest.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )

            df = pd.DataFrame(
                [
                    {"Majority protein IDs": "P1", "Score": 10.0},
                    {"Majority protein IDs": "P2", "Score": 20.0},
                ]
            )

            with self.assertLogs(
                "omics.proteomics.maxquant.picked_group_fdr",
                level="WARNING",
            ) as logs:
                actual = filter_protein_groups_with_picked_group_fdr(df, maxquant_dir)

            self.assertTrue(actual.empty)
            self.assertEqual(actual.columns.tolist(), df.columns.tolist())
            self.assertTrue(
                any("protein output is empty" in message for message in logs.output)
            )

    @patch("omics.proteomics.maxquant.picked_group_fdr.subprocess.run")
    def test_run_picked_group_fdr_uses_combined_meta_workflow(self, mock_run):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            fasta_path = tmp_path / "fasta.faa"
            mqpar_path = tmp_path / "mqpar.xml"
            output_dir = tmp_path / "picked"
            evidence_paths = [
                tmp_path / "run_a" / "evidence.txt",
                tmp_path / "run_b" / "evidence.txt",
            ]

            fasta_path.write_text(">sp|P00001|TEST\nMPEPTIDESEQ\n", encoding="utf-8")
            mqpar_path.write_text(self.contents_mqpar_valid.decode("utf-8"), encoding="utf-8")
            for idx, evidence_path in enumerate(evidence_paths):
                evidence_path.parent.mkdir(parents=True, exist_ok=True)
                evidence_path.write_text(
                    f"Experiment\tPEP\nSample {idx}\t0.1\n",
                    encoding="utf-8",
                )

            def _fake_run(cmd, **kwargs):
                if any("run_mokapot" in part for part in cmd):
                    (output_dir / "andromeda.mokapot.peptides.txt").write_text(
                        (
                            "SpecId\tLabel\tScanNr\tExpMass\tPeptide\t"
                            "mokapot score\tmokapot q-value\tmokapot PEP\tProteins\n"
                            "input_1\tTrue\t1\t1000\t-.MPEPTIDESEQ.-\t"
                            "10\t0.001\t0.001\tP1\n"
                        ),
                        encoding="utf-8",
                    )
                elif any("update_evidence_from_pout" in part for part in cmd):
                    (output_dir / "evidence.txt").write_text(
                        "Experiment\tPEP\nCombined\t0.1\n",
                        encoding="utf-8",
                    )
                elif cmd[3] == "picked_group_fdr":
                    (output_dir / "proteinGroups.txt").write_text(
                        "Majority protein IDs\tQ-value\nP1\t0.01\n",
                        encoding="utf-8",
                    )
                elif any("filter_fdr_maxquant" in part for part in cmd):
                    (output_dir / "proteinGroups.fdr1.txt").write_text(
                        "Majority protein IDs\tQ-value\nP1\t0.01\n",
                        encoding="utf-8",
                    )

            mock_run.side_effect = _fake_run

            result = run_picked_group_fdr(
                pipeline_identifier="demo/pipeline",
                selected_run_set=[],
                fasta_path=fasta_path,
                mqpar_path=mqpar_path,
                evidence_paths=evidence_paths,
                output_dir=output_dir,
            )

            self.assertEqual(result["status"], "success")
            self.assertEqual(mock_run.call_count, 5)
            first_cmd = mock_run.call_args_list[0].args[0]
            self.assertEqual(first_cmd[0:3], [first_cmd[0], "-u", "-m"])
            self.assertEqual(first_cmd[3], "picked_group_fdr.pipeline.andromeda2pin")
            self.assertTrue((output_dir / "meta.txt").is_file())
            self.assertEqual(
                (output_dir / "meta.txt").read_text(encoding="utf-8"),
                "".join(f"{path}\n" for path in evidence_paths),
            )
            third_cmd = mock_run.call_args_list[2].args[0]
            self.assertEqual(third_cmd[3], "picked_group_fdr.pipeline.update_evidence_from_pout")
            self.assertEqual(
                third_cmd[third_cmd.index("--mq_evidence") + 1 : third_cmd.index("--perc_results")],
                [str(path) for path in evidence_paths],
            )
            fourth_cmd = mock_run.call_args_list[3].args[0]
            self.assertEqual(fourth_cmd[3], "picked_group_fdr")
            self.assertEqual(
                fourth_cmd[fourth_cmd.index("--mq_evidence") + 1],
                str(output_dir / "evidence.txt"),
            )
            self.assertIn("combined_evidence", result["artifacts"])
            self.assertIn("mokapot_peptides", result["artifacts"])

    def test_pipeline_admin_manifest_display_is_compact_summary(self):
        pipeline = self._create_pipeline(name="summary-picked-pipe")
        pipeline.picked_group_fdr_last_manifest = json.dumps(
            {
                "included_results": [
                    {"result_id": 17, "raw_file": "sample_a.raw"},
                    {"result_id": 18, "raw_file": "sample_b.raw"},
                ],
                "excluded_results": [
                    {"result_id": 19, "raw_file": "sample_c.raw", "reason": "Missing evidence.txt."}
                ],
                "mqpar_settings": {
                    "protein_fdr": 1.0,
                    "peptide_fdr": 1.0,
                    "enzyme_name": "Trypsin/P",
                },
                "artifacts": {
                    "protein_groups": "/tmp/proteinGroups.txt",
                    "protein_groups_filtered": "/tmp/proteinGroups.fdr1.txt",
                    "stderr": "/tmp/picked_group_fdr.err",
                },
            },
            sort_keys=True,
        )
        admin_instance = PipelineAdmin(Pipeline, AdminSite())

        rendered = admin_instance.picked_group_fdr_last_manifest_display(pipeline)

        self.assertIn("Included runs:", rendered)
        self.assertIn("sample_a.raw, sample_b.raw", rendered)
        self.assertIn("Missing evidence.txt.", rendered)
        self.assertIn("proteinGroups.fdr1.txt", rendered)
        self.assertNotIn("\"included_results\"", rendered)

    @patch.object(Result, "run")
    @patch("maxquant.admin.run_pipeline_picked_group_fdr.delay")
    def test_pipeline_admin_run_picked_group_fdr_queues_task(
        self,
        mock_delay,
        _mock_run,
    ):
        pipeline = self._create_pipeline(name="admin-picked-pipe")
        raw = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample-admin.raw", b"..."),
            created_by=self.user,
        )
        result = Result.objects.get(raw_file=raw)
        self._write_file(result.output_dir_maxquant / "maxquant.out", "Finish writing tables")
        self._write_file(result.output_dir_maxquant / "evidence.txt", "Sequence\tPEP\nA\t0.1\n")
        mock_delay.return_value.id = "picked-task-1"

        self.client.force_login(self.user)
        response = self.client.get(
            reverse("admin:maxquant_pipeline_run_picked_group_fdr", args=[pipeline.pk]),
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        pipeline.refresh_from_db()
        self.assertEqual(pipeline.picked_group_fdr_task_id, "picked-task-1")
        self.assertEqual(pipeline.picked_group_fdr_last_status, "requested")
        self.assertEqual(pipeline.picked_group_fdr_last_run_dir != "", True)
        self.assertIn("Queued picked-group-FDR", response.content.decode("utf-8"))

    def test_pipeline_admin_clear_picked_group_fdr_resets_status_fields(self):
        pipeline = self._create_pipeline(name="clear-picked-pipe")
        pipeline.picked_group_fdr_task_id = "picked-task-old"
        pipeline.picked_group_fdr_last_status = "completed"
        pipeline.picked_group_fdr_last_run_dir = "20260426-020045"
        pipeline.picked_group_fdr_last_error = "old error"
        pipeline.picked_group_fdr_last_manifest = json.dumps({"status": "completed"})
        pipeline.save()

        self.client.force_login(self.user)
        response = self.client.get(
            reverse("admin:maxquant_pipeline_clear_picked_group_fdr", args=[pipeline.pk]),
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        pipeline.refresh_from_db()
        self.assertEqual(pipeline.picked_group_fdr_task_id, "")
        self.assertIsNone(pipeline.picked_group_fdr_task_submitted_at)
        self.assertIsNone(pipeline.picked_group_fdr_last_started_at)
        self.assertIsNone(pipeline.picked_group_fdr_last_finished_at)
        self.assertEqual(pipeline.picked_group_fdr_last_status, "never_run")
        self.assertEqual(pipeline.picked_group_fdr_last_error, "")
        self.assertEqual(pipeline.picked_group_fdr_last_run_dir, "")
        self.assertEqual(pipeline.picked_group_fdr_last_manifest, "")
        self.assertIn(
            "Output files were not deleted",
            response.content.decode("utf-8"),
        )

    @patch("maxquant.tasks.write_per_result_picked_group_fdr_quant_files")
    @patch("maxquant.tasks.run_picked_group_fdr")
    @patch.object(Result, "run")
    def test_run_pipeline_picked_group_fdr_writes_completed_status_to_manifest(
        self,
        _mock_result_run,
        mock_run_picked_group_fdr,
        mock_write_per_result,
    ):
        pipeline = self._create_pipeline(name="task-picked-pipe")
        raw = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample-task.raw", b"..."),
            created_by=self.user,
        )
        result = Result.objects.get(raw_file=raw)
        self._write_file(
            result.output_dir_maxquant / "maxquant.out",
            "Finish writing tables",
        )
        self._write_file(
            result.output_dir_maxquant / "evidence.txt",
            "Experiment\tPEP\nSample 1\t0.1\n",
        )

        run_dir_name = "20260426-120000"
        run_dir_path = pipeline.picked_group_fdr_root / run_dir_name

        def _fake_run_picked_group_fdr(**kwargs):
            output_dir = Path(kwargs["output_dir"])
            output_dir.mkdir(parents=True, exist_ok=True)
            filtered = output_dir / "proteinGroups.fdr1.txt"
            filtered.write_text(
                "Majority protein IDs\tQ-value\nP1\t0.01\n",
                encoding="utf-8",
            )
            return {
                "status": "success",
                "artifacts": {
                    "protein_groups_filtered": str(filtered),
                    "protein_groups": str(output_dir / "proteinGroups.txt"),
                    "stderr": str(output_dir / "picked_group_fdr.err"),
                    "stdout": str(output_dir / "picked_group_fdr.out"),
                },
                "log_excerpt": {"stdout": ["ok"], "stderr": []},
            }

        mock_run_picked_group_fdr.side_effect = _fake_run_picked_group_fdr
        mock_write_per_result.return_value = {"written": [], "skipped": []}

        run_pipeline_picked_group_fdr.apply(
            kwargs={
                "pipeline_id": pipeline.pk,
                "selected_result_ids": [result.pk],
                "run_dir": run_dir_name,
            },
            task_id="picked-task-success",
        )

        manifest = json.loads(
            (run_dir_path / "manifest.json").read_text(encoding="utf-8")
        )
        pipeline.refresh_from_db()

        self.assertEqual(manifest["status"], "completed")
        self.assertEqual(pipeline.picked_group_fdr_last_status, "completed")
        self.assertEqual(
            json.loads(pipeline.picked_group_fdr_last_manifest)["status"],
            "completed",
        )

    @patch.object(Result, "run")
    def test_write_per_result_quant_file_uses_whitelisted_original_rows_and_suffix_columns(
        self,
        _mock_run,
    ):
        pipeline = self._create_pipeline(name="quant-picked-pipe")
        raw_a = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample-a.raw", b"..."),
            created_by=self.user,
        )
        raw_b = RawFile.objects.create(
            pipeline=pipeline,
            orig_file=SimpleUploadedFile("sample-b.raw", b"..."),
            created_by=self.user,
        )
        result_a = Result.objects.get(raw_file=raw_a)
        result_b = Result.objects.get(raw_file=raw_b)
        run_dir = pipeline.picked_group_fdr_root / "20260424-120000"
        filtered_path = run_dir / "proteinGroups.fdr1.txt"
        older_completed_dir = pipeline.picked_group_fdr_root / "20260425-120000"

        self._write_file(
            result_a.output_dir_maxquant / "proteinGroups.txt",
            (
                "Majority protein IDs\tReporter intensity corrected 1 SampleA\n"
                "P1\t10\n"
                "P3\t30\n"
            ),
        )
        self._write_file(
            result_a.output_dir_maxquant / "evidence.txt",
            "Experiment\nSampleA\n",
        )
        self._write_file(
            result_b.output_dir_maxquant / "proteinGroups.txt",
            (
                "Majority protein IDs\tReporter intensity corrected 1 SampleB\n"
                "P2\t20\n"
            ),
        )
        self._write_file(
            result_b.output_dir_maxquant / "evidence.txt",
            "Experiment\nSampleB\n",
        )
        self._write_file(
            filtered_path,
            (
                "Majority protein IDs\tScore\tIntensity SampleA\t"
                "Reporter intensity corrected 1 SampleA\tIntensity SampleB\t"
                "Reporter intensity corrected 1 SampleB\n"
                "P1\t100\t1000\t11\t0\t0\n"
                "P2\t200\t0\t0\t2000\t22\n"
                "P4\t400\t4000\t44\t5000\t55\n"
            ),
        )
        self._write_file(run_dir / "manifest.json", '{"status": "completed"}')
        self._write_file(
            older_completed_dir / "proteinGroups.fdr1.txt",
            "Majority protein IDs\nP3\n",
        )
        self._write_file(older_completed_dir / "manifest.json", '{"status": "completed"}')

        result = write_per_result_picked_group_fdr_quant_files(
            [
                {
                    "result_id": result_a.pk,
                    "raw_file": str(raw_a.logical_name),
                    "evidence_path": str(result_a.output_dir_maxquant / "evidence.txt"),
                    "maxquant_output_dir": str(result_a.output_dir_maxquant),
                },
                {
                    "result_id": result_b.pk,
                    "raw_file": str(raw_b.logical_name),
                    "evidence_path": str(result_b.output_dir_maxquant / "evidence.txt"),
                    "maxquant_output_dir": str(result_b.output_dir_maxquant),
                },
            ],
            filtered_path,
        )

        out_a = result_a.output_dir_maxquant / PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS
        out_b = result_b.output_dir_maxquant / PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS
        self.assertEqual(
            [row["result_id"] for row in result["written"]],
            [result_a.pk, result_b.pk],
        )
        self.assertEqual(result["skipped"], [])
        self.assertTrue(out_a.is_file())
        self.assertTrue(out_b.is_file())

        out_a_text = out_a.read_text(encoding="utf-8")
        out_b_text = out_b.read_text(encoding="utf-8")
        self.assertIn("Reporter intensity corrected 1 SampleA", out_a_text)
        self.assertNotIn("Reporter intensity corrected 1 SampleB", out_a_text)
        self.assertIn("P1\t100\t1000\t11", out_a_text)
        self.assertNotIn("P2\t200", out_a_text)
        self.assertNotIn("P4\t400", out_a_text)
        self.assertIn("Reporter intensity corrected 1 SampleB", out_b_text)
        self.assertNotIn("Reporter intensity corrected 1 SampleA", out_b_text)
        self.assertIn("P2\t200\t2000\t22", out_b_text)
