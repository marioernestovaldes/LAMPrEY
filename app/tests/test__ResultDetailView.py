import pandas as pd
from unittest.mock import patch
from django.test import TestCase
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile

from user.models import User
from project.models import Project
from maxquant.models import Pipeline, RawFile, Result

class ResultDetailViewTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            email="tester@example.com", password="pass1234"
        )
        self.project = Project.objects.create(
            name="Project 1", description="First project", created_by=self.user
        )

        contents_mqpar = b"<mqpar></mqpar>"
        contents_fasta = b">protein\nSEQUENCE"

        self.pipeline = Pipeline.objects.create(
            name="pipe1",
            project=self.project,
            created_by=self.user,
            fasta_file=SimpleUploadedFile("my_fasta.fasta", contents_fasta),
            mqpar_file=SimpleUploadedFile("my_mqpar.xml", contents_mqpar),
        )

        self.raw_file = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake.raw", b"..."),
            created_by=self.user
        )
        self.result, _ = Result.objects.get_or_create(raw_file=self.raw_file)

    @patch("maxquant.views.isfile", return_value=True)
    @patch("maxquant.views.pd.read_csv")
    def test_result_detail_view(self, mock_read_csv, mock_isfile):
        # Mock the dataframe returned by read_csv
        mock_df = pd.DataFrame({"Intensity": [1, 2, 3], "Retention time": [0.1, 0.2, 0.3]})
        mock_read_csv.return_value = mock_df

        self.client.force_login(self.user)
        url = reverse("maxquant:mq_detail", kwargs={"pk": self.result.pk})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertIn("figures", response.context)
        self.assertIn("summary_stats", response.context)

    def _write_file(self, fn, text):
        fn.parent.mkdir(parents=True, exist_ok=True)
        fn.write_text(text, encoding="utf-8")

    def test_result_detail_view_keeps_partial_protein_plots_when_score_missing(self):
        raw_name = self.raw_file.name
        self._write_file(
            self.result.output_dir_rawtools / f"{raw_name}_Ms_TIC_chromatogram.txt",
            "RetentionTime\tIntensity\n1\t10\n2\t20\n",
        )
        self._write_file(
            self.result.output_dir_rawtools / f"{raw_name}_Ms2_TIC_chromatogram.txt",
            "RetentionTime\tIntensity\n1\t5\n2\t15\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "summary.txt",
            "MS/MS submitted\tMS/MS identified\tMS/MS identified [%]\n10\t5\t50\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "evidence.txt",
            "Retention time\tMissed cleavages\tUncalibrated - Calibrated m/z [ppm]\tCharge\n"
            "1\t0\t0.1\t2\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "peptides.txt",
            "Length\tReporter intensity corrected 1\n8\t100\n9\t200\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "proteinGroups.txt",
            "Peptides\tReporter intensity corrected 1\tIntensity\n"
            "4\t1000\t1000\n"
            "7\t2000\t2000\n",
        )

        self.client.force_login(self.user)
        url = reverse("maxquant:mq_detail", kwargs={"pk": self.result.pk})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        section_titles = [section["title"] for section in response.context["figure_sections"]]
        self.assertIn("Quantification and channel intensity", section_titles)
        joined_warnings = "\n".join(response.context["data_warnings"])
        self.assertIn("`Score`", joined_warnings)
        self.assertContains(response, "Skipped Andromeda score plot")

    def test_result_detail_view_reads_comma_delimited_protein_groups(self):
        raw_name = self.raw_file.name
        self._write_file(
            self.result.output_dir_rawtools / f"{raw_name}_Ms_TIC_chromatogram.txt",
            "RetentionTime\tIntensity\n1\t10\n2\t20\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "proteinGroups.txt",
            "Peptides,Score,Intensity,Reporter intensity corrected 1\n"
            "4,12.5,1000,900\n"
            "7,8.0,2000,1800\n",
        )

        self.client.force_login(self.user)
        url = reverse("maxquant:mq_detail", kwargs={"pk": self.result.pk})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Channel intensity distribution (protein groups)")
        self.assertContains(response, "Andromeda Scores")

    def test_result_detail_view_reads_protein_groups_with_inconsistent_rows(self):
        raw_name = self.raw_file.name
        self._write_file(
            self.result.output_dir_rawtools / f"{raw_name}_Ms_TIC_chromatogram.txt",
            "RetentionTime\tIntensity\n1\t10\n2\t20\n",
        )
        self._write_file(
            self.result.output_dir_maxquant / "proteinGroups.txt",
            "Peptides,Intensity,Reporter intensity corrected 1\n"
            "4,1000,900\n"
            "7,2000\n",
        )

        self.client.force_login(self.user)
        url = reverse("maxquant:mq_detail", kwargs={"pk": self.result.pk})
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        joined_warnings = "\n".join(response.context["data_warnings"])
        self.assertIn("`Score`", joined_warnings)
        self.assertContains(response, "Channel intensity distribution (protein groups)")
