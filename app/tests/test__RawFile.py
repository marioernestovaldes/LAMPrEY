
from django.test import TestCase
from django.urls import reverse
import os
from django.db import IntegrityError
from project.models import Project
from maxquant.models import Pipeline
from maxquant.models import Result

from maxquant.models import RawFile

from glob import glob

from django.core.files.uploadedfile import SimpleUploadedFile

from user.models import User


class RawFileTestCase(TestCase):
    def setUp(self):
        if not hasattr(self, "pipeline"):
            print("Setup RawFileTestCase")
            self.project = Project.objects.create(
                name="project", description="A test project"
            )

            contents_mqpar = b"<mqpar></mqpar>"
            contents_fasta = b">protein\nSEQUENCE"

            self.pipeline = Pipeline.objects.create(
                name="pipe",
                project=self.project,
                fasta_file=SimpleUploadedFile("my_fasta.fasta", contents_fasta),
                mqpar_file=SimpleUploadedFile("my_mqpar.xml", contents_mqpar),
                rawtools_args="-p -q -x -u -l -m -r TMT11 -chro 12TB",
            )

            self.raw_file = RawFile.objects.create(
                pipeline=self.pipeline, orig_file=SimpleUploadedFile("fake.raw", b"...")
            )
            print("...done setup RawFileTestCase.")

    def test__raw_file_exists(self):
        assert self.raw_file.path.is_file(), self.raw_file.path

    def test__rawfile_output_dir_created(self):
        path = self.raw_file.output_dir
        files = glob(f"{self.raw_file.pipeline.path}/**/*", recursive=True)
        files.sort()
        print(files)
        assert path.is_dir(), f"{path} NOT FOUND\n\t" + "\n\t".join(files)

    def test__maxquant_results_created(self):
        result = Result.objects.get(raw_file=self.raw_file)
        assert result is not None, result

    def test__unsaved_rawfile_storage_scope_uses_unique_temp_namespace(self):
        raw_a = RawFile(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("pending.raw", b"..."),
        )
        raw_b = RawFile(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("pending.raw", b"..."),
        )

        self.assertNotIn("_rf0_", raw_a.storage_scope)
        self.assertNotIn("_rf0_", raw_b.storage_scope)
        self.assertNotEqual(raw_a.storage_scope, raw_b.storage_scope)
        self.assertIn("_tmp", raw_a.storage_scope)
        self.assertIn("_tmp", raw_b.storage_scope)

    def test__path_prefers_existing_legacy_file_over_empty_namespaced_dir(self):
        namespaced_dir = self.raw_file.pipeline.input_path / self.raw_file.storage_scope
        legacy_path = self.raw_file._legacy_path

        os.makedirs(namespaced_dir, exist_ok=True)
        os.makedirs(legacy_path.parent, exist_ok=True)
        self.raw_file.path.rename(legacy_path)

        self.assertTrue(namespaced_dir.is_dir())
        self.assertFalse((namespaced_dir / self.raw_file.name).is_file())
        self.assertTrue(legacy_path.is_file())
        self.assertEqual(self.raw_file.path, legacy_path)

    def test__path_keeps_namespaced_file_when_only_legacy_dir_exists(self):
        namespaced_path = self.raw_file.pipeline.input_path / self.raw_file.storage_scope / self.raw_file.name
        legacy_path = self.raw_file._legacy_path

        os.makedirs(legacy_path.parent, exist_ok=True)

        self.assertTrue(namespaced_path.is_file())
        self.assertTrue(legacy_path.parent.is_dir())
        self.assertFalse(legacy_path.is_file())
        self.assertEqual(self.raw_file.path, namespaced_path)

    def test__duplicate_owned_rawfile_is_rejected(self):
        owner = User.objects.create_user(email="owner@example.com", password="pass1234")
        first = RawFile.objects.create(
            pipeline=self.pipeline,
            created_by=owner,
            orig_file=SimpleUploadedFile("owned-duplicate-a.raw", b"..."),
        )
        second = RawFile.objects.create(
            pipeline=self.pipeline,
            created_by=owner,
            orig_file=SimpleUploadedFile("owned-duplicate-b.raw", b"..."),
        )
        RawFile.objects.filter(pk=first.pk).update(orig_file="upload/owned-duplicate.raw")

        with self.assertRaises(IntegrityError):
            RawFile.objects.filter(pk=second.pk).update(orig_file="upload/owned-duplicate.raw")

    def test__duplicate_null_owner_rawfile_is_rejected(self):
        first = RawFile.objects.create(
            pipeline=self.pipeline,
            created_by=User.objects.create_user(email="null-owner@example.com", password="pass1234"),
            orig_file=SimpleUploadedFile("null-owner-duplicate-a.raw", b"..."),
        )
        RawFile.objects.filter(pk=first.pk).update(
            created_by=None,
            orig_file="upload/null-owner-duplicate.raw",
        )

        second = RawFile.objects.create(
            pipeline=self.pipeline,
            created_by=User.objects.create_user(email="null-owner-2@example.com", password="pass1234"),
            orig_file=SimpleUploadedFile("null-owner-duplicate-b.raw", b"..."),
        )

        with self.assertRaises(IntegrityError):
            RawFile.objects.filter(pk=second.pk).update(
                created_by=None,
                orig_file="upload/null-owner-duplicate.raw",
            )


class SameRawFileCanBeUploadedToMultiplePipelines(TestCase):
    def setUp(self):
        self.project = Project.objects.create(
            name="project", description="A test project"
        )

        contents_mqpar = b"<mqpar></mqpar>"
        contents_fasta = b">protein\nSEQUENCE"

        self.pipeline_A = Pipeline.objects.create(
            name="pipeA",
            project=self.project,
            fasta_file=SimpleUploadedFile("my_fasta.fasta", contents_fasta),
            mqpar_file=SimpleUploadedFile("my_mqpar.xml", contents_mqpar),
            rawtools_args="-p -q -x -u -l -m -r TMT11 -chro 12TB",
        )

        self.pipeline_B = Pipeline.objects.create(
            name="pipeB",
            project=self.project,
            fasta_file=SimpleUploadedFile("my_fasta.fasta", contents_fasta),
            mqpar_file=SimpleUploadedFile("my_mqpar.xml", contents_mqpar),
            rawtools_args="-p -q -x -u -l -m -r TMT11 -chro 12TB",
        )

    def test__upload_same_raw_file_to_different_pipelines(self):

        self.raw_file = RawFile.objects.create(
            pipeline=self.pipeline_A, orig_file=SimpleUploadedFile("fake.raw", b"...")
        )

        self.raw_file = RawFile.objects.create(
            pipeline=self.pipeline_B, orig_file=SimpleUploadedFile("fake.raw", b"...")
        )


class ReuploadAfterResultDeletionRestoresResult(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            email="tester@example.com", password="pass1234"
        )
        self.project = Project.objects.create(
            name="project", description="A test project", created_by=self.user
        )

        contents_mqpar = b"<mqpar></mqpar>"
        contents_fasta = b">protein\nSEQUENCE"

        self.pipeline = Pipeline.objects.create(
            name="pipe",
            project=self.project,
            created_by=self.user,
            fasta_file=SimpleUploadedFile("my_fasta.fasta", contents_fasta),
            mqpar_file=SimpleUploadedFile("my_mqpar.xml", contents_mqpar),
            rawtools_args="-p -q -x -u -l -m -r TMT11 -chro 12TB",
        )

        self.raw_file = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake.raw", b"..."),
            created_by=self.user,
        )
        self.result = Result.objects.get(raw_file=self.raw_file)

    def test_reupload_same_filename_recreates_missing_result(self):
        self.client.force_login(self.user)

        self.result.delete()
        self.assertFalse(Result.objects.filter(raw_file=self.raw_file).exists())

        response = self.client.post(
            reverse("maxquant:basic_upload"),
            data={
                "project": self.project.pk,
                "pipeline": self.pipeline.pk,
                "orig_file": SimpleUploadedFile("fake.raw", b"..."),
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["is_valid"])
        self.assertTrue(payload.get("already_exists"))
        self.assertTrue(payload.get("restored_result"))

        self.assertEqual(
            RawFile.objects.filter(pipeline=self.pipeline, orig_file="upload/fake.raw").count(),
            1,
        )
        self.assertTrue(Result.objects.filter(raw_file=self.raw_file).exists())
