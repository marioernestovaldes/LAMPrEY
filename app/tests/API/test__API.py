from django.test import TestCase
from datetime import date
from unittest.mock import patch
from types import SimpleNamespace
from uuid import uuid4
from project.models import Project
from maxquant.models import Pipeline

from maxquant.models import RawFile
from maxquant.models import Result


from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command


from django.test import Client
from django.urls import reverse
from rest_framework.authtoken.models import Token
from user.models import User
from api.views import get_protein_quant_fn

URL = "http://localhost:8000"


class ApiTestCase(TestCase):
    def setUp(self):
        if not hasattr(self, "pipeline"):
            print("Setup")
            self.user = User.objects.create_user(
                email="api-user@example.com",
                password="testpass123",
            )
            self.member_user = User.objects.create_user(
                email="member@example.com",
                password="testpass123",
            )
            self.project = Project.objects.create(
                name="project", description="A test project", created_by=self.user
            )
            self.project.users.add(self.user)
            self.project.users.add(self.member_user)

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

    def test__projects(self):
        c = Client()
        c.force_login(self.user)
        url = f"{URL}/api/projects"
        actual = c.post(
            url,
            data={"uid": self.user.uuid},
            content_type="application/json",
        ).json()
        expected = [
            {
                "pk": self.project.pk,
                "name": "project",
                "description": "A test project",
                "slug": "project",
            }
        ]
        assert actual == expected, actual

    def test__token_create_requires_auth(self):
        c = Client()
        response = c.post(reverse("api:token"))
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__token_create_returns_reusable_token(self):
        c = Client()
        c.force_login(self.user)

        response = c.post(reverse("api:token"))
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        payload = response.json()

        assert payload["created"] is True, payload
        assert payload["token"], payload
        assert Token.objects.filter(user=self.user, key=payload["token"]).exists()

        response = c.post(reverse("api:token"))
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        second_payload = response.json()

        assert second_payload["created"] is False, second_payload
        assert second_payload["token"] == payload["token"], second_payload

    def test__token_delete_revokes_current_users_token(self):
        token = Token.objects.create(user=self.user)
        c = Client()
        c.force_login(self.user)

        response = c.delete(reverse("api:token"))
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.json() == {"deleted": True}, response.json()
        assert not Token.objects.filter(pk=token.pk).exists()

    def test__projects_unauthenticated(self):
        """Verify that unauthenticated requests are rejected."""
        c = Client()
        url = f"{URL}/api/projects"
        response = c.post(url)
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__projects_reject_uid_impersonation_without_auth(self):
        c = Client()
        url = f"{URL}/api/projects"
        response = c.post(
            url,
            data={"uid": str(self.user.uuid)},
            content_type="application/json",
        )
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__pipelines(self):
        """Test pipeline list endpoint."""
        c = Client()
        c.force_login(self.user)
        url = f"{URL}/api/pipelines"
        response = c.post(url, {"project": "project"}, content_type="application/json")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert len(data) == 1
        assert data[0]["name"] == "pipe"
        assert data[0]["uuid"] == str(self.pipeline.uuid)

    def test__pipeline_uploaders(self):
        c = Client()
        c.force_login(self.user)
        url = f"{URL}/api/pipeline-uploaders"
        response = c.post(
            url,
            {"project": self.project.slug, "pipeline": self.pipeline.slug},
            content_type="application/json",
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.json() == [
            {"label": self.user.email, "value": self.user.email}
        ], response.json()

    def test__pipelines_unauthenticated(self):
        """Verify unauthenticated pipeline requests are rejected."""
        c = Client()
        url = f"{URL}/api/pipelines"
        response = c.post(url, {"project": "project"}, content_type="application/json")
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__projects_accept_token_auth(self):
        token = Token.objects.create(user=self.user)
        c = Client()
        response = c.post(
            f"{URL}/api/projects",
            HTTP_AUTHORIZATION=f"Token {token.key}",
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert len(data) == 1
        assert data[0]["slug"] == self.project.slug

    def test__pipelines_accept_token_auth(self):
        token = Token.objects.create(user=self.user)
        c = Client()
        response = c.post(
            f"{URL}/api/pipelines",
            {"project": self.project.slug},
            content_type="application/json",
            HTTP_AUTHORIZATION=f"Token {token.key}",
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        data = response.json()
        assert len(data) == 1
        assert data[0]["uuid"] == str(self.pipeline.uuid)

    def test__invalid_token_is_rejected(self):
        c = Client()
        response = c.post(
            f"{URL}/api/projects",
            HTTP_AUTHORIZATION="Token invalid-token",
        )
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__pipelines_reject_uid_impersonation_without_auth(self):
        c = Client()
        url = f"{URL}/api/pipelines"
        response = c.post(
            url,
            {"project": "project", "uid": str(self.user.uuid)},
            content_type="application/json",
        )
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__create_flag_requires_auth(self):
        """Verify flag creation requires authentication."""
        c = Client()
        url = f"{URL}/api/flag/create"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "raw_files": ["fake.raw"],
        })
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__create_flag_requires_project_membership(self):
        """Verify users can only flag files in their projects."""
        other_user = User.objects.create_user(
            email="other@example.com",
            password="testpass123",
        )
        c = Client()
        c.force_login(other_user)
        url = f"{URL}/api/flag/create"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "raw_files": ["fake.raw"],
        })
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__delete_flag_requires_auth(self):
        """Verify flag deletion requires authentication."""
        c = Client()
        url = f"{URL}/api/flag/delete"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "raw_files": ["fake.raw"],
        })
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__project_member_cannot_create_flag_for_other_users_raw_file(self):
        c = Client()
        c.force_login(self.member_user)
        url = f"{URL}/api/flag/create"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "raw_files": ["fake.raw"],
        })

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        self.raw_file.refresh_from_db()
        assert self.raw_file.flagged is False

    def test__project_member_cannot_delete_flag_for_other_users_raw_file(self):
        self.raw_file.flagged = True
        self.raw_file.save(update_fields=["flagged"])

        c = Client()
        c.force_login(self.member_user)
        url = f"{URL}/api/flag/delete"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "raw_files": ["fake.raw"],
        })

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        self.raw_file.refresh_from_db()
        assert self.raw_file.flagged is True

    def test__rawfile_requires_auth(self):
        """Verify rawfile endpoint requires authentication."""
        c = Client()
        url = f"{URL}/api/rawfile"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "action": "flag",
            "raw_files": ["fake.raw"],
        })
        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__upload_raw_requires_auth(self):
        c = Client()
        response = c.post(
            reverse("api:upload-raw"),
            data={
                "pid": str(self.pipeline.uuid),
                "orig_file": SimpleUploadedFile("unauth.raw", b"raw-bytes"),
            },
        )

        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__qc_data_requires_pipeline_access(self):
        other_user = User.objects.create_user(
            email="qc-other@example.com",
            password="testpass123",
        )
        c = Client()
        c.force_login(other_user)
        url = f"{URL}/api/qc-data"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "data_range": 0,
        }, content_type="application/json")

        assert response.status_code == 403, f"Expected 403, got {response.status_code}"

    def test__rawfile_action_uses_run_key_for_uuid_prefixed_uppercase_upload(self):
        prefixed_raw = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("CaseStudy.RAW", b"..."),
            created_by=self.user,
        )
        RawFile.objects.filter(pk=prefixed_raw.pk).update(
            orig_file=f"upload/{uuid4().hex}_CaseStudy.RAW"
        )
        prefixed_raw.refresh_from_db()

        c = Client()
        c.force_login(self.user)

        response = c.post(
            f"{URL}/api/rawfile",
            {
                "project": self.project.slug,
                "pipeline": self.pipeline.slug,
                "action": "accept",
                "run_keys": [prefixed_raw.display_ref],
            },
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        prefixed_raw.refresh_from_db()
        assert prefixed_raw.use_downstream is True

        response = c.post(
            f"{URL}/api/rawfile",
            {
                "project": self.project.slug,
                "pipeline": self.pipeline.slug,
                "action": "reject",
                "run_keys": [prefixed_raw.display_ref],
            },
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        prefixed_raw.refresh_from_db()
        assert prefixed_raw.use_downstream is False

    def test__create_and_delete_flag_use_run_key_for_uuid_prefixed_uppercase_upload(self):
        prefixed_raw = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("FlagMe.RAW", b"..."),
            created_by=self.user,
        )
        RawFile.objects.filter(pk=prefixed_raw.pk).update(
            orig_file=f"upload/{uuid4().hex}_FlagMe.RAW"
        )
        prefixed_raw.refresh_from_db()

        c = Client()
        c.force_login(self.user)

        response = c.post(
            f"{URL}/api/flag/create",
            {
                "project": self.project.slug,
                "pipeline": self.pipeline.slug,
                "run_keys": [prefixed_raw.display_ref],
            },
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        prefixed_raw.refresh_from_db()
        assert prefixed_raw.flagged is True

        response = c.post(
            f"{URL}/api/flag/delete",
            {
                "project": self.project.slug,
                "pipeline": self.pipeline.slug,
                "run_keys": [prefixed_raw.display_ref],
            },
        )

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        prefixed_raw.refresh_from_db()
        assert prefixed_raw.flagged is False

    @patch("maxquant.Result.rawtools_qc.delay", return_value=SimpleNamespace(id="qc-task"))
    @patch(
        "maxquant.Result.rawtools_metrics.delay",
        return_value=SimpleNamespace(id="metrics-task"),
    )
    @patch("maxquant.Result.run_maxquant.delay", return_value=SimpleNamespace(id="mq-task"))
    def test__upload_raw_rejects_pipeline_uuid_outside_users_projects(
        self,
        _mock_run_maxquant,
        _mock_rawtools_metrics,
        _mock_rawtools_qc,
    ):
        other_owner = User.objects.create_user(
            email="api-other-owner@example.com",
            password="testpass123",
        )
        other_project = Project.objects.create(
            name="other project",
            description="Another test project",
            created_by=other_owner,
        )
        other_pipeline = Pipeline.objects.create(
            name="other-pipe",
            project=other_project,
            created_by=other_owner,
            fasta_file=SimpleUploadedFile("other_fasta.fasta", b">protein\nSEQUENCE"),
            mqpar_file=SimpleUploadedFile("other_mqpar.xml", b"<mqpar></mqpar>"),
            rawtools_args="-p -q -x -u -l -m -r TMT11 -chro 12TB",
        )

        c = Client()
        c.force_login(self.user)
        response = c.post(
            reverse("api:upload-raw"),
            data={
                "pid": str(other_pipeline.uuid),
                "orig_file": SimpleUploadedFile("foreign.raw", b"raw-bytes"),
            },
        )

        assert response.status_code == 403, f"Expected 403, got {response.status_code}"
        assert not RawFile.objects.filter(
            pipeline=other_pipeline,
            orig_file="upload/foreign.raw",
        ).exists()
        assert not Result.objects.filter(raw_file__pipeline=other_pipeline).exists()

    @patch("maxquant.Result.rawtools_qc.delay", return_value=SimpleNamespace(id="qc-task"))
    @patch(
        "maxquant.Result.rawtools_metrics.delay",
        return_value=SimpleNamespace(id="metrics-task"),
    )
    @patch("maxquant.Result.run_maxquant.delay", return_value=SimpleNamespace(id="mq-task"))
    def test__upload_raw_allows_project_member_and_auto_creates_result(
        self,
        _mock_run_maxquant,
        _mock_rawtools_metrics,
        _mock_rawtools_qc,
    ):
        c = Client()
        c.force_login(self.member_user)
        response = c.post(
            reverse("api:upload-raw"),
            data={
                "pid": str(self.pipeline.uuid),
                "orig_file": SimpleUploadedFile("member-upload.raw", b"raw-bytes"),
            },
        )

        assert response.status_code == 201, f"Expected 201, got {response.status_code}"
        raw_file = RawFile.objects.get(
            pipeline=self.pipeline,
            created_by=self.member_user,
            orig_file="upload/member-upload.raw",
        )
        result = Result.objects.get(raw_file=raw_file)

        assert result is not None
        assert result.raw_file.pipeline == self.pipeline

    @patch("api.views.get_protein_quant_fn", return_value=[])
    def test__protein_groups_empty_result_returns_json_object(self, _mock_get_fns):
        c = Client()
        c.force_login(self.user)
        url = f"{URL}/api/protein-groups"
        response = c.post(url, {
            "project": "project",
            "pipeline": "pipe",
            "data_range": 0,
            "raw_files": [],
            "columns": ["Score"],
            "protein_names": ["P1"],
        })

        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        assert response.json() == {}, response.content

    def test__get_protein_quant_fn_data_range_limits_results(self):
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-1.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 1),
        )
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-2.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 2),
        )
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-3.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 3),
        )

        with patch(
            "maxquant.Result.Result.create_protein_quant",
            side_effect=lambda: "protein_quant.parquet",
        ):
            fns = get_protein_quant_fn(
                self.project.slug,
                self.pipeline.slug,
                data_range=2,
                user=self.user,
            )

        assert len(fns) == 2, fns

    def test__get_protein_quant_fn_data_range_limits_filtered_raw_files(self):
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-1.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 1),
        )
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-2.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 2),
        )
        RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("fake-3.raw", b"..."),
            created_by=self.user,
            created=date(2024, 1, 3),
        )

        with patch(
            "maxquant.Result.Result.create_protein_quant",
            autospec=True,
            side_effect=lambda result: result.raw_file.logical_name,
        ):
            fns = get_protein_quant_fn(
                self.project.slug,
                self.pipeline.slug,
                data_range=2,
                raw_files=["fake-1.raw", "fake-2.raw", "fake-3.raw"],
                user=self.user,
            )

        assert fns == ["fake-2.raw", "fake-3.raw"], fns

    def test__get_protein_quant_fn_skips_schema_parse_failures(self):
        result = Result.objects.get(raw_file=self.raw_file)
        result.output_dir_maxquant.mkdir(parents=True, exist_ok=True)
        (result.output_dir_maxquant / "proteinGroups.txt").write_text(
            "\t".join(
                [
                    "Majority protein IDs",
                    "Fasta headers",
                    "Intensity",
                    "Reporter intensity corrected 1 sample",
                ]
            )
            + "\n"
            + "\t".join(["P1", "header-1", "1000", "10"])
            + "\n",
            encoding="utf-8",
        )

        fns = get_protein_quant_fn(
            self.project.slug,
            self.pipeline.slug,
            data_range=10,
            user=self.user,
        )

        assert fns == [], fns
        assert result.maxquant_status == "failed"
        assert "MaxQuant parse error" in (
            result.output_dir_maxquant / "maxquant.err"
        ).read_text(encoding="utf-8")


class ApiDemoDocsSmokeTestCase(TestCase):
    def test_seeded_demo_supports_documented_read_flow(self):
        call_command("bootstrap_demo", user="demo-api@example.com", with_results=True)

        user = User.objects.get(email="demo-api@example.com")
        project = Project.objects.get(name="Demo Project")
        pipeline = Pipeline.objects.get(name="TMT QC Demo", project=project)

        c = Client()
        c.force_login(user)

        projects_response = c.post(reverse("api:projects"))
        assert projects_response.status_code == 200, projects_response.content
        projects = projects_response.json()
        assert any(item["slug"] == project.slug for item in projects), projects

        pipelines_response = c.post(
            reverse("api:mq-pipelines"),
            {"project": project.slug},
            content_type="application/json",
        )
        assert pipelines_response.status_code == 200, pipelines_response.content
        pipelines = pipelines_response.json()
        assert any(
            item["slug"] == pipeline.slug and item["uuid"] == str(pipeline.uuid)
            for item in pipelines
        ), pipelines

        qc_response = c.post(
            reverse("api:qc-data"),
            {"project": project.slug, "pipeline": pipeline.slug, "data_range": 3},
            content_type="application/json",
        )
        assert qc_response.status_code == 200, qc_response.content
        qc_payload = qc_response.json()
        assert "RawFile" in qc_payload, qc_payload
        assert len(qc_payload["RawFile"]) == 3, qc_payload
