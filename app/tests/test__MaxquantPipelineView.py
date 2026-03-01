from django.test import TestCase
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile

from user.models import User
from project.models import Project
from maxquant.models import Pipeline, RawFile, Result


class MaxquantPipelineViewTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            email="tester@example.com", password="pass1234"
        )
        self.admin_user = User.objects.create_superuser(
            email="admin@example.com", password="pass1234"
        )
        self.other_user = User.objects.create_user(
            email="other@example.com", password="pass1234"
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
        self.owner_raw = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("owner.raw", b"..."),
            created_by=self.user,
        )
        self.other_raw = RawFile.objects.create(
            pipeline=self.pipeline,
            orig_file=SimpleUploadedFile("other.raw", b"..."),
            created_by=self.other_user,
        )
        self.owner_result = Result.objects.get(raw_file=self.owner_raw)
        self.other_result = Result.objects.get(raw_file=self.other_raw)

    def test_pipeline_view_loads(self):
        self.client.force_login(self.user)
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertIn("maxquant_runs", response.context)

    def test_pipeline_view_with_invalid_search_session_falls_back_to_default_queryset(self):
        self.client.force_login(self.user)
        session = self.client.session
        session["search-files"] = {"raw_file": "x" * 101}
        session.save()

        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        runs = list(response.context["maxquant_runs"].object_list)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].pk, self.owner_result.pk)

    def test_pipeline_view_requires_login(self):
        """Verify pipeline view requires authentication."""
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)
        self.assertEqual(response.status_code, 302)
        self.assertIn("/admin/login/", response.url)

    def test_admin_sees_user_column_and_filter(self):
        self.client.force_login(self.admin_user)
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<th>User</th>", html=False)
        self.assertContains(response, "id=\"mq-runs-user-filter\"", html=False)
        self.assertTrue(response.context["is_admin_session"])

    def test_regular_user_does_not_see_user_filter(self):
        self.client.force_login(self.user)
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, "<th>User</th>", html=False)
        self.assertNotContains(response, "id=\"mq-runs-user-filter\"", html=False)
        self.assertFalse(response.context["is_admin_session"])

    def test_admin_can_filter_runs_by_uploader(self):
        self.client.force_login(self.admin_user)
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url, {"uploader": str(self.other_user.id)})

        self.assertEqual(response.status_code, 200)
        runs = list(response.context["maxquant_runs"].object_list)
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].pk, self.other_result.pk)

    def test_pipeline_view_unauthorized_user(self):
        """Verify users cannot view pipelines they don't have access to."""
        self.client.force_login(self.other_user)
        url = reverse("maxquant:detail", kwargs={
            "project": self.project.slug,
            "pipeline": self.pipeline.slug
        })
        response = self.client.get(url)
        self.assertEqual(response.status_code, 404)
