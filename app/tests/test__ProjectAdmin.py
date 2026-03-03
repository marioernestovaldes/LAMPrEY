from django.test import TestCase
from django.urls import reverse

from project.models import Project
from user.models import User


class ProjectAdminTestCase(TestCase):
    def setUp(self):
        self.admin_user = User.objects.create_superuser(
            email="admin@example.com", password="pass1234"
        )

    def test_add_form_prefills_project_defaults(self):
        self.client.force_login(self.admin_user)

        response = self.client.get(reverse("admin:project_project_add"))

        self.assertEqual(response.status_code, 200)
        form = response.context["adminform"].form

        self.assertTrue(form.initial["name"].startswith("Project "))
        self.assertEqual(
            form.initial["description"],
            "Describe the project goals, samples, and any notes for collaborators.",
        )
        self.assertEqual(form.initial["users"], [self.admin_user.pk])
        self.assertEqual(form.initial["name"], "Project 1")

    def test_add_form_uses_next_available_project_name(self):
        Project.objects.create(
            name="Project 1",
            description="Existing project",
            created_by=self.admin_user,
        )

        self.client.force_login(self.admin_user)
        response = self.client.get(reverse("admin:project_project_add"))

        self.assertEqual(response.status_code, 200)
        form = response.context["adminform"].form
        self.assertEqual(form.initial["name"], "Project 2")
