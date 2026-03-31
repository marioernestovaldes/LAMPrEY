from django.test import TestCase
from django.urls import reverse

from user.models import User
from project.models import Project


class ProjectListViewTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(
            email="tester@example.com", password="pass1234"
        )
        Project.objects.create(name="Project 1", description="First project", created_by=self.user)
        Project.objects.create(name="Project 2", description="Second project", created_by=self.user)

    def test_projects_render_on_list_page(self):
        self.client.force_login(self.user)
        url = reverse("project:list")
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Project 1")
        self.assertContains(response, "Project 2")

    def test_long_project_description_is_truncated_in_table(self):
        Project.objects.create(
            name="Project 3",
            description="Long project description. " * 20,
            created_by=self.user,
        )

        self.client.force_login(self.user)
        url = reverse("project:list")
        response = self.client.get(url)

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="table-description-text"', html=False)
        self.assertContains(response, 'title="Long project description.', html=False)
