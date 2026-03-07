from importlib import import_module

from django.conf import settings
from django.test import SimpleTestCase


class AppEntrypointsTestCase(SimpleTestCase):
    def test_settings_points_asgi_to_main_entrypoint(self):
        self.assertEqual(settings.ASGI_APPLICATION, "main.asgi.application")

    def test_wsgi_entrypoint_imports_application(self):
        module = import_module("main.wsgi")

        self.assertTrue(hasattr(module, "application"))
        self.assertIsNotNone(module.application)

    def test_asgi_entrypoint_imports_application(self):
        module = import_module("main.asgi")

        self.assertTrue(hasattr(module, "application"))
        self.assertIsNotNone(module.application)
