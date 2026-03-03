from django.contrib import admin

from .models import Project


# class MemberShipInline(admin.TabularInline):
#    model = Project.users.through


class ProjectAdmin(admin.ModelAdmin):
    readonly_fields = ("pk", "path", "slug", "created", "path_exists", "created_by")
    list_display = ("pk", "name", "path", "path_exists", "created_by", "created")
    search_fields = ("name", "slug", "path", "created_by__email")

    # inlines = [
    #    MemberShipInline,
    # ]

    filter_horizontal = ("users",)

    def _default_project_name(self):
        index = 1
        while True:
            candidate = f"Project {index}"
            if not Project.objects.filter(name=candidate).exists():
                return candidate
            index += 1

    def get_changeform_initial_data(self, request):
        initial = super().get_changeform_initial_data(request)

        if not initial.get("name"):
            initial["name"] = self._default_project_name()

        if not initial.get("description"):
            initial["description"] = (
                "Describe the project goals, samples, and any notes for collaborators."
            )

        if request.user.is_authenticated and not initial.get("users"):
            initial["users"] = [request.user.pk]

        return initial

    class Media:
        css = {"all": ("css/admin-shared-changelist.css",)}


admin.site.register(Project, ProjectAdmin)
