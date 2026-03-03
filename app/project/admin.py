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

    class Media:
        css = {"all": ("css/admin-shared-changelist.css",)}


admin.site.register(Project, ProjectAdmin)
