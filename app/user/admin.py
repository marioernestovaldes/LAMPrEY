from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

# from django.contrib.auth.models import User
from .models import User


class CustomUserAdmin(UserAdmin):
    list_display = ("email", "is_staff", "is_superuser")
    list_filter = ()
    readonly_fields = ("last_login", "date_joined", "uuid")
    ordering = ("email",)
    search_fields = ("first_name", "last_name", "email")  # 🖘 no username
    fieldsets = (
        (
            "Fields",
            {
                "fields": (
                    "email",
                    "uuid",
                    "date_joined",
                    "last_login",
                    "is_active",
                    "is_staff",
                    "is_superuser",
                    "groups",
                    "user_permissions",
                    "password",
                )
            },
        ),
    )
    add_fieldsets = (
        (
            None,
            {
                "classes": ("wide",),
                "fields": ("email", "password1", "password2"),
                #              🖞 without username
            },
        ),
    )

    class Media:
        css = {"all": ("css/admin-user-changelist.css",)}
        js = ("js/admin-user-changelist.js",)


admin.site.register(User, CustomUserAdmin)
