from types import MethodType

from django.contrib import admin


def _sort_setup_models(models):
    preferred_order = {
        "Projects": 0,
        "Pipelines": 1,
        "RawFiles": 2,
        "Results": 3,
    }
    return sorted(
        models,
        key=lambda model: (preferred_order.get(model["name"], 99), model["name"].lower()),
    )


def _sort_user_models(models):
    preferred_order = {
        "Users": 0,
        "Groups": 1,
    }
    return sorted(
        models,
        key=lambda model: (preferred_order.get(model["name"], 99), model["name"].lower()),
    )


def _custom_get_app_list(self, request, app_label=None):
    app_dict = self._build_app_dict(request, app_label)

    if app_label in {None, "maxquant"}:
        setup_app = app_dict.get("maxquant")
        project_app = app_dict.pop("project", None) if app_label is None else app_dict.get("project")

        if setup_app is not None:
            setup_app["name"] = "Setup"
            setup_models = list(setup_app.get("models", []))
            if project_app is not None:
                setup_models = list(project_app.get("models", [])) + setup_models
            setup_app["models"] = _sort_setup_models(setup_models)
            app_dict["maxquant"] = setup_app

    if app_label in {None, "user"}:
        user_app = app_dict.get("user")
        auth_app = app_dict.pop("auth", None) if app_label is None else app_dict.get("auth")

        if user_app is not None:
            user_app["name"] = "Users"
            user_models = list(user_app.get("models", []))
            if auth_app is not None:
                user_models = user_models + list(auth_app.get("models", []))
            user_app["models"] = _sort_user_models(user_models)
            app_dict["user"] = user_app

    app_order = {
        "user": 0,
        "maxquant": 1,
        "api": 3,
        "dashboards": 4,
    }

    app_list = sorted(
        app_dict.values(),
        key=lambda app: (app_order.get(app["app_label"], 99), app["name"].lower()),
    )

    for app in app_list:
        if app["app_label"] == "maxquant":
            app["models"] = _sort_setup_models(app["models"])
        elif app["app_label"] == "user":
            app["models"] = _sort_user_models(app["models"])
        else:
            app["models"].sort(key=lambda model: model["name"])

    return app_list


admin.site.get_app_list = MethodType(_custom_get_app_list, admin.site)
