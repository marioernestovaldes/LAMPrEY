(function () {
    function layoutUserAdminToolbar() {
        if (!document.body.classList.contains("app-user") || !document.body.classList.contains("model-user")) {
            return;
        }

        var changelist = document.getElementById("changelist");
        var changelistForm = document.getElementById("changelist-form");
        var searchToolbar = document.getElementById("toolbar");
        var objectTools = document.querySelector("#content-main > .object-tools");
        var actions = changelistForm ? changelistForm.querySelector(".actions") : null;

        if (!changelist || !changelistForm || !searchToolbar || !objectTools || !actions) {
            return;
        }

        if (changelist.querySelector(".pqc-admin-user-toolbar-row")) {
            return;
        }

        actions.querySelectorAll("select, input, button, textarea").forEach(function (field) {
            field.setAttribute("form", "changelist-form");
        });

        var primaryRow = document.createElement("div");
        primaryRow.className = "pqc-admin-user-toolbar-row pqc-admin-user-toolbar-row-primary";
        primaryRow.appendChild(searchToolbar);
        primaryRow.appendChild(objectTools);

        var secondaryRow = document.createElement("div");
        secondaryRow.className = "pqc-admin-user-toolbar-row pqc-admin-user-toolbar-row-secondary";
        secondaryRow.appendChild(actions);

        changelistForm.parentNode.insertBefore(primaryRow, changelistForm);
        changelistForm.parentNode.insertBefore(secondaryRow, changelistForm);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", layoutUserAdminToolbar);
    } else {
        layoutUserAdminToolbar();
    }
}());
