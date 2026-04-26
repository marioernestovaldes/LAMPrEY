import html
import json
import logging
from pathlib import Path

from django import forms
from django.contrib import admin, messages
from django.conf import settings
from django.http import HttpResponseRedirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from .models import (
    Pipeline,
    RawFile,
    Result,
)
from .defaults import ensure_bundled_maxquant_installed
from .tasks import run_pipeline_picked_group_fdr
from omics.proteomics.maxquant.picked_group_fdr import (
    PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS,
    validate_pipeline_for_picked_group_fdr,
)

logger = logging.getLogger(__name__)


def _read_upload_head(upload, size=4096):
    if not upload:
        return ""

    try:
        position = upload.tell()
    except (AttributeError, OSError):
        position = None

    try:
        chunk = upload.read(size)
    finally:
        if position is not None:
            upload.seek(position)

    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="ignore")
    return chunk or ""


def _read_path_head(path, size=4096):
    candidate = Path(path)
    if not candidate.is_file():
        return ""

    with candidate.open("rb") as handle:
        chunk = handle.read(size)

    return chunk.decode("utf-8", errors="ignore")


def _first_nonempty_line(text):
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _looks_like_mqpar(text):
    lowered = text.lower()
    return any(
        marker in lowered
        for marker in (
            "<?xml",
            "<maxquantparams",
            "<fastafiles>",
            "<filepaths>",
            "<experiments>",
        )
    )


def _looks_like_fasta(text):
    first_line = _first_nonempty_line(text)
    if not first_line.startswith(">"):
        return False
    return len(first_line) > 1


def _mqpar_warning_message(text):
    if not text:
        return None
    if _looks_like_fasta(text):
        return (
            "MQPAR upload warning: this file looks like a FASTA file, not an mqpar.xml file. "
            "The pipeline was saved, but MaxQuant may fail unless you replace the mqpar upload."
        )
    if not _looks_like_mqpar(text):
        return (
            "MQPAR upload warning: this file does not look like a valid mqpar.xml file. "
            "The pipeline was saved, but MaxQuant may fail unless you replace the mqpar upload."
        )
    if "<maxquantparams" not in text.lower():
        return (
            "MQPAR upload warning: this XML file does not look like an mqpar.xml file with a "
            "<MaxQuantParams> root element. The pipeline was saved, but MaxQuant may fail unless "
            "you replace the mqpar upload."
        )
    return None


def _fasta_warning_message(text):
    if not text:
        return None
    if _looks_like_mqpar(text):
        return (
            "FASTA upload warning: this file looks like an mqpar.xml file, not a FASTA file. "
            "The pipeline was saved, but downstream runs may fail unless you replace the FASTA upload."
        )
    if not _looks_like_fasta(text):
        return (
            "FASTA upload warning: this file does not look like a FASTA file. Expected the first "
            "non-empty line to start with '>'. The pipeline was saved, but downstream runs may fail "
            "unless you replace the FASTA upload."
        )
    return None


def _pipeline_file_warnings(pipeline):
    warnings = []

    mqpar_warning = _mqpar_warning_message(_read_path_head(pipeline.mqpar_path))
    if mqpar_warning:
        warnings.append(mqpar_warning)

    fasta_warning = _fasta_warning_message(_read_path_head(pipeline.fasta_path))
    if fasta_warning:
        warnings.append(fasta_warning)

    return warnings


def _comma_joined(items, fallback="-"):
    cleaned = [str(item) for item in items if str(item).strip()]
    if not cleaned:
        return fallback
    return ", ".join(cleaned)


class PipelineAdminForm(forms.ModelForm):
    class Meta:
        model = Pipeline
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upload_warnings = []

    def clean(self):
        cleaned_data = super().clean()
        self.upload_warnings = []
        fasta_file = cleaned_data.get("fasta_file")
        has_existing_fasta = bool(
            self.instance and self.instance.pk and self.instance.fasta_path.is_file()
        )

        if not fasta_file and not has_existing_fasta:
            self.add_error(
                "fasta_file",
                (
                    "A FASTA file is required to create a runnable pipeline. "
                    "Browsers clear selected file inputs after validation errors, "
                    "so if another field fails validation you must choose the FASTA file again."
                ),
            )

        mqpar_file = cleaned_data.get("mqpar_file")
        if mqpar_file:
            mqpar_warning = _mqpar_warning_message(_read_upload_head(mqpar_file))
            if mqpar_warning:
                self.upload_warnings.append(mqpar_warning)

        if fasta_file:
            fasta_warning = _fasta_warning_message(_read_upload_head(fasta_file))
            if fasta_warning:
                self.upload_warnings.append(fasta_warning)

        return cleaned_data


class RawFileAdmin(admin.ModelAdmin):
    model = RawFile

    exclude = ("md5sum", "slug")
    list_per_page = 20

    list_display = (
        "display_name",
        "owner",
        "project",
        "pipeline",
        "use_downstream",
        "flagged",
        "path",
        "created",
    )

    sortable_by = (
        "created",
        "pipeline",
        "orig_file",
        "use_downstream",
        "flagged",
        "created_by",
    )

    list_filter = ()

    search_fields = ("orig_file", "pipeline__name", "pipeline__project__name", "created_by__email")

    group_by = "pipeline"

    ordering = ("-created",)

    actions = (
        "allow_use_downstream",
        "prevent_use_downstream",
        "flag_selected",
        "unflag_selected",
        "save_and_run",
    )

    class Media:
        css = {"all": ("css/admin-shared-changelist.css",)}

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.select_related("pipeline__project", "created_by")

    @admin.display(ordering="pipeline__project__name", description="Project")
    def project(self, obj):
        return obj.pipeline.project

    @admin.display(ordering="orig_file", description="Name")
    def display_name(self, obj):
        return f"{obj.logical_name} ({obj.display_ref})"

    @admin.display(ordering="created_by__email", description="User")
    def owner(self, obj):
        return obj.created_by

    def regroup_by(self):
        return "pipeline"

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ["path", "pipeline", "created", "orig_file"]
        else:
            return ["path", "created"]

    def prevent_use_downstream(modeladmin, request, queryset):
        queryset.update(use_downstream=False)

    def allow_use_downstream(modeladmin, request, queryset):
        queryset.update(use_downstream=True)

    @admin.action(description="Flag selected raw files")
    def flag_selected(modeladmin, request, queryset):
        queryset.update(flagged=True)

    @admin.action(description="Unflag selected raw files")
    def unflag_selected(modeladmin, request, queryset):
        queryset.update(flagged=False)

    def save_and_run(modeladmin, request, queryset):
        for raw_file in queryset:
            raw_file.save()


class PipelineAdmin(admin.ModelAdmin):
    form = PipelineAdminForm

    ordering = ("name",)

    list_filter = ()

    list_display = ("name", "project", "picked_group_fdr_admin_status", "created", "created_by")
    search_fields = ("name", "project__name", "created_by__email", "description")

    sortable_by = ("name", "created", "pipeline")

    def _default_pipeline_name(self):
        index = 1
        while True:
            candidate = f"Pipeline {index}"
            if not Pipeline.objects.filter(name=candidate).exists():
                return candidate
            index += 1

    def get_changeform_initial_data(self, request):
        initial = super().get_changeform_initial_data(request)

        ensure_bundled_maxquant_installed()

        if not initial.get("maxquant_executable"):
            initial["maxquant_executable"] = settings.DEFAULT_MAXQUANT_EXECUTABLE

        if not initial.get("name"):
            initial["name"] = self._default_pipeline_name()

        if not initial.get("description"):
            initial["description"] = (
                "Describe the pipeline purpose, processing settings, and sample scope."
            )

        return initial

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        if db_field.name == "maxquant_executable":
            ensure_bundled_maxquant_installed()
        formfield = super().formfield_for_dbfield(db_field, request, **kwargs)

        if db_field.name == "project" and hasattr(formfield.widget, "can_add_related"):
            formfield.widget.can_add_related = False
            formfield.widget.can_change_related = False
            formfield.widget.can_delete_related = False
            formfield.widget.can_view_related = False

        if db_field.name == "maxquant_executable":
            choices = []
            for value, label in formfield.choices:
                if value == settings.DEFAULT_MAXQUANT_EXECUTABLE:
                    label = settings.DEFAULT_MAXQUANT_LABEL
                choices.append((value, label))
            formfield.choices = choices
            formfield.help_text = (
                "Bundled MaxQuant 2.4.12.0 is installed automatically and recommended. "
                "Choose a different installed executable only if needed."
            )

        if db_field.name == "mqpar_file":
            formfield.help_text = (
                "Leave this empty to use the bundled mqpar_2.4.12.0.xml template that matches "
                "the recommended MaxQuant version. Upload a file only to override it. "
                "The form performs a basic content check and warns if the upload does not look "
                "like a MaxQuant parameter XML file."
            )

        if db_field.name == "fasta_file":
            formfield.help_text = (
                "Required. If the form fails validation for any reason, browsers clear file "
                "inputs, so you must select the FASTA file again before saving. "
                "The form also checks that the uploaded file looks like a FASTA file."
            )

        return formfield

    def get_fieldsets(self, request, obj=None):
        base = (
            (None, {"fields": ("project", "name", "created", "created_by", "description")}),
            (
                "MaxQuant",
                {
                    "fields": (
                        "maxquant_executable",
                        "mqpar_file",
                        "fasta_file",
                    )
                },
            ),
            ("RawTools", {"fields": ("rawtools_args",)}),
            ("Info", {"fields": ("slug", "uuid", "path", "fasta_path", "mqpar_path")}),
        )
        if obj is None:
            return base
        return base + (
            (
                "Picked Group FDR",
                {
                    "fields": (
                        "picked_group_fdr_help_display",
                        "picked_group_fdr_run_action",
                        "picked_group_fdr_clear_action",
                        "picked_group_fdr_validation_summary",
                        "picked_group_fdr_status_summary",
                        "picked_group_fdr_last_output_dir_display",
                        "picked_group_fdr_last_manifest_display",
                        "picked_group_fdr_last_error_display",
                    )
                },
            ),
        )

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return (
                "created",
                "created_by",
                "slug",
                "uuid",
                "path",
                "fasta_path",
                "mqpar_path",
                "download_fasta",
                "download_mqpar",
                "project",
                "picked_group_fdr_help_display",
                "picked_group_fdr_run_action",
                "picked_group_fdr_clear_action",
                "picked_group_fdr_validation_summary",
                "picked_group_fdr_status_summary",
                "picked_group_fdr_last_output_dir_display",
                "picked_group_fdr_last_manifest_display",
                "picked_group_fdr_last_error_display",
            )
        else:
            return (
                "created",
                "created_by",
                "slug",
                "uuid",
                "path",
                "fasta_path",
                "mqpar_path",
                "download_fasta",
                "download_mqpar",
                "picked_group_fdr_help_display",
                "picked_group_fdr_run_action",
                "picked_group_fdr_clear_action",
                "picked_group_fdr_validation_summary",
                "picked_group_fdr_status_summary",
                "picked_group_fdr_last_output_dir_display",
                "picked_group_fdr_last_manifest_display",
                "picked_group_fdr_last_error_display",
            )

    class Media:
        css = {"all": ("css/admin-shared-changelist.css",)}

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        for warning in getattr(form, "upload_warnings", ()):
            self.message_user(request, warning, level=messages.WARNING)

    def change_view(self, request, object_id, form_url="", extra_context=None):
        response = super().change_view(request, object_id, form_url, extra_context)

        if request.method == "GET":
            obj = self.get_object(request, object_id)
            if obj is not None:
                for warning in _pipeline_file_warnings(obj):
                    self.message_user(request, warning, level=messages.WARNING)

        return response

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/run-picked-group-fdr/",
                self.admin_site.admin_view(self.run_picked_group_fdr_view),
                name="maxquant_pipeline_run_picked_group_fdr",
            ),
            path(
                "<path:object_id>/clear-picked-group-fdr/",
                self.admin_site.admin_view(self.clear_picked_group_fdr_view),
                name="maxquant_pipeline_clear_picked_group_fdr",
            ),
        ]
        return custom_urls + urls

    def picked_group_fdr_run_action(self, obj):
        if not obj or not obj.pk:
            return "-"
        if obj.picked_group_fdr_is_active:
            return "Picked-group-FDR is currently queued or running."
        url = reverse(
            "admin:maxquant_pipeline_run_picked_group_fdr",
            args=[obj.pk],
        )
        return format_html(
            '<a class="button" href="{}">Run picked-group FDR</a>',
            url,
        )

    picked_group_fdr_run_action.short_description = "Run Picked Group FDR"

    def picked_group_fdr_clear_action(self, obj):
        if not obj or not obj.pk:
            return "-"
        if obj.picked_group_fdr_is_active:
            return "Picked-group-FDR status cannot be cleared while queued or running."
        if (
            obj.picked_group_fdr_last_status == "never_run"
            and not obj.picked_group_fdr_last_manifest
            and not obj.picked_group_fdr_last_run_dir
        ):
            return "-"
        url = reverse(
            "admin:maxquant_pipeline_clear_picked_group_fdr",
            args=[obj.pk],
        )
        return format_html(
            '<a class="button" href="{}">Clear picked-group FDR status</a>',
            url,
        )

    picked_group_fdr_clear_action.short_description = "Clear Picked Group FDR Status"

    def picked_group_fdr_help_display(self, obj):
        return format_html(
            """
            <div style="max-width: 72em;">
                <p><strong>Purpose:</strong> Picked-group-FDR performs a pipeline-level protein-group
                false discovery rate correction across the eligible MaxQuant runs in this pipeline.
                It uses the pipeline FASTA file, the pipeline <code>mqpar.xml</code>, and the
                <code>evidence.txt</code> file from each completed run.</p>
                <p><strong>When to use it:</strong> run this after MaxQuant has completed for the
                runs you want to compare together. The current integration is intended for
                cross-run protein-group validation and result review, not for single-run QC.</p>
                <p><strong>Current behavior:</strong> the correction is admin-only, writes results
                side-by-side under the pipeline output directory, and does not replace the default
                MaxQuant outputs used elsewhere in the application.</p>
                <p><strong>Requirements:</strong> eligible runs must have readable
                <code>evidence.txt</code> files, the pipeline must have a readable FASTA and
                <code>mqpar.xml</code>, the MaxQuant configuration must use
                <code>proteinFdr = 1</code> and <code>peptideFdr = 1</code>, and Mokapot
                rescoring must finish successfully.</p>
                <p><strong>Status guide:</strong> <code>completed</code> means the corrected
                protein-group output files were written. <code>failed</code> means the correction
                stopped before completion; use the saved error summary and
                <code>picked_group_fdr.err</code> in the last output directory for the technical
                details.</p>
            </div>
            """
        )

    picked_group_fdr_help_display.short_description = "Help"

    def picked_group_fdr_validation_summary(self, obj):
        if not obj or not obj.pk:
            return "-"
        validation = validate_pipeline_for_picked_group_fdr(obj)
        if validation.get("status") == "ok":
            return validation.get("message")
        return validation.get("message", "Pipeline is not eligible for picked-group-FDR.")

    picked_group_fdr_validation_summary.short_description = "Eligibility"

    def picked_group_fdr_status_summary(self, obj):
        if not obj or not obj.pk:
            return "-"
        submitted = obj.picked_group_fdr_task_submitted_at
        finished = obj.picked_group_fdr_last_finished_at
        parts = [f"Status: {obj.picked_group_fdr_last_status}"]
        if submitted:
            parts.append(f"Queued: {submitted}")
        if finished:
            parts.append(f"Finished: {finished}")
        return " | ".join(parts)

    picked_group_fdr_status_summary.short_description = "Last Run Status"

    def picked_group_fdr_admin_status(self, obj):
        return obj.picked_group_fdr_last_status

    picked_group_fdr_admin_status.short_description = "Picked Group FDR"

    def picked_group_fdr_last_output_dir_display(self, obj):
        if not obj or not obj.picked_group_fdr_last_run_path:
            return "-"
        return str(obj.picked_group_fdr_last_run_path)

    picked_group_fdr_last_output_dir_display.short_description = "Last Output Directory"

    def picked_group_fdr_last_manifest_display(self, obj):
        manifest = obj.picked_group_fdr_manifest_data if obj else {}
        if not manifest:
            return "-"
        included_results = manifest.get("included_results", [])
        excluded_results = manifest.get("excluded_results", [])
        mqpar_settings = manifest.get("mqpar_settings", {})
        artifacts = manifest.get("artifacts", {})
        log_excerpt = manifest.get("log_excerpt", {})

        included_names = [
            row.get("raw_file") or f"Result {row.get('result_id')}" for row in included_results
        ]
        excluded_summaries = []
        for row in excluded_results[:10]:
            raw_file = row.get("raw_file") or f"Result {row.get('result_id')}"
            reason = row.get("reason") or "Excluded"
            excluded_summaries.append(f"{raw_file}: {reason}")
        if len(excluded_results) > 10:
            excluded_summaries.append(
                f"... and {len(excluded_results) - 10} more excluded run(s)"
            )

        output_files = []
        if artifacts.get("protein_groups_filtered"):
            output_files.append("proteinGroups.fdr1.txt")
        if artifacts.get("protein_groups"):
            output_files.append("proteinGroups.txt")
        if artifacts.get("stdout"):
            output_files.append("picked_group_fdr.out")
        if artifacts.get("stderr"):
            output_files.append("picked_group_fdr.err")
        if artifacts.get("per_result_protein_groups"):
            output_files.append(PICKED_GROUP_FDR_PER_RESULT_PROTEIN_GROUPS)

        stdout_excerpt = log_excerpt.get("stdout", [])
        stderr_excerpt = log_excerpt.get("stderr", [])
        log_html = ""
        if stdout_excerpt or stderr_excerpt:
            excerpt_blocks = []
            if stdout_excerpt:
                excerpt_blocks.append(
                    f"<p><strong>Stdout excerpt:</strong></p><pre>{html.escape(chr(10).join(stdout_excerpt))}</pre>"
                )
            if stderr_excerpt:
                excerpt_blocks.append(
                    f"<p><strong>Stderr excerpt:</strong></p><pre>{html.escape(chr(10).join(stderr_excerpt))}</pre>"
                )
            log_html = "".join(excerpt_blocks)

        return format_html(
            """
            <div style="max-width: 72em;">
                <p><strong>Included runs:</strong> {} ({})</p>
                <p><strong>Excluded runs:</strong> {}</p>
                <p><strong>Key settings:</strong> proteinFdr={}, peptideFdr={}, enzyme={}</p>
                <p><strong>Artifacts:</strong> {}</p>
                {}
            </div>
            """,
            len(included_results),
            _comma_joined(included_names),
            _comma_joined(excluded_summaries, fallback="None"),
            mqpar_settings.get("protein_fdr", "-"),
            mqpar_settings.get("peptide_fdr", "-"),
            mqpar_settings.get("enzyme_name", "-"),
            _comma_joined(output_files, fallback="Not available yet"),
            mark_safe(log_html),
        )

    picked_group_fdr_last_manifest_display.short_description = "Run Summary"

    def picked_group_fdr_last_error_display(self, obj):
        if not obj or not obj.picked_group_fdr_last_error:
            return "-"
        return format_html("<pre>{}</pre>", obj.picked_group_fdr_last_error)

    picked_group_fdr_last_error_display.short_description = "Last Error"

    def run_picked_group_fdr_view(self, request, object_id):
        obj = self.get_object(request, object_id)
        if obj is None:
            self.message_user(request, "Pipeline not found.", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:maxquant_pipeline_changelist"))
        if obj.picked_group_fdr_is_active:
            self.message_user(
                request,
                "Picked-group-FDR is already queued or running for this pipeline.",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(reverse("admin:maxquant_pipeline_change", args=[obj.pk]))

        validation = validate_pipeline_for_picked_group_fdr(obj)
        if validation.get("status") != "ok":
            logger.warning(
                "[picked_group_fdr] admin queue rejected pipeline_id=%s reason=%s",
                obj.pk,
                validation.get("message"),
            )
            self.message_user(
                request,
                validation.get("message", "Pipeline is not eligible for picked-group-FDR."),
                level=messages.ERROR,
            )
            return HttpResponseRedirect(reverse("admin:maxquant_pipeline_change", args=[obj.pk]))

        run_dir = timezone.now().strftime("%Y%m%d-%H%M%S")
        manifest = {
            "pipeline_id": obj.pk,
            "pipeline_name": obj.name,
            "project": str(obj.project),
            "requested_at": timezone.now().isoformat(),
            "fasta_path": validation["fasta_path"],
            "mqpar_path": validation["mqpar_path"],
            "mqpar_settings": validation["mqpar_settings"],
            "included_results": validation["included_results"],
            "excluded_results": validation["excluded_results"],
            "run_dir": str(obj.picked_group_fdr_root / run_dir),
        }
        logger.info(
            "[picked_group_fdr] admin queue requested pipeline_id=%s run_dir=%s included=%s excluded=%s user=%s",
            obj.pk,
            run_dir,
            len(validation["included_results"]),
            len(validation["excluded_results"]),
            getattr(request.user, "pk", None),
        )
        async_result = run_pipeline_picked_group_fdr.delay(
            obj.pk,
            [item["result_id"] for item in validation["included_results"]],
            run_dir=run_dir,
        )
        obj.picked_group_fdr_task_id = async_result.id
        obj.picked_group_fdr_task_submitted_at = timezone.now()
        obj.picked_group_fdr_last_started_at = None
        obj.picked_group_fdr_last_finished_at = None
        obj.picked_group_fdr_last_status = "requested"
        obj.picked_group_fdr_last_error = ""
        obj.picked_group_fdr_last_run_dir = run_dir
        obj.picked_group_fdr_last_manifest = json.dumps(manifest, sort_keys=True)
        obj.save(
            update_fields=[
                "picked_group_fdr_task_id",
                "picked_group_fdr_task_submitted_at",
                "picked_group_fdr_last_started_at",
                "picked_group_fdr_last_finished_at",
                "picked_group_fdr_last_status",
                "picked_group_fdr_last_error",
                "picked_group_fdr_last_run_dir",
                "picked_group_fdr_last_manifest",
            ]
        )
        self.message_user(
            request,
            f"Queued picked-group-FDR for {len(validation['included_results'])} run(s); "
            f"{len(validation['excluded_results'])} run(s) excluded.",
            level=messages.SUCCESS,
        )
        logger.info(
            "[picked_group_fdr] admin queue succeeded pipeline_id=%s task_id=%s run_dir=%s",
            obj.pk,
            async_result.id,
            run_dir,
        )
        return HttpResponseRedirect(reverse("admin:maxquant_pipeline_change", args=[obj.pk]))

    def clear_picked_group_fdr_view(self, request, object_id):
        obj = self.get_object(request, object_id)
        if obj is None:
            self.message_user(request, "Pipeline not found.", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:maxquant_pipeline_changelist"))
        if obj.picked_group_fdr_is_active:
            logger.warning(
                "[picked_group_fdr] admin clear rejected active pipeline_id=%s status=%s",
                obj.pk,
                obj.picked_group_fdr_last_status,
            )
            self.message_user(
                request,
                "Picked-group-FDR status cannot be cleared while queued or running.",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(reverse("admin:maxquant_pipeline_change", args=[obj.pk]))

        obj.picked_group_fdr_task_id = ""
        obj.picked_group_fdr_task_submitted_at = None
        obj.picked_group_fdr_last_started_at = None
        obj.picked_group_fdr_last_finished_at = None
        obj.picked_group_fdr_last_status = "never_run"
        obj.picked_group_fdr_last_error = ""
        obj.picked_group_fdr_last_run_dir = ""
        obj.picked_group_fdr_last_manifest = ""
        obj.save(
            update_fields=[
                "picked_group_fdr_task_id",
                "picked_group_fdr_task_submitted_at",
                "picked_group_fdr_last_started_at",
                "picked_group_fdr_last_finished_at",
                "picked_group_fdr_last_status",
                "picked_group_fdr_last_error",
                "picked_group_fdr_last_run_dir",
                "picked_group_fdr_last_manifest",
            ]
        )
        self.message_user(
            request,
            "Cleared picked-group-FDR status for this pipeline. Output files were not deleted.",
            level=messages.SUCCESS,
        )
        logger.info(
            "[picked_group_fdr] admin clear succeeded pipeline_id=%s user=%s",
            obj.pk,
            getattr(request.user, "pk", None),
        )
        return HttpResponseRedirect(reverse("admin:maxquant_pipeline_change", args=[obj.pk]))


class ResultAdmin(admin.ModelAdmin):
    list_per_page = 20

    readonly_fields = (
        "raw_file",
        "created",
        "created_by",
        "path",
        "link",
        "run_dir",
        "raw_fn",
        "mqpar_fn",
        "fasta_fn",
        "pipeline",
        "parquet_path",
        "create_protein_quant",
        "n_files_maxquant",
        "n_files_rawtools_metrics",
        "n_files_rawtools_qc",
        "maxquant_execution_time",
        "project",
        "maxquant_errors",
        "rawtools_qc_errors",
        "rawtools_metrics_errors",
        "download_raw",
    )

    list_display = (
        "display_name",
        "owner",
        "project",
        "pipeline",
        "n_files_maxquant",
        "n_files_rawtools_metrics",
        "n_files_rawtools_qc",
        "status_protein_quant_parquet",
        "maxquant_execution_time",
        "created",
    )

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "project",
                    "pipeline",
                    "created",
                    "raw_file",
                    "created_by",
                    "link",
                    "download_raw",
                )
            },
        ),
        ("Paths", {"fields": ("raw_fn", "mqpar_fn", "fasta_fn", "run_dir", "path")}),
        (
            "Info",
            {
                "fields": (
                    "n_files_maxquant",
                    "maxquant_execution_time",
                    "n_files_rawtools_metrics",
                    "n_files_rawtools_qc",
                )
            },
        ),
        (
            "Errors",
            {
                "fields": (
                    "maxquant_errors",
                    "rawtools_qc_errors",
                    "rawtools_metrics_errors",
                )
            },
        ),
    )

    ordering = ("-created",)

    list_filter = ()

    search_fields = (
        "raw_file__orig_file",
        "raw_file__pipeline__name",
        "raw_file__pipeline__project__name",
        "created_by__email",
    )

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.select_related("raw_file__pipeline__project", "created_by")

    def download_raw(self, obj):
        return obj.raw_file.download

    def project(self, obj):
        return obj.raw_file.pipeline.project

    @admin.display(ordering="raw_file__orig_file", description="Name")
    def display_name(self, obj):
        return f"{obj.raw_file.logical_name} ({obj.raw_file.display_ref})"

    @admin.display(ordering="created_by__email", description="User")
    def owner(self, obj):
        return obj.created_by

    def regroup_by(self):
        return ("project", "pipeline")

    def rerun_maxquant(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_maxquant(rerun=True)

    def rerun_rawtools(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_rawtools_metrics(rerun=True)
            mq_run.run_rawtools_qc(rerun=True)

    def rerun_rawtools_qc(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_rawtools_qc(rerun=True)

    def rerun_rawtools_metrics(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_rawtools_metrics(rerun=True)

    def start_maxquant(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_maxquant(rerun=False)

    def start_rawtools(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_rawtools_qc(rerun=False)
            mq_run.run_rawtools_metrics(rerun=False)

    def start_all(modeladmin, request, queryset):
        for mq_run in queryset:
            mq_run.run_maxquant(rerun=False)
            mq_run.run_rawtools_qc(rerun=False)
            mq_run.run_rawtools_metrics(rerun=False)

    actions = [
        start_all,
        start_maxquant,
        start_rawtools,
        rerun_maxquant,
        rerun_rawtools,
        rerun_rawtools_qc,
        rerun_rawtools_metrics,
    ]

    class Media:
        css = {"all": ("css/admin-shared-changelist.css",)}


class MaxQuantExecutableAdmin(admin.ModelAdmin):

    fieldsets = ((None, {"fields": ("created", "filename", "description")}),)

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ("created", "filename")
        else:
            return ("created",)


admin.site.register(Pipeline, PipelineAdmin)
#admin.site.register(MaxQuantExecutable, MaxQuantExecutableAdmin)
admin.site.register(RawFile, RawFileAdmin)
admin.site.register(Result, ResultAdmin)
