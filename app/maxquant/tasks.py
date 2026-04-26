import os
import logging
import signal
import subprocess
import time
import json
from contextlib import ExitStack
from celery import shared_task
from django.apps import apps
from django.utils import timezone

from omics.proteomics.maxquant import MaxquantRunner
from omics.proteomics.maxquant.picked_group_fdr import (
    collect_pipeline_evidence_inputs,
    format_picked_group_fdr_failure,
    run_picked_group_fdr,
    validate_pipeline_for_picked_group_fdr,
    write_per_result_picked_group_fdr_quant_files,
)

from omics.proteomics.rawtools.quality_control import (
    rawtools_metrics_cmd,
    rawtools_metrics_spec,
    rawtools_qc_cmd,
    rawtools_qc_spec,
)
from maxquant.dashboard_cache import warm_dashboard_caches_for_result

PROCESS_TRACKING_FIELDS = {
    "maxquant": ("maxquant_pid", "maxquant_pgid"),
    "rawtools_metrics": ("rawtools_metrics_pid", "rawtools_metrics_pgid"),
    "rawtools_qc": ("rawtools_qc_pid", "rawtools_qc_pgid"),
}


def _warm_dashboard_caches(result_id):
    if not result_id:
        return
    Result = apps.get_model("maxquant", "Result")
    result = Result.objects.filter(pk=result_id).select_related("raw_file__pipeline__project").first()
    if result is None:
        return
    warm_dashboard_caches_for_result(result)


def _update_pipeline_picked_group_fdr_state(pipeline_id, **updates):
    if not pipeline_id:
        return
    Pipeline = apps.get_model("maxquant", "Pipeline")
    Pipeline.objects.filter(pk=pipeline_id).update(**updates)


def _safe_float(env_name, default):
    try:
        return float(os.getenv(env_name, default))
    except (TypeError, ValueError):
        return float(default)


def _safe_int(env_name, default):
    try:
        return int(os.getenv(env_name, default))
    except (TypeError, ValueError):
        return int(default)


def _available_memory_gb():
    # Linux containers expose MemAvailable in /proc/meminfo.
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    kb = float(line.split()[1])
                    return kb / 1024.0 / 1024.0
    except Exception:
        return None
    return None


def _normalized_load():
    try:
        load_1m = os.getloadavg()[0]
    except OSError:
        return None
    n_cpu = os.cpu_count() or 1
    return load_1m / float(n_cpu)


def _resources_available(min_free_mem_gb, max_load_per_cpu):
    available_gb = _available_memory_gb()
    norm_load = _normalized_load()

    mem_ok = True if available_gb is None else (available_gb >= min_free_mem_gb)
    load_ok = True if norm_load is None else (norm_load <= max_load_per_cpu)
    return mem_ok and load_ok, available_gb, norm_load


def _defer_if_busy(task, kind, min_free_mem_gb, max_load_per_cpu):
    ok, avail_gb, norm_load = _resources_available(
        min_free_mem_gb=min_free_mem_gb,
        max_load_per_cpu=max_load_per_cpu,
    )
    if ok:
        return

    retry_seconds = _safe_int("RESOURCE_RETRY_SECONDS", 60)
    logging.info(
        "[%s] Deferring for %ss (min_mem=%.1fGB, avail_mem=%sGB, max_load=%.2f, load=%s)",
        kind,
        retry_seconds,
        min_free_mem_gb,
        "?" if avail_gb is None else f"{avail_gb:.2f}",
        max_load_per_cpu,
        "?" if norm_load is None else f"{norm_load:.2f}",
    )
    raise task.retry(countdown=retry_seconds)


def _is_canceled_result(result_id):
    if not result_id:
        return False
    Result = apps.get_model("maxquant", "Result")
    return Result.objects.filter(pk=result_id, cancel_requested_at__isnull=False).exists()


def _terminate_process_group(proc, grace_seconds=5):
    if proc is None:
        return
    pgid = proc.pid
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except Exception as exc:
        logging.warning("Failed to SIGTERM process group for pid=%s: %s", pgid, exc)
        return

    def _pg_exists(group_id):
        try:
            os.killpg(group_id, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            # Process group exists but is not signalable by this user.
            return True
        except Exception:
            return True

    deadline = time.monotonic() + max(0, grace_seconds)
    while time.monotonic() < deadline:
        if not _pg_exists(pgid):
            return
        time.sleep(0.2)

    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except Exception as exc:
        logging.warning("Failed to SIGKILL process group for pid=%s: %s", pgid, exc)


def _set_running_process(result_id, tracking_key, proc):
    if not result_id or not tracking_key or proc is None:
        return
    fields = PROCESS_TRACKING_FIELDS.get(tracking_key)
    if fields is None:
        return
    pid_field, pgid_field = fields
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        pgid = None
    except OSError as exc:
        logging.warning(
            "[%s] Failed to resolve pgid for pid=%s: %s",
            tracking_key,
            proc.pid,
            exc,
        )
        pgid = None

    Result = apps.get_model("maxquant", "Result")
    Result.objects.filter(pk=result_id).update(
        **{
            pid_field: proc.pid,
            pgid_field: pgid,
        }
    )


def _clear_running_process(result_id, tracking_key, pid=None, pgid=None):
    if not result_id or not tracking_key:
        return
    fields = PROCESS_TRACKING_FIELDS.get(tracking_key)
    if fields is None:
        return
    pid_field, pgid_field = fields
    filters = {"pk": result_id}
    if pid is not None:
        filters[pid_field] = pid
    if pgid is not None:
        filters[pgid_field] = pgid
    Result = apps.get_model("maxquant", "Result")
    Result.objects.filter(**filters).update(
        **{
            pid_field: None,
            pgid_field: None,
        }
    )


def _reap_process(proc, timeout_seconds=2.0):
    if proc is None:
        return
    try:
        proc.wait(timeout=max(0.1, float(timeout_seconds)))
    except subprocess.TimeoutExpired:
        # Keep non-blocking behavior for worker threads; we'll try once more.
        try:
            proc.poll()
        except Exception:
            return
    except Exception:
        return


def _run_cancelable_process(
    cmd,
    kind,
    result_id=None,
    tracking_key=None,
    shell=False,
    executable=None,
    cwd=None,
    stdout_path=None,
    stderr_path=None,
):
    poll_seconds = max(0.2, _safe_float("CANCEL_POLL_SECONDS", 2.0))
    kill_grace_seconds = _safe_int("CANCEL_KILL_GRACE_SECONDS", 5)
    with ExitStack() as stack:
        stdout_handle = (
            stack.enter_context(open(stdout_path, "w", encoding="utf-8"))
            if stdout_path
            else None
        )
        stderr_handle = (
            stack.enter_context(open(stderr_path, "w", encoding="utf-8"))
            if stderr_path
            else None
        )
        try:
            proc = subprocess.Popen(
                cmd,
                shell=shell,
                executable=executable,
                cwd=cwd,
                stdout=stdout_handle,
                stderr=stderr_handle,
                preexec_fn=os.setsid,
            )
        except Exception as exc:
            logging.exception("[%s] Failed to start command: %s", kind, exc)
            return 1

        pgid = None
        try:
            pgid = os.getpgid(proc.pid)
        except ProcessLookupError:
            pass
        except OSError as exc:
            logging.warning("[%s] Failed to read process group for pid=%s: %s", kind, proc.pid, exc)

        _set_running_process(result_id, tracking_key, proc)
        logging.info("[%s] started pid=%s pgid=%s", kind, proc.pid, pgid)
        try:
            while True:
                rc = proc.poll()
                if rc is not None:
                    logging.info("[%s] finished pid=%s rc=%s", kind, proc.pid, rc)
                    return rc

                if _is_canceled_result(result_id):
                    logging.info(
                        "[%s] cancel requested during execution; terminating pid=%s pgid=%s",
                        kind,
                        proc.pid,
                        pgid,
                    )
                    _terminate_process_group(proc, grace_seconds=kill_grace_seconds)
                    _reap_process(proc)
                    return -1

                time.sleep(poll_seconds)
        finally:
            _clear_running_process(
                result_id=result_id,
                tracking_key=tracking_key,
                pid=proc.pid,
                pgid=pgid,
            )


@shared_task(bind=True, max_retries=None)
def rawtools_metrics(
    self, raw, output_dir, arguments=None, rerun=False, result_id=None
):
    if _is_canceled_result(result_id):
        logging.info("[rawtools_metrics] canceled before start (result_id=%s)", result_id)
        return
    _defer_if_busy(
        task=self,
        kind="rawtools_metrics",
        min_free_mem_gb=_safe_float("MIN_FREE_MEM_GB_RAWTOOLS", 2),
        max_load_per_cpu=_safe_float("MAX_LOAD_PER_CPU_RAWTOOLS", 0.95),
    )
    cmd = rawtools_metrics_cmd(
        raw=raw, output_dir=output_dir, rerun=rerun, arguments=arguments
    )
    if cmd is not None:
        if _is_canceled_result(result_id):
            logging.info(
                "[rawtools_metrics] canceled before execution (result_id=%s)",
                result_id,
            )
            return
        logging.info(f"[rawtools_metrics] {cmd}")
        print(f"[rawtools_metrics] {cmd}")
        spec = rawtools_metrics_spec(
            raw=raw,
            output_dir=output_dir,
            arguments=arguments,
        )
        _run_cancelable_process(
            spec["args"],
            kind="rawtools_metrics",
            result_id=result_id,
            tracking_key="rawtools_metrics",
            cwd=spec["cwd"],
            stdout_path=spec["stdout"],
            stderr_path=spec["stderr"],
        )


@shared_task(bind=True, max_retries=None)
def rawtools_qc(self, input_dir, output_dir, rerun=False, result_id=None):
    if _is_canceled_result(result_id):
        logging.info("[rawtools_qc] canceled before start (result_id=%s)", result_id)
        return
    _defer_if_busy(
        task=self,
        kind="rawtools_qc",
        min_free_mem_gb=_safe_float("MIN_FREE_MEM_GB_RAWTOOLS", 2),
        max_load_per_cpu=_safe_float("MAX_LOAD_PER_CPU_RAWTOOLS", 0.95),
    )
    cmd = rawtools_qc_cmd(input_dir=input_dir, output_dir=output_dir, rerun=rerun)
    if cmd is not None:
        if _is_canceled_result(result_id):
            logging.info(
                "[rawtools_qc] canceled before execution (result_id=%s)", result_id
            )
            return
        logging.info(f"[rawtools_qc] {cmd}")
        print(f"[rawtools_qc] {cmd}")
        spec = rawtools_qc_spec(input_dir=input_dir, output_dir=output_dir)
        rc = _run_cancelable_process(
            spec["args"],
            kind="rawtools_qc",
            result_id=result_id,
            tracking_key="rawtools_qc",
            cwd=spec["cwd"],
            stdout_path=spec["stdout"],
            stderr_path=spec["stderr"],
        )
        if rc == 0:
            _warm_dashboard_caches(result_id)


@shared_task(bind=True, max_retries=None)
def run_maxquant(self, raw_file, params, rerun=False, result_id=None):
    if _is_canceled_result(result_id):
        logging.info("[run_maxquant] canceled before start (result_id=%s)", result_id)
        return
    _defer_if_busy(
        task=self,
        kind="run_maxquant",
        min_free_mem_gb=_safe_float("MIN_FREE_MEM_GB_MAXQUANT", 8),
        max_load_per_cpu=_safe_float("MAX_LOAD_PER_CPU_MAXQUANT", 0.85),
    )
    if _is_canceled_result(result_id):
        logging.info(
            "[run_maxquant] canceled before execution (result_id=%s)", result_id
        )
        return
    mq = MaxquantRunner(verbose=True, **params)
    logging.info(f"[run_maxquant] raw_file={raw_file} params={params} rerun={rerun}")
    print(f"[run_maxquant] raw_file={raw_file} params={params} rerun={rerun}")
    # Prepare run dirs/files via MaxquantRunner, but execute through the
    # cancel-aware shell runner so cancel requests can stop active MaxQuant jobs.
    cmd = mq.run(raw_file, rerun=rerun, run=False)
    if cmd is None:
        return
    rc = _run_cancelable_process(
        cmd,
        kind="run_maxquant",
        result_id=result_id,
        tracking_key="maxquant",
        shell=True,
        executable="/bin/bash",
    )
    if rc == 0:
        _warm_dashboard_caches(result_id)


@shared_task(bind=True, max_retries=None)
def run_pipeline_picked_group_fdr(self, pipeline_id, selected_result_ids=None, run_dir=None):
    _defer_if_busy(
        task=self,
        kind="run_pipeline_picked_group_fdr",
        min_free_mem_gb=_safe_float("MIN_FREE_MEM_GB_MAXQUANT", 8),
        max_load_per_cpu=_safe_float("MAX_LOAD_PER_CPU_MAXQUANT", 0.85),
    )
    Pipeline = apps.get_model("maxquant", "Pipeline")
    pipeline = Pipeline.objects.filter(pk=pipeline_id).select_related("project").first()
    if pipeline is None:
        logging.warning("[picked_group_fdr] Pipeline %s not found", pipeline_id)
        return

    _update_pipeline_picked_group_fdr_state(
        pipeline_id,
        picked_group_fdr_last_status="running",
        picked_group_fdr_last_started_at=timezone.now(),
        picked_group_fdr_last_finished_at=None,
        picked_group_fdr_last_error="",
    )
    logging.info(
        "[picked_group_fdr] task started pipeline_id=%s task_id=%s requested_results=%s run_dir=%s",
        pipeline_id,
        self.request.id,
        list(selected_result_ids or []),
        run_dir or "<auto>",
    )

    try:
        validation = validate_pipeline_for_picked_group_fdr(
            pipeline,
            result_ids=selected_result_ids,
        )
        if validation.get("status") != "ok":
            raise RuntimeError(validation.get("message") or "Pipeline validation failed.")

        collection = collect_pipeline_evidence_inputs(pipeline, result_ids=selected_result_ids)
        included_results = collection["included_results"]
        evidence_paths = [item["evidence_path"] for item in included_results]
        run_dir_path = pipeline.picked_group_fdr_root / (
            run_dir or timezone.now().strftime("%Y%m%d-%H%M%S")
        )
        run_dir_path.mkdir(parents=True, exist_ok=True)
        logging.info(
            "[picked_group_fdr] task inputs pipeline_id=%s included=%s excluded=%s output_dir=%s",
            pipeline_id,
            len(included_results),
            len(collection["excluded_results"]),
            run_dir_path,
        )

        manifest = {
            "status": "running",
            "pipeline_id": pipeline.pk,
            "pipeline_name": pipeline.name,
            "project": str(pipeline.project),
            "queued_task_id": self.request.id,
            "fasta_path": validation["fasta_path"],
            "mqpar_path": validation["mqpar_path"],
            "mqpar_settings": validation["mqpar_settings"],
            "included_results": included_results,
            "excluded_results": collection["excluded_results"],
            "requested_result_ids": list(selected_result_ids or []),
            "run_dir": str(run_dir_path),
        }
        (run_dir_path / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        result = run_picked_group_fdr(
            pipeline_identifier=f"{pipeline.project.slug}/{pipeline.slug}",
            selected_run_set=included_results,
            fasta_path=validation["fasta_path"],
            mqpar_path=validation["mqpar_path"],
            evidence_paths=evidence_paths,
            output_dir=run_dir_path,
        )

        manifest["artifacts"] = result.get("artifacts", {})
        manifest["log_excerpt"] = result.get("log_excerpt", {})
        manifest["status"] = "completed"
        per_result_quant = write_per_result_picked_group_fdr_quant_files(
            included_results,
            manifest["artifacts"]["protein_groups_filtered"],
        )
        manifest["artifacts"]["per_result_protein_groups"] = per_result_quant["written"]
        manifest["per_result_protein_groups_skipped"] = per_result_quant["skipped"]
        (run_dir_path / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _update_pipeline_picked_group_fdr_state(
            pipeline_id,
            picked_group_fdr_last_status="completed",
            picked_group_fdr_last_finished_at=timezone.now(),
            picked_group_fdr_last_error="",
            picked_group_fdr_last_manifest=json.dumps(manifest, sort_keys=True),
            picked_group_fdr_last_run_dir=run_dir_path.name,
        )
        logging.info(
            "[picked_group_fdr] task completed pipeline_id=%s task_id=%s output_dir=%s artifacts=%s per_result_written=%s per_result_skipped=%s",
            pipeline_id,
            self.request.id,
            run_dir_path,
            sorted(manifest.get("artifacts", {}).keys()),
            len(per_result_quant["written"]),
            len(per_result_quant["skipped"]),
        )
        excerpt = manifest.get("log_excerpt", {}).get("stdout", [])
        if excerpt:
            logging.info("[picked_group_fdr] Pipeline %s summary:", pipeline_id)
            for line in excerpt:
                logging.info("[picked_group_fdr] %s", line)
    except Exception as exc:
        logging.exception("[picked_group_fdr] Pipeline %s failed: %s", pipeline_id, exc)
        run_dir_name = run_dir or timezone.now().strftime("%Y%m%d-%H%M%S")
        run_dir_path = pipeline.picked_group_fdr_root / run_dir_name
        run_dir_path.mkdir(parents=True, exist_ok=True)
        error_path = run_dir_path / "picked_group_fdr.err"
        error_summary = (
            str(exc)
            if "Picked-group-FDR correction was not successful" in str(exc)
            else format_picked_group_fdr_failure(exc)
        )
        if error_path.exists():
            existing_error = error_path.read_text(encoding="utf-8", errors="ignore")
            if error_summary not in existing_error:
                with error_path.open("a", encoding="utf-8") as handle:
                    if existing_error and not existing_error.endswith("\n"):
                        handle.write("\n")
                    handle.write(f"\n{error_summary}\n")
        else:
            error_path.write_text(error_summary, encoding="utf-8")
        manifest_path = run_dir_path / "manifest.json"
        manifest = {}
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                manifest = {}
        manifest["status"] = "failed"
        manifest["error_summary"] = error_summary
        manifest["raw_error"] = str(exc)
        manifest_path.write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        _update_pipeline_picked_group_fdr_state(
            pipeline_id,
            picked_group_fdr_last_status="failed",
            picked_group_fdr_last_finished_at=timezone.now(),
            picked_group_fdr_last_error=error_summary,
            picked_group_fdr_last_manifest=json.dumps(manifest, sort_keys=True),
            picked_group_fdr_last_run_dir=run_dir_path.name,
        )
        raise
