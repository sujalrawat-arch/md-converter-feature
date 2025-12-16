import logging
import os

from pdf_extractor import context, pipeline


def test_run_pipeline_invokes_steps_in_sequence(monkeypatch, tmp_path):
    output_root = tmp_path / "exec" / "textract_output"
    data_dir = tmp_path / "exec" / "data"
    monkeypatch.setattr(context, "OUTPUT_ROOT", str(output_root))
    monkeypatch.setattr(context, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(context, "MAX_PAGES", 5)

    logger = logging.getLogger("pipeline-test")
    logger.propagate = False
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    monkeypatch.setattr(pipeline, "setup_logger", lambda ctx: logger)
    monkeypatch.setattr(pipeline, "USE_VISION", True)

    calls = []

    def make_stub(name, status=None):
        def _stub(ctx, log):
            calls.append(name)
            ctx.save_status(status or name)
        return _stub

    monkeypatch.setattr(pipeline, "step_00_download", make_stub("download", "download"))
    monkeypatch.setattr(pipeline, "step_00_convert_to_pdf", make_stub("convert", "convert_pdf"))
    monkeypatch.setattr(pipeline, "step_01_classify", make_stub("classify", "classify"))
    monkeypatch.setattr(pipeline, "step_02_rotation", make_stub("rotation", "rotation"))
    monkeypatch.setattr(pipeline, "step_03_upload_norm_for_textract", make_stub("upload_textract", "upload_textract"))

    def stub_step_04(ctx, log, vision_callback=None):
        calls.append("textract")
        if vision_callback:
            vision_callback()
        ctx.save_status("textract")

    monkeypatch.setattr(pipeline, "step_04_textract", stub_step_04)
    monkeypatch.setattr(pipeline, "step_05_render_for_vision", make_stub("vision_render", "vision_rendered"))
    monkeypatch.setattr(pipeline, "step_06_vision_async", make_stub("vision_async", "vision"))
    monkeypatch.setattr(pipeline, "step_07_unify", make_stub("unify"))
    monkeypatch.setattr(pipeline, "step_08_upload_and_cleanup", make_stub("upload_cleanup", "done"))

    pipeline.run_pipeline("job-123", "s3://bucket/file.pdf")

    assert calls == [
        "download",
        "convert",
        "classify",
        "rotation",
        "upload_textract",
        "textract",
        "vision_render",
        "vision_async",
        "unify",
        "upload_cleanup",
    ]

    status_file = os.path.join(output_root, "job-123", "status.json")
    assert os.path.exists(status_file)


def test_run_pipeline_skips_vision_when_disabled(monkeypatch, tmp_path):
    output_root = tmp_path / "exec" / "textract_output"
    data_dir = tmp_path / "exec" / "data"
    monkeypatch.setattr(context, "OUTPUT_ROOT", str(output_root))
    monkeypatch.setattr(context, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(context, "MAX_PAGES", 5)

    logger = logging.getLogger("pipeline-test-skip-vision")
    logger.propagate = False
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    monkeypatch.setattr(pipeline, "setup_logger", lambda ctx: logger)
    monkeypatch.setattr(pipeline, "USE_VISION", False)

    calls = []

    def make_stub(name, status=None):
        def _stub(ctx, log):
            calls.append(name)
            ctx.save_status(status or name)
        return _stub

    monkeypatch.setattr(pipeline, "step_00_download", make_stub("download", "download"))
    monkeypatch.setattr(pipeline, "step_00_convert_to_pdf", make_stub("convert", "convert_pdf"))
    monkeypatch.setattr(pipeline, "step_01_classify", make_stub("classify", "classify"))
    monkeypatch.setattr(pipeline, "step_02_rotation", make_stub("rotation", "rotation"))
    monkeypatch.setattr(pipeline, "step_03_upload_norm_for_textract", make_stub("upload_textract", "upload_textract"))

    def stub_step_04(ctx, log, vision_callback=None):
        calls.append("textract")
        assert vision_callback is None
        ctx.save_status("textract")

    def fail_if_called(_ctx, _log):  # pragma: no cover - guard
        raise AssertionError("Vision steps should be skipped when USE_VISION is False")

    monkeypatch.setattr(pipeline, "step_04_textract", stub_step_04)
    monkeypatch.setattr(pipeline, "step_05_render_for_vision", fail_if_called)
    monkeypatch.setattr(pipeline, "step_06_vision_async", fail_if_called)
    monkeypatch.setattr(pipeline, "step_07_unify", make_stub("unify"))
    monkeypatch.setattr(pipeline, "step_08_upload_and_cleanup", make_stub("upload_cleanup", "done"))

    pipeline.run_pipeline("job-456", "s3://bucket/file.pdf")

    assert calls == [
        "download",
        "convert",
        "classify",
        "rotation",
        "upload_textract",
        "textract",
        "unify",
        "upload_cleanup",
    ]

    status_file = os.path.join(output_root, "job-456", "status.json")
    assert os.path.exists(status_file)
