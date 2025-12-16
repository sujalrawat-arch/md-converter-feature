import os

from pdf_extractor import context


def test_jobctx_build_and_status_roundtrip(monkeypatch, tmp_path):
    output_root = tmp_path / "output"
    data_dir = tmp_path / "data"
    monkeypatch.setattr(context, "OUTPUT_ROOT", str(output_root))
    monkeypatch.setattr(context, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(context, "MAX_PAGES", 3)

    ctx = context.JobCtx.build("file-1", "s3://bucket/doc.pdf")
    assert os.path.isdir(ctx.job_dir)
    assert ctx.local_pdf.endswith("file-1.pdf")
    ctx.source_path = os.path.join(str(data_dir), "file-1.docx")
    ctx.local_pdf = os.path.join(str(data_dir), "file-1.pdf")

    ctx.page_count = 10
    ctx.image_pages = [0, 1, 2, 3]
    ctx.text_pages = [0, 2, 3]
    ctx.chart_pages = [1]
    ctx.save_status("classify", {"extra": True})

    ctx_loaded = context.JobCtx.build("file-1", "dummy")
    ctx_loaded.load_status()
    assert ctx_loaded.last_step == "classify"
    assert ctx_loaded.page_count == 3  # capped by MAX_PAGES
    assert ctx_loaded.image_pages == [0, 1, 2]
    assert ctx_loaded.text_pages == [0, 2]  # trimmed to MAX_PAGES
    assert ctx_loaded.chart_pages == [1]
    assert ctx_loaded.source_path.endswith("file-1.docx")
    assert ctx_loaded.local_pdf.endswith("file-1.pdf")

    status_path = os.path.join(ctx.job_dir, "status.json")
    assert os.path.exists(status_path)
