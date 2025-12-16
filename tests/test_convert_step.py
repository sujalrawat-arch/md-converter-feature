import os

from pdf_extractor import context
from pdf_extractor.steps.convert_pdf import step_00_convert_to_pdf


def test_step_convert_creates_pdf(monkeypatch, tmp_path):
    output_root = tmp_path / "output"
    data_dir = tmp_path / "data"
    monkeypatch.setattr(context, "OUTPUT_ROOT", str(output_root))
    monkeypatch.setattr(context, "DATA_DIR", str(data_dir))

    class DummyLog:
        def info(self, *_, **__):
            return None

    log = DummyLog()

    ctx = context.JobCtx.build("file-x", "s3://bucket/doc.txt")
    source = data_dir / "file-x.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("Hello world from text file")
    ctx.source_path = str(source)
    ctx.last_step = "download"

    step_00_convert_to_pdf(ctx, log)

    assert ctx.last_step == "convert_pdf"
    assert ctx.local_pdf.endswith(".pdf")
    assert os.path.exists(ctx.local_pdf)