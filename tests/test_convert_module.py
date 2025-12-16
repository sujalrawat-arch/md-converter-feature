import os

from pdf_extractor import convert


def test_convert_txt_to_pdf(tmp_path):
    src = tmp_path / "sample.txt"
    src.write_text("Hello world\nThis is a test document.")
    out_dir = tmp_path / "out"
    pdf_path = convert.convert_to_pdf(str(src), str(out_dir))
    assert pdf_path.endswith(".pdf")
    assert os.path.exists(pdf_path)


def test_convert_dispatch_docx(monkeypatch, tmp_path):
    src = tmp_path / "doc.docx"
    src.write_text("dummy")
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    called = {}

    def fake_docx(input_path, output_dir):
        called["args"] = (input_path, output_dir)
        os.makedirs(output_dir, exist_ok=True)
        target = os.path.join(output_dir, "doc.pdf")
        with open(target, "w", encoding="utf-8") as fh:
            fh.write("pdf")
        return target

    monkeypatch.setattr(convert, "convert_docx_to_pdf", fake_docx)
    pdf_path = convert.convert_to_pdf(str(src), str(out_dir))
    assert pdf_path.endswith("doc.pdf")
    assert called["args"] == (str(src), str(out_dir))