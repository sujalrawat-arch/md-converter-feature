from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    aws_region: str
    bucket_src: str | None
    bucket_tex: str | None
    bucket_out: str | None
    platform_home: str
    exec_home: str
    output_root: str
    data_dir: str
    openai_api_key: str
    aws_key: str | None
    aws_secret: str | None
    max_pages: int
    use_vision: bool
    keep_vision_images: bool


def _bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _bool_env_multi(names: list[str], default: bool = True) -> bool:
    """Return the first defined boolean-like env value from the provided list."""
    for n in names:
        raw = os.getenv(n)
        if raw is not None:
            return raw.strip().lower() not in {"0", "false", "no", "off"}
    return default


def _default_platform_home() -> str:
    return os.getenv("PLATFORM_HOME", os.getcwd())


def load_settings() -> Settings:
    platform_home = _default_platform_home()
    exec_home = os.getenv("PDF_EXTRACTOR_HOME", os.path.join(platform_home, "exec"))
    output_root = os.path.join(exec_home, "textract_output")
    data_dir = os.path.join(exec_home, "data")
    for path in (exec_home, output_root, data_dir):
        os.makedirs(path, exist_ok=True)
    return Settings(
        aws_region=os.getenv("AWS_REGION", "ap-south-1"),
        bucket_src=os.getenv("AWS_BUCKET_PDF_READER_SOURCE"),
        bucket_tex=os.getenv("AWS_BUCKET_PDF_READER_TEXTRACT"),
        bucket_out=os.getenv("AWS_BUCKET_PDF_READER_OUTPUT"),
        platform_home=platform_home,
        exec_home=exec_home,
        output_root=output_root,
        data_dir=data_dir,
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        aws_key=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        max_pages=int(os.getenv("PDF_EXTRACTOR_MAX_PAGES", "200")),
        # Honor either env var; PDF_EXTRACTOR_USE_VISION takes precedence if set.
        use_vision=_bool_env_multi(["PDF_EXTRACTOR_USE_VISION", "USE_VISION"], True),
        keep_vision_images=_bool_env("PDF_EXTRACTOR_KEEP_VISION_IMAGES", False),
    )


SETTINGS = load_settings()

AWS_REGION = SETTINGS.aws_region
BUCKET_SRC = SETTINGS.bucket_src
BUCKET_TEX = SETTINGS.bucket_tex
BUCKET_OUT = SETTINGS.bucket_out
PLATFORM_HOME = SETTINGS.platform_home
EXEC_HOME = SETTINGS.exec_home
OUTPUT_ROOT = SETTINGS.output_root
DATA_DIR = SETTINGS.data_dir
OPENAI_API_KEY = SETTINGS.openai_api_key
AWS_KEY = SETTINGS.aws_key
AWS_SEC = SETTINGS.aws_secret
MAX_PAGES = SETTINGS.max_pages
USE_VISION = SETTINGS.use_vision
KEEP_VISION_IMAGES = SETTINGS.keep_vision_images
