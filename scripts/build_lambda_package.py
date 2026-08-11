from __future__ import annotations

import argparse
import shutil
import tempfile
import zipfile
from pathlib import Path


def build(root: Path, output: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="pulsestream-lambda-") as temporary:
        stage = Path(temporary)
        shutil.copytree(root / "src/pulsestream", stage / "pulsestream")
        shutil.copytree(root / "lambdas", stage / "lambdas")
        output.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(value for value in stage.rglob("*") if value.is_file()):
                archive.write(path, path.relative_to(stage))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="dist/pulsestream-lambdas.zip")
    arguments = parser.parse_args()
    repository = Path(__file__).resolve().parents[1]
    build(repository, repository / arguments.output)
