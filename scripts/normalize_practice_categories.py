import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app, db
from app.utils.category_normalization import normalize_category
from models.database import Practice
from models.uploaded_file import UploadedFile


def run():
    app = create_app()
    with app.app_context():
        practice_rows = Practice.query.all()
        uploaded_files = UploadedFile.query.all()

        practice_changes = 0
        upload_changes = 0

        for practice in practice_rows:
            canonical = normalize_category(practice.category)
            if canonical != practice.category:
                practice.category = canonical
                practice_changes += 1

        for uploaded_file in uploaded_files:
            canonical = normalize_category(uploaded_file.category)
            if canonical != uploaded_file.category:
                uploaded_file.category = canonical
                upload_changes += 1

        if practice_changes or upload_changes:
            db.session.commit()

        print(
            f"Normalized {practice_changes} Practice rows and {upload_changes} UploadedFile rows."
        )


if __name__ == "__main__":
    run()
