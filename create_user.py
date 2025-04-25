from app import create_app
from models.database import db
from models.user import User
from werkzeug.security import generate_password_hash

app = create_app()

with app.app_context():
    username = "bgoka21"      # Replace with your desired username
    password = "Rocketsare#1"      # Replace with your desired password
    password_hash = generate_password_hash(password)

    if not User.query.filter_by(username=username).first():
        new_user = User(username=username, password_hash=password_hash, is_admin=True)
        db.session.add(new_user)
        db.session.commit()
        print(f"User '{username}' created successfully.")
    else:
        print("User already exists.")
