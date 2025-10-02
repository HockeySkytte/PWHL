from flask_app import app as application

# Expose as `app` for gunicorn default command `gunicorn app:app`
app = application
