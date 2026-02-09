from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
import os

load_dotenv()
USER_DG =os.getenv('oracle_dataguard_user')
PASSWORD_DG =os.getenv('oracle_dataguard_password')
HOST_DG =os.getenv('oracle_dataguard_host')
PORT_DG =os.getenv('oracle_dataguard_port')
SERVICES_NAME_DG = os.getenv('oracle_dataguard_db')

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = f"oracle+cx_oracle://{USER_DG}:{PASSWORD_DG}@{HOST_DG}:{PORT_DG}/?service_name={SERVICES_NAME_DG}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    from .routes import main
    app.register_blueprint(main)
    return app


