import os
from flask import Flask

app = Flask(__name__)
app.secret_key = 'Romerito-Senpai'
app.config['UPLOAD_FOLDER'] = os.path.join('static', 'uploads')