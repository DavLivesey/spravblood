from flask import Flask, render_template
from psql_commans import DBCommands
from routes import main

db = DBCommands()


app = Flask(__name__)
app.register_blueprint(main, url_prefix='/')




if __name__ == "__main__":
    app.run(debug=True)