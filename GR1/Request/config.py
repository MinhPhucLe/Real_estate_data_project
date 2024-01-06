from app import app
from flaskext.mysql import MySQL

mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'admin'
app.config['MYSQL_DATABASE_PASSWORD'] = 'admin'
app.config['MYSQL_DATABASE_DB'] = 'real_estate_request'
app.config['MYSQL_DATABASE_HOST'] = '172.24.144.1'
mysql.init_app(app)