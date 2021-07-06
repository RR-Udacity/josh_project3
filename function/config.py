DEBUG = True
POSTGRES_URL=os.environ["POSTGRES_URL"]
POSTGRES_USER="psql_admin"
POSTGRES_PW=os.environ["POSTGRES_PW"]
POSTGRES_DB=os.environ["POSTGRES_DB"]
DB_URL = 'postgresql://{user}:{pw}@{url}/{db}'.format(user=POSTGRES_USER,pw=POSTGRES_PW,url=POSTGRES_URL,db=POSTGRES_DB)
SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI') or DB_URL
CONFERENCE_ID = 1
SECRET_KEY = 'LWd2tzlprdGHCIPHTd4tp5SBFgDszm'
SERVICE_BUS_CONNECTION_STRING =os.environ['SERVICE_BUS_CONNECTION_STRING']
SERVICE_BUS_QUEUE_NAME ='joshnotificationqueue'
ADMIN_EMAIL_ADDRESS: 'Clark.Kent@joshhaines.com'
SENDGRID_API_KEY = os.environ['SENDGRID_API_KEY']
