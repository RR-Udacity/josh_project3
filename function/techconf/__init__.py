import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from icecream import ic


## Variables
POSTGRES_URL = os.environ["POSTGRES_URL"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PW = os.environ["POSTGRES_PW"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
SSLMODE = os.environ["SSLMODE"]
DB_URL = "postgresql://{user}:{pw}@{url}/{db}?sslmode=require".format(
    user=POSTGRES_USER, pw=POSTGRES_PW, url=POSTGRES_URL, db=POSTGRES_DB
)
CONN_STRING = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(
    POSTGRES_URL, POSTGRES_USER, POSTGRES_DB, POSTGRES_PW, SSLMODE
)
SERVICE_BUS_CONNECTION_STRING = os.environ["SERVICE_BUS_CONNECTION_STRING"]
SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]

ic.enable()  # icecream logging


def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode("utf-8"))
    ic(notification_id)
    logging.info(
        "Python ServiceBus queue trigger processed message: %s", notification_id
    )

    # TODO: Get connection to database
    conn = psycopg2.connect(CONN_STRING)  # postgres connection
    # engine = create_engine(DB_URL)

    try:
        # TODO: Get notification message and subject from database using the notification_id
        cur = conn.cursor()  # create a cursor
        cur.execute(
            "SELECT message, subject FROM notification WHERE id={};".format(
                notification_id
            )
        )
        result = cur.fetchall()
        message = result[0][0]
        subject = result[0][1]
        ic(message)
        ic(subject)
        cur.close()

        # TODO: Get attendees email and name
        cur = conn.cursor()
        cur.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cur.fetchall()
        cur.close()
        ic(attendees)

        # TODO: Loop through each attendee and send an email with a personalized subject
        logging.info("Sending emails to {} attendees.".format(len(attendees)))
        success = 0
        for attendee in attendees:
            print(
                "Sending email to {} {} at {}".format(attendee[0], attendee[1], attendee[2])
            )
            message = Mail(
                from_email="Clark.Kent@JoshHaines.com",
                to_emails=attendee[2],
                subject="{} - An Update from TechConf".format(attendee[0]),
                html_content="TechConf Stuff & Things",
            )
            try:
                sg = SendGridAPIClient(SENDGRID_API_KEY)
                response = sg.send(message)
                ic(response.status_code)
                if response.status_code == 202:
                    success += 1
            except Exception as e:
                ic(e.message)

        print("{} of {} emails sent successfully!".format(success, len(attendees)))

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cur = conn.cursor()
        cur.execute("UPDATE notification SET status ")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        # conn.commit()
        conn.close()
        logging.info("Database connection closed.")
