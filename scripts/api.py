## ===================================================
## LIBRARY MANAGEMENT SYSTEM
## CLARA PEREZ ALONSO
## GABRIEL
## ===================================================

from flask import Flask, jsonify, request
from dotenv import load_dotenv
import logging
import psycopg2
import bcrypt
import os
import time


# Load evironment variable
load_dotenv()

# Create the Flask application
app = Flask(__name__) 

##########################################################
## DATABASE CONECTION
##########################################################
def db_connection():
    db = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME")
    )
    return db

##########################################################
## ROOT ENDPOINT
##########################################################
@app.route('/')
def hello():
    """
    Root endpoint to check that the server is alive
    """
    return """
    <h1>Library Management System - SGD 2025/2026</h1>
    <p> Server active. Use Postman to access the endpoints</p>
    """
##########################################################
## MAIN
##########################################################
if __name__ == "__main__":

    # Set up the logging
    logging.basicConfig(filename="logs/log_file.log")
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s',
                              '%H:%M:%S')
                              # "%Y-%m-%d %H:%M:%S") # not using DATE to simplify
    ch.setFormatter(formatter)
    logger.addHandler(ch)


    time.sleep(1) # just to let the DB start before this print 


    logger.info("\n---------------------------------------------------------------\n" + 
                  "Library API online: http://localhost:8080/\n\n")

    app.run(host="0.0.0.0", port=8080, debug=True, threaded=True)



