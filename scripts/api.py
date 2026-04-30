from flask import Flask, jsonify, request
import logging
import psycopg2
import time

app = Flask(__name__) 


@app.route('/') 
def hello(): 
    return """

    Hello World!  <br/>
    <br/>
    Check the sources for instructions on how to use the endpoints!<br/>
    <br/>
    """





@app.route("/borrow_book", methods=['POST'])
def borrow_book():
    data = request.get_json()

    user_id = data["user_id"]
    isbn = data["ISBN"]

    conn = db_connection()
    cur = conn.cursor()

    try:
        conn.autocommit = False
        cur.execute("""
            SELECT num_copies
            FROM book
            WHERE isbn = %s
            FOR UPDATE;
        """, (isbn,))
        row = cur.fetchone()

        if row is None:
            return jsonify({"error": "Book not found"})

        total_copies = row[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM loan
            WHERE book_isbn = %s AND return_date IS NULL;
        """, (isbn,))
        borrowed = cur.fetchone()[0]

        if borrowed >= total_copies:
            return jsonify({"error": "No copies available"})

        cur.execute("""
            INSERT INTO loan (readers_users_user_id, book_isbn)
            VALUES (%s, %s);
        """, (user_id, isbn))

        conn.commit()

        return jsonify({"status": "ok"})

    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)})

    finally:
        conn.close()






@app.route("/return_book", methods=['POST'])
def return_book():
    data = request.get_json()

    if not data or "user_id" not in data or "ISBN" not in data:
        return jsonify({"status": 400, "errors": "Missing parameters"})

    user_id = data["user_id"]
    isbn = data["ISBN"]

    conn = db_connection()
    cur = conn.cursor()

    try:
        conn.autocommit = False

        cur.execute("""
            UPDATE loan
            SET return_date = CURRENT_TIMESTAMP
            WHERE readers_users_user_id = %s
              AND book_isbn = %s
              AND return_date IS NULL;
        """, (user_id, isbn))

        if cur.rowcount == 0:
            conn.rollback()
            return jsonify({"status": 400, "errors": "No active loan found"})

        conn.commit()

        return jsonify({"status": 200, "results": "Book returned"})

    except Exception as e:
        conn.rollback()
        return jsonify({"status": 500, "errors": str(e)})

    finally:
        cur.close()
        conn.close()

##########################################################
## DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user="postgres",
        password="Puntacana1",
        host="localhost",
        port="5432",
        database="sgd_library"
    )
    return db

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


    time.sleep(1) # just to let the DB start before this print :-)



    logger.info("\n---------------------------------------------------------------\n" + 
                  "API v1.0 online: http://localhost:8080/borrow_book\n\n")

    logger.info("\n---------------------------------------------------------------\n" + 
                  "API v1.0 online: http://localhost:8080/return_book\n\n")



    
    # NOTE: change to 5000 or remove the port parameter if you are running as a Docker container
    app.run(host="0.0.0.0", port=8080, debug=True, threaded=True)



