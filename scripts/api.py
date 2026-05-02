## ===================================================
## LIBRARY MANAGEMENT SYSTEM
## CLARA PEREZ ALONSO
## GABRIEL
## ===================================================

from flask import Flask, jsonify, request
from dotenv import load_dotenv
import logging
import psycopg2
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
    <p> Server active</p>
    """

##########################################################
## ENDPOINT 1: Register User (POST)
## Any person can register as a reader or librarian
## Administrators are inserted directly into the BD 
##########################################################

@app.route('/sgdproj/user', methods=['POST'])
def register_user():
    logger.info('### POST /sgdproj/user ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'email', 'password', 'role']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    email = payload['email']
    password = payload['password']
    role = payload['role'].lower()

    if role not in ['reader', 'librarian']:
        return jsonify({'status': 400,
                        'errors':f'Role must be reader or librarian. Admins are inserted directly in the database'})

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                INSERT INTO users (username, email, password)
                VALUES (%s,%s, %s)
                RETURNING user_id           
            """,
            (username, email, password
             )
        )
        new_user_id = cur.fetchone()[0]

        if role == 'reader':
            cur.execute ('INSERT INTO readers (users_user_id) VALUES (%s)',
                         (new_user_id,))
        elif role == 'librarian':
            cur.execute ('INSERT INTO librarian (users_user_id) VALUES (%s)',
                         (new_user_id,))


        conn.commit()
        logger.info(f'User {username} registered with id {new_user_id} as {role}')
        return jsonify({
            'status':200,
            'results':new_user_id
        })      
    except psycopg2.errors.UniqueViolation as e:
        conn.rollback()
        logger.warning(f' Duplicate username or email:{e}')
        return jsonify({'status': 400,
                        'errors':f'Username or email already exists'
        })
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        logger.error(f'Error registering user: {e}')
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn is not None:
            conn.close()
            




##########################################################
## ENDPOINT 2: 
##########################################################
@app.route('/sgdproj/book', methods=['POST'])
def add_book():
    logger.info('### POST /sgdproj/book ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password', 'ISBN', 'book_name', 'author', 'book_description', 'pages', 'copies', 'genres']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']
    isbn = payload['ISBN']
    book_name = payload['book_name']
    author = payload['author']
    book_description = payload['book_description']
    pages = payload['pages']
    copies = payload['copies']
    genres = payload['genres']

    if copies < 1:
        return jsonify({'status': 400,
                        'errors':f'At least one copy required'})

    if not genres or len(genres)==0:
        return jsonify({'status': 400,
                        'errors':f'At least one genre required'})

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))

        if not cur.fetchone():
            return jsonify({'status': 400,
                        'errors':f'User is not admin'})

        cur.execute(
            """
                INSERT INTO book (isbn, title, author, description, num_pages, num_copies, administrator_users_user_id)             
                VALUES (%s,%s,%s,%s,%s,%s,%s)      
            """,
            (isbn, book_name, author, book_description, pages, copies,user_id,))

       
        for g in genres: 
            cur.execute('SELECT genre_id FROM genre WHERE name = %s', (g,))
            genre_row = cur.fetchone()

            if not genre_row:
                return jsonify({'status': 400,
                        'errors':f'Genre noot found: {g}'})
            genre_id = genre_row[0]

            cur.execute("""
                        INSERT INTO book_genre (book_isbn, genre_genre_id)
                        VALUES (%s, %s)
                        """, (isbn, genre_id))
            
        conn.commit()
        return jsonify({
            'status':200,
            'results':isbn
        })      
    except psycopg2.errors.UniqueViolation :
        conn.rollback()
        return jsonify({'status': 400,
                        'errors':f'Book with this ISBN already exits'
        })
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn is not None:
            conn.close()





##########################################################
## ENDPOINT 3: 
##########################################################
@app.route('/sgdproj/update_copies', methods=['PUT'])
def update_copies():
    logger.info('### POST /sgdproj/update_copies ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password', 'ISBN', 'copies']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']
    isbn = payload['ISBN']
    copies = payload['copies']

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))

        if not cur.fetchone():
            return jsonify({'status': 400,
                        'errors':f'User is not admin'})


        cur.execute(
            """
                SELECT num_copies FROM book WHERE isbn = %s
            """,
            (isbn,))
        book = cur.fetchone()

        if not book:
            return jsonify({'status': 400,
                        'errors':f'Book does not exist'})
       
        cur.execute(
            """
                SELECT COUNT(*)
                FROM loan
                WHERE book_isbn = %s AND return_date IS NULL
            """,
            (isbn,))
    
        borrowed = cur.fetchone()[0]

        new_copies = copies
        if new_copies < borrowed:
            return jsonify({'status': 400,
                        'errors':f'Cannot set copies below borrowed ({borrowed})'})


        cur.execute("""
                    UPDATE book 
                    SET num_copies = %s
                    WHERE isbn = %s
                    """, (new_copies, isbn))
            
        conn.commit()
        return jsonify({
            'status':200,
            'results':isbn
        })      
    
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn:
            conn.close()






##########################################################
## ENDPOINT 4: 
##########################################################
@app.route('/sgdproj/list_book_by_genre', methods=['GET'])
def list_book_by_genre():
    logger.info('### POST /sgdproj/list_book_by_genre ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password', 'genre_id']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']
    genre_id = payload['genre_id']

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_admin = cur.fetchone()
        
        cur.execute(
            """
                SELECT *
                FROM librarian
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_librarian = cur.fetchone()

        if not is_admin and not is_librarian:
            return jsonify({'status': 400,
                        'errors':f'User not authorized'})


        cur.execute(
            """
                SELECT b.isbn, b.title, b.author, b.num_pages, b.num_copies,
                    COALESCE(b.num_copies - COUNT(l.loan_id), b.num_copies) AS available_copies
                FROM book b JOIN book_genre bg ON b.isbn = bg.book_isbn
                            LEFT JOIN loan l ON b.isbn=l.book_isbn AND l.return_date IS NULL
                WHERE bg.genre_genre_id = %s
                GROUP BY b.isbn 
            """,
            (genre_id,))
        books = cur.fetchall()

        result = []
        for book in books:
            isbn, title, author, pages, copies, available = book
            cur.execute("""
                        SELECT g.name 
                        FROM genre g
                        JOIN book_genre bg ON g.genre_id = bg.genre_genre_id
                        WHERE bg.book_isbn = %s
                        """, (isbn,))
            genres = [g[0] for g in cur.fetchall()]

            result.append({
                'ISBN':isbn,
                'book_name': title,
                'author':author,
                'pages':pages,
                'copies':copies,
                'available_copies': available,
                'genres': genres

            })
      

        return jsonify({
            'status':200,
            'results':result
        })      
    
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn:
            conn.close()




##########################################################
## ENDPOINT 5: 
##########################################################
@app.route('/sgdproj/find_book_by_name', methods=['GET'])
def find_book_by_name():
    logger.info('### POST /sgdproj/find_book_by_name ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password', 'book_name']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']
    book_name = payload['book_name']

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_admin = cur.fetchone()
        
        cur.execute(
            """
                SELECT *
                FROM librarian
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_librarian = cur.fetchone()

        if not is_admin and not is_librarian:
            return jsonify({'status': 400,
                        'errors':f'User not authorized'})


        cur.execute(
            """
                SELECT b.isbn, b.title, b.author, b.num_pages, b.num_copies,
                    COALESCE(b.num_copies - COUNT(l.loan_id), b.num_copies) AS available_copies
                FROM book b JOIN book_genre bg ON b.isbn = bg.book_isbn
                            LEFT JOIN loan l ON b.isbn=l.book_isbn AND l.return_date IS NULL
                WHERE LOWER(b.title) LIKE LOWER(%s) 
                GROUP BY b.isbn 
            """,
            ('%' + book_name +'%',))
        books = cur.fetchall()

        result = []
        for book in books:
            isbn, title, author, pages, copies, available = book
            cur.execute("""
                        SELECT g.name 
                        FROM genre g
                        JOIN book_genre bg ON g.genre_id = bg.genre_genre_id
                        WHERE bg.book_isbn = %s
                        """, (isbn,))
            genres = [g[0] for g in cur.fetchall()]

            result.append({
                'ISBN':isbn,
                'book_name': title,
                'author':author,
                'pages':pages,
                'copies':copies,
                'available_copies': available,
                'genres': genres

            })
      

        return jsonify({
            'status':200,
            'results':result
        })      
    
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn:
            conn.close()



##########################################################
## ENDPOINT 6: 
##########################################################
@app.route('/sgdproj/find_book_by_isbn', methods=['GET'])
def find_book_by_isbn():
    logger.info('### POST /sgdproj/find_book_by_isbn ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password', 'ISBN']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']
    isbn = payload['ISBN']

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_admin = cur.fetchone()
        
        cur.execute(
            """
                SELECT *
                FROM librarian
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_librarian = cur.fetchone()

        if not is_admin and not is_librarian:
            return jsonify({'status': 400,
                        'errors':f'User not authorized'})


        cur.execute(
            """
                SELECT b.isbn, b.title, b.author, b.num_pages, b.num_copies,
                    COALESCE(b.num_copies - COUNT(l.loan_id), b.num_copies) AS available_copies
                FROM book b JOIN book_genre bg ON b.isbn = bg.book_isbn
                            LEFT JOIN loan l ON b.isbn=l.book_isbn AND l.return_date IS NULL
                WHERE b.isbn = %s
                GROUP BY b.isbn 
            """,
            (isbn,))
        books = cur.fetchall()

        result = []
        for book in books:
            isbn, title, author, pages, copies, available = book
            cur.execute("""
                        SELECT g.name 
                        FROM genre g
                        JOIN book_genre bg ON g.genre_id = bg.genre_genre_id
                        WHERE bg.book_isbn = %s
                        """, (isbn,))
            genres = [g[0] for g in cur.fetchall()]

            result.append({
                'ISBN':isbn,
                'book_name': title,
                'author':author,
                'pages':pages,
                'copies':copies,
                'available_copies': available,
                'genres': genres

            })
      

        return jsonify({
            'status':200,
            'results':result
        })      
    
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn:
            conn.close()




# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
##########################################################
## ENDPOINT 7: 
##########################################################
@app.route('/sgdproj/loaned_books', methods=['GET'])
def loaned_books():
    logger.info('### POST /sgdproj/loaned_books ###')
    payload = request.get_json()
    logger.debug(f'payload: {payload}')

    required_fields = ['username', 'password']
    for field in required_fields:
        if field not in payload:
            return jsonify({'status': 400,
                            'errors':f'missing required field: {field}'})
    username = payload['username']
    password = payload['password']

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
                SELECT user_id, password
                FROM users
                WHERE username = %s        
            """,
            (username,))
        user = cur.fetchone()

        if not user:
            return jsonify({'status': 400,
                        'errors':f'User does not exist'})

        user_id, db_password = user

        if password != db_password:
            return jsonify({'status': 400,
                        'errors':f'Invalid password'})


        cur.execute(
            """
                SELECT *
                FROM administrator
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_admin = cur.fetchone()
        
        cur.execute(
            """
                SELECT *
                FROM librarian
                WHERE users_user_id = %s        
            """,
            (user_id,))
        is_librarian = cur.fetchone()

        if not is_admin and not is_librarian:
            return jsonify({'status': 400,
                        'errors':f'User not authorized'})


        cur.execute(
            """
                SELECT b.isbn, b.title, u.user_id, u.username, l.loan_date
                FROM loan l JOIN book b ON b.isbn=l.book_isbn 
                            JOIN users u ON l.readers_users_user_id = u.user_id
                WHERE l.return_date IS NULL
                ORDER BY l.loan_date DESC;
            """)
        rows = cur.fetchall()

        result = []
        for row in rows:
            isbn, title, loaner_id, loaner_name, loan_date = row

            result.append({
                'ISBN':isbn,
                'book_name': title,
                'loaner_id':loaner_id,
                'loaner_name':loaner_name,
                'loan_date':loan_date
            })
      

        return jsonify({
            'status':200,
            'results':result
        })      
    
    
    except (Exception,psycopg2.DatabaseError) as e:
        conn.rollback()
        return jsonify({'status': 500,
                        'errors':str(e)
        })
    
    finally:
        if conn:
            conn.close()




























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



