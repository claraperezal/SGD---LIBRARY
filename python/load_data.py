import pandas as pd
import psycopg2
import random
from datetime import datetime
import bcrypt
import os
from dotenv import load_dotenv


# --- PART 1: HASHING (For your DB Script) ---
def hash_password(plain_text_password):
    # Convert string to bytes
    password_bytes = plain_text_password.encode('utf-8')

    hashed_password = bcrypt.hashpw(password_bytes, bcrypt.gensalt())

    # Return as string to store in CSV easily
    return hashed_password.decode('utf-8')


def verify_password(plain_text_password, stored_hashed_password):
    # Convert both to bytes for comparison
    password_bytes = plain_text_password.encode('utf-8')
    hashed_bytes = stored_hashed_password.encode('utf-8')

    # bcrypt.checkpw handles the comparison logic safely
    return bcrypt.checkpw(password_bytes, hashed_bytes)


load_dotenv()
def db_connection():
    db = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME")
    )
    return db
def query(connection, statement, values=None):
    cur = connection.cursor()
    try:
        cur.execute(statement, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        connection.rollback()
    finally: 
        cur.close()
        

conn = db_connection()

with open('../sql/create_db.sql', 'r') as f:
    sql_script = f.read()
query(conn, sql_script, None)
print('DATABASE CREATED')

genres_types = pd.read_csv('csv/genre.csv')
for row in genres_types.values:
    query(conn, 'INSERT INTO genre (name) values(%s)', (row[1],))

books = pd.read_csv('csv/book.csv')
for row in books.values:
    query(
        conn,
        'INSERT INTO book (isbn, title, description, num_pages, num_copies, registration_date) '
        'VALUES (%s, %s, %s, %s, %s, %s)',
        (row[0], row[1], row[2], int(row[3]), int(row[4]), row[5])
    )

book_genre=pd.read_csv('csv/book_genre.csv')
for row in book_genre.values:
    query(conn, 'INSERT INTO book_genre (book_isbn, genre_genre_id) values(%s,%s)',(row[0], row[1]))

authors=pd.read_csv('csv/author.csv')
for row in authors.values:
    query(conn, 'INSERT INTO author (author_name) values(%s)',(row[1],))

book_author=pd.read_csv('csv/book_author.csv')
for row in book_author.values:
    query(conn,'INSERT INTO book_author (book_isbn, author_author_id) values(%s,%s)',(row[0], row[1]))

user=pd.read_csv('csv/users.csv')
for row in user.values:
    query(conn, 'INSERT INTO users (username, email, password) values(%s,%s,%s)',(row[1], row[2], hash_password(row[3])))

readers=pd.read_csv('csv/readers.csv')
admin= pd.read_csv('csv/admin.csv')
librarian= pd.read_csv('csv/librarian.csv')

for row in readers.values:
    query(conn, "INSERT INTO readers (users_user_id) values(%s)",(int(row[0]),))

for row in librarian.values:
    query(conn, 'INSERT INTO librarian(users_user_id) values(%s)  ',(int(row[0]), ))
for row in admin.values:
    query(conn, 'INSERT INTO administrator(users_user_id) values(%s)',(int(row[0]),))

loan = pd.read_csv('csv/loan.csv')
for row in loan.values:
    query(conn, 
        'INSERT INTO loan (loan_date, return_date, readers_users_user_id, book_isbn) values(%s,%s,%s,%s)',
        (row[1], row[2] if pd.notna(row[2]) and row[2] != '' else None, int(row[3]), row[4]))

reviews = pd.read_csv('csv/review.csv')
for row in reviews.values:
    query(conn,'INSERT INTO review ( rating, comment, review_date, readers_users_user_id, book_isbn) VALUES (%s,%s,%s,%s,%s)',( row[0], row[1], row[2], row[3], row[4] ))


print("DONE")