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
    # NOTE: change the host to "db" if you are running as a Docker container
    db = psycopg2.connect(user = os.getenv("DB_USER"),
                            password = os.getenv("DB_PASSWORD"),
                            host = os.getenv("DB_HOST"), #"db",
                            port = os.getenv("DB_PORT"),
                            database = os.getenv("DB_NAME"))
    return db


def query(connection, statement, values=None):
    cur = connection.cursor()
    try:
        cur.execute(statement, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        connection.rollback()
        cur.close()


conn = db_connection()

drop_tables="""
DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS loan CASCADE;
DROP TABLE IF EXISTS book_author CASCADE;
DROP TABLE IF EXISTS author CASCADE;
DROP TABLE IF EXISTS book_genre CASCADE;
DROP TABLE IF EXISTS book CASCADE;
DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS readers CASCADE;
DROP TABLE IF EXISTS librarian CASCADE;
DROP TABLE IF EXISTS administrator CASCADE;
DROP TABLE IF EXISTS users CASCADE;
"""
query(conn, drop_tables, None)
print("DROPPED")
create_tables = """
CREATE TABLE users (
	user_id	 SERIAL,
	username VARCHAR(50) NOT NULL,
	email	 VARCHAR(100) NOT NULL,
	password VARCHAR(255) NOT NULL,
	PRIMARY KEY(user_id),
	UNIQUE(username),
	UNIQUE(email)
);

CREATE TABLE administrator (
	users_user_id INTEGER NOT NULL,
	PRIMARY KEY(users_user_id),
	CONSTRAINT fk_administrator_user
	FOREIGN KEY (users_user_id)
	REFERENCES users(user_id)
	ON DELETE CASCADE
);

CREATE TABLE librarian (
	users_user_id INTEGER NOT NULL,
	PRIMARY KEY(users_user_id),
	CONSTRAINT fk_librarian_user
	FOREIGN KEY (users_user_id)
	REFERENCES users(user_id)
	ON DELETE CASCADE
);

CREATE TABLE readers (
	users_user_id INTEGER NOT NULL,
	PRIMARY KEY(users_user_id),
	CONSTRAINT fk_readers_user
	FOREIGN KEY (users_user_id)
	REFERENCES users(user_id)
	ON DELETE CASCADE
);


CREATE TABLE genre (
	genre_id SERIAL,
	name	 VARCHAR(100) NOT NULL UNIQUE,
	PRIMARY KEY(genre_id)
);


CREATE TABLE book (
	isbn			 VARCHAR(50),
	title			 VARCHAR(255) NOT NULL,
	description		 TEXT,
	num_pages	     INTEGER NOT NULL,
	num_copies		 INTEGER NOT NULL,
	registration_date		 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	administrator_users_user_id INTEGER,
	PRIMARY KEY(isbn),

	CONSTRAINT fk_book_administrator
		FOREIGN KEY (administrator_users_user_id)
		REFERENCES administrator(users_user_id)
		ON DELETE SET NULL,

	CONSTRAINT chk_num_pages_positive
		CHECK (num_pages > 0),

	CONSTRAINT chk_num_copies_non_negative
		CHECK (num_copies >= 0)
);


CREATE TABLE book_genre (
	book_isbn	 VARCHAR(50) NOT NULL,
	genre_genre_id INTEGER NOT NULL,
	PRIMARY KEY(book_isbn, genre_genre_id),

	CONSTRAINT fk_book_genre_book
		FOREIGN KEY(book_isbn)
		REFERENCES book(isbn),

	CONSTRAINT fk_book_genre_genre
		FOREIGN KEY(genre_genre_id)
		REFERENCES genre(genre_id)
);

CREATE TABLE author(
	author_id SERIAL,
	author_name VARCHAR(255) NOT NULL UNIQUE, 
	PRIMARY KEY(author_id)
);

CREATE TABLE book_author(
	book_isbn VARCHAR(50) NOT NULL,
	author_author_id INTEGER NOT NULL,

	PRIMARY KEY(book_isbn, author_author_id),

	CONSTRAINT fk_book_author_book
		FOREIGN KEY(book_isbn)
		REFERENCES book(isbn),

	CONSTRAINT fk_book_author_author
		FOREIGN KEY(author_author_id)
		REFERENCES author(author_id)
);

CREATE TABLE loan (
	loan_id		     SERIAL,
	loan_date		 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	return_date		 TIMESTAMP,
	readers_users_user_id INTEGER NOT NULL,
	book_isbn		 VARCHAR(50) NOT NULL,
	PRIMARY KEY(loan_id),

	CONSTRAINT fk_loan_reader
		FOREIGN KEY(readers_users_user_id)
		REFERENCES readers(users_user_id),

	CONSTRAINT fk_loan_book
		FOREIGN KEY(book_isbn)
		REFERENCES book(isbn),

	CONSTRAINT chk_return_after_loan
		CHECK (return_date IS NULL OR return_date >= loan_date)
);

CREATE TABLE review(
	review_id		 BIGSERIAL,
	rating		 INTEGER NOT NULL,
	comment		 TEXT,
	review_date		 TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	readers_users_user_id INTEGER NOT NULL,
	book_isbn		 VARCHAR(50) NOT NULL,
	PRIMARY KEY(review_id),

	CONSTRAINT fk_review_reader
		FOREIGN KEY (readers_users_user_id)
		REFERENCES readers(users_user_id),

	CONSTRAINT fk_review_book
		FOREIGN KEY (book_isbn)
		REFERENCES book(isbn),

	CONSTRAINT uq_review_reader_book
		UNIQUE (readers_users_user_id, book_isbn),

	CONSTRAINT chk_rating_range
		CHECK (rating BETWEEN 1 AND 5)
		
);
"""

query(conn, create_tables, None)

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
    query(conn, 'INSERT INTO book_genre (book_isbn,genre_genre_id) values(%s,%s)',(row[0], row[1]))

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