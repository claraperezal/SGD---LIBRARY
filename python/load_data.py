import pandas as pd
import psycopg2
import random
from datetime import datetime
import bcrypt


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



def db_connection():
    # NOTE: change the host to "db" if you are running as a Docker container
    db = psycopg2.connect(user = "gabs",
                            password = "admin",
                            host = "localhost", #"db",
                            port = "5432",
                            database = "projeto")
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
DROP TABLE IF EXISTS book_genre CASCADE;
DROP TABLE IF EXISTS book_author CASCADE;
DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS loan CASCADE;

DROP TABLE IF EXISTS admin CASCADE;
DROP TABLE IF EXISTS librarian CASCADE;
DROP TABLE IF EXISTS readers CASCADE;

DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS author CASCADE;
DROP TABLE IF EXISTS book CASCADE;

DROP TABLE IF EXISTS users CASCADE;
"""
query(conn, drop_tables, None)
print("DROPPED")
create_tables = """
CREATE TABLE users (
	user_id	 INTEGER,
	username VARCHAR(50) NOT NULL,
	email	 VARCHAR(512) NOT NULL,
	password VARCHAR(512) NOT NULL,
	PRIMARY KEY(user_id)
);

CREATE TABLE loan (
	loan_id		 INTEGER,
	loan_date		 TIMESTAMP NOT NULL,
	return_date		 TIMESTAMP,
	readers_users_user_id INTEGER NOT NULL,
	book_isbn		 VARCHAR(50) NOT NULL,
	PRIMARY KEY(loan_id)
);

CREATE TABLE review (
	review_id		 BIGINT,
	rating		 INTEGER NOT NULL,
	comment		 VARCHAR(512),
	review_date		 TIMESTAMP NOT NULL,
	readers_users_user_id INTEGER NOT NULL,
	book_isbn		 VARCHAR(50) NOT NULL,
	PRIMARY KEY(review_id)
);

CREATE TABLE book (
	isbn		 VARCHAR(50),
	title		 VARCHAR(50) NOT NULL,
	description	 VARCHAR(512),
	num_pages	 INTEGER NOT NULL,
	num_copies	 INTEGER NOT NULL,
	registration_date TIMESTAMP NOT NULL,
	PRIMARY KEY(isbn)
);

CREATE TABLE genre (
	genre_id INTEGER,
	name	 VARCHAR(20) NOT NULL,
	PRIMARY KEY(genre_id)
);

CREATE TABLE librarian (
	salary	 INTEGER,
	users_user_id INTEGER,
	PRIMARY KEY(users_user_id)
);

CREATE TABLE readers (
	membership_number VARCHAR(512),
	users_user_id	 INTEGER,
	PRIMARY KEY(users_user_id)
);

CREATE TABLE author (
	author_id BIGINT,
	name	 VARCHAR(512),
	PRIMARY KEY(author_id)
);

CREATE TABLE admin (
	access_level	 SMALLINT,
	users_user_id INTEGER,
	PRIMARY KEY(users_user_id)
);

CREATE TABLE book_author (
	book_isbn	 VARCHAR(50),
	author_author_id BIGINT,
	PRIMARY KEY(book_isbn,author_author_id)
);

CREATE TABLE book_genre (
	book_isbn	 VARCHAR(50),
	genre_genre_id INTEGER,
	PRIMARY KEY(book_isbn,genre_genre_id)
);

ALTER TABLE users ADD UNIQUE (username, email);
ALTER TABLE users ADD CONSTRAINT Check_email CHECK (email LIKE '%@%.%');
ALTER TABLE users ADD CONSTRAINT Strong_password CHECK (LENGTH(password) >= 6);
ALTER TABLE loan ADD CONSTRAINT loan_fk1 FOREIGN KEY (readers_users_user_id) REFERENCES readers(users_user_id);
ALTER TABLE loan ADD CONSTRAINT loan_fk2 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE review ADD CONSTRAINT review_fk1 FOREIGN KEY (readers_users_user_id) REFERENCES readers(users_user_id);
ALTER TABLE review ADD CONSTRAINT review_fk2 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE review ADD CONSTRAINT Rating CHECK (rating BETWEEN 0 and 5);
ALTER TABLE book ADD CONSTRAINT positive_pages CHECK (num_pages > 0);
ALTER TABLE book ADD CONSTRAINT non_negative_copies CHECK (num_copies >= 0);
ALTER TABLE genre ADD UNIQUE (name);
ALTER TABLE librarian ADD CONSTRAINT librarian_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE readers ADD CONSTRAINT readers_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE admin ADD CONSTRAINT admin_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE book_author ADD CONSTRAINT book_author_fk1 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE book_author ADD CONSTRAINT book_author_fk2 FOREIGN KEY (author_author_id) REFERENCES author(author_id);
ALTER TABLE book_genre ADD CONSTRAINT book_genre_fk1 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE book_genre ADD CONSTRAINT book_genre_fk2 FOREIGN KEY (genre_genre_id) REFERENCES genre(genre_id);
"""
query(conn, create_tables)

genres_types = pd.read_csv('csv/genre.csv')
for row in genres_types.values:
    query(conn, 'INSERT INTO genre (genre_id, name) values(%s,%s)', (row[0], row[1],))

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
    query(conn, 'INSERT INTO author (author_id, name) values(%s,%s)',(row[0], row[1]))

book_author=pd.read_csv('csv/book_author.csv')
for row in book_author.values:
    query(conn,'INSERT INTO book_author (book_isbn, author_author_id) values(%s,%s)',(row[0], row[1]))

user=pd.read_csv('csv/users.csv')
for row in user.values:
    query(conn, 'INSERT INTO users (user_id, username,email,password) values(%s,%s,%s,%s)',(row[0], row[1], row[2], hash_password(row[3])))

readers=pd.read_csv('csv/readers.csv')
for row in readers.values:
    query(conn, "INSERT INTO readers (membership_number, users_user_id) values(%s,%s)",(row[0],row[1]))


reviews = pd.read_csv('csv/review.csv')
for row in reviews.values:
    query(conn,'INSERT INTO review (review_id, rating, comment, review_date, readers_users_user_id, book_isbn) VALUES (%s,%s,%s,%s,%s,%s)',(row[0], row[1], row[2], row[3], row[4], row[5] ))


print("DONE")