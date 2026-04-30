-- Drops in case they already exist
DROP TABLE IF EXISTS review CASCADE;
DROP TABLE IF EXISTS loan CASCADE;
DROP TABLE IF EXISTS book_genre CASCADE;
DROP TABLE IF EXISTS book CASCADE;
DROP TABLE IF EXISTS genre CASCADE;
DROP TABLE IF EXISTS readers CASCADE;
DROP TABLE IF EXISTS librarian CASCADE;
DROP TABLE IF EXISTS administrator CASCADE;
DROP TABLE IF EXISTS users CASCADE;




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
	author			 VARCHAR(255) NOT NULL,
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

	CONSTRAINT chk_num_pages_positve
		CHECK (num_pages > 0),

	CONSTRAINT chk_num_copies_negative
		CHECK (num_copies >= 0)
);


CREATE TABLE book_genre (
	book_isbn	 VARCHAR(50) NOT NULL,
	genre_genre_id INTEGER NOT NULL,
	PRIMARY KEY(book_isbn, genre_genre_id),

	CONSTRAINT fk_book_genre_book
		FOREIGN KEY(book_isbn)
		REFERENCES book(isbn)
		ON DELETE CASCADE,

	CONSTRAINT fk_book_genre_genre
		FOREIGN KEY(genre_genre_id)
		REFERENCES genre(genre_id)
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
		REFERENCES book(isbn)
		ON DELETE CASCADE,

	CONSTRAINT chk_retunr_after_loan
		CHECK (return_date IS NULL OR return_date >= loan_date)
);

CREATE TABLE review (
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
		REFERENCES book(isbn)
		ON DELETE CASCADE,

	CONSTRAINT uq_review_reader_book
		UNIQUE (readers_users_user_id, book_isbn),

	CONSTRAINT chk_rating_range
		CHECK (rating BETWEEN 1 AND 5)
		
);

CREATE INDEX idx_loan_active
ON loan (book_isbn)
WHERE return_date IS NULL;

CREATE INDEX idx_user_loans
ON loan (readers_users_user_id)
WHERE return_date IS NULL;