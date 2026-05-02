CREATE TABLE users (
	user_id	 INTEGER,
	username VARCHAR(20) NOT NULL,
	email	 VARCHAR(50) NOT NULL,
	password VARCHAR(50) NOT NULL,
	role	 SMALLINT,
	PRIMARY KEY(user_id)
);

CREATE TABLE loan (
	loan_id	 INTEGER,
	loan_date	 TIMESTAMP NOT NULL,
	return_date	 TIMESTAMP,
	users_user_id INTEGER NOT NULL,
	book_isbn	 VARCHAR(50) NOT NULL,
	PRIMARY KEY(loan_id)
);

CREATE TABLE review (
	review_id	 BIGINT,
	rating	 INTEGER NOT NULL,
	comment	 VARCHAR(512),
	review_date	 TIMESTAMP NOT NULL,
	users_user_id INTEGER,
	book_isbn	 VARCHAR(50) NOT NULL,
	PRIMARY KEY(review_id)
);

CREATE TABLE book (
	isbn		 VARCHAR(50),
	title		 VARCHAR(20) NOT NULL,
	description	 VARCHAR(512),
	num_pages	 INTEGER NOT NULL,
	tot_copies	 INTEGER NOT NULL,
	registration_date TIMESTAMP NOT NULL,
	PRIMARY KEY(isbn)
);

CREATE TABLE genre (
	genre_id INTEGER,
	name	 VARCHAR(20) NOT NULL,
	PRIMARY KEY(genre_id)
);

CREATE TABLE author (
	author_id INTEGER,
	name	 VARCHAR(512),
	PRIMARY KEY(author_id)
);

CREATE TABLE book_author (
	book_isbn	 VARCHAR(50),
	author_author_id INTEGER,
	PRIMARY KEY(book_isbn,author_author_id)
);

CREATE TABLE book_genre (
	book_isbn	 VARCHAR(50),
	genre_genre_id INTEGER,
	PRIMARY KEY(book_isbn,genre_genre_id)
);

ALTER TABLE users ADD UNIQUE (username, email);
ALTER TABLE users ADD CONSTRAINT valid_role CHECK ( role IN (1, 2, 3));
ALTER TABLE loan ADD CONSTRAINT loan_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE loan ADD CONSTRAINT loan_fk2 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE review ADD CONSTRAINT review_fk1 FOREIGN KEY (users_user_id) REFERENCES users(user_id);
ALTER TABLE review ADD CONSTRAINT review_fk2 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE review ADD CONSTRAINT user_book_review UNIQUE (users_user_id, book_isbn);
ALTER TABLE review ADD CONSTRAINT valid_review CHECK (rating BETWEEN 1 AND 5);
ALTER TABLE genre ADD UNIQUE (name);
ALTER TABLE book_author ADD CONSTRAINT book_author_fk1 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE book_author ADD CONSTRAINT book_author_fk2 FOREIGN KEY (author_author_id) REFERENCES author(author_id);
ALTER TABLE book_genre ADD CONSTRAINT book_genre_fk1 FOREIGN KEY (book_isbn) REFERENCES book(isbn);
ALTER TABLE book_genre ADD CONSTRAINT book_genre_fk2 FOREIGN KEY (genre_genre_id) REFERENCES genre(genre_id);

