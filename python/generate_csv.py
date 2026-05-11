
import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()
random.seed(42)

os.makedirs('csv', exist_ok=True)

# =====================================================
# CONFIG
# =====================================================

NUM_READERS = 110
NUM_LIBRARIANS = 4
NUM_ADMINS = 1
NUM_USERS = NUM_READERS + NUM_LIBRARIANS + NUM_ADMINS

NUM_BOOKS = 1000
NUM_AUTHORS = 200
NUM_LOANS = 7000
NUM_REVIEWS = 450

# =====================================================
# GENRES
# =====================================================

genres = [
    'Fantasy',
    'Science Fiction',
    'Mystery',
    'Thriller',
    'Romance',
    'Horror',
    'Historical',
    'Adventure',
    'Drama',
    'Non-Fiction'
]

# =====================================================
# USERS
# =====================================================

domains = [
    'gmail.com',
    'hotmail.com',
    'yahoo.com',
    'outlook.com',
    'icloud.com',
    'librarymail.com'
]

users = []

used_usernames = set()
used_emails = set()

for i in range(1, NUM_USERS + 1):

    first = fake.first_name().lower()
    last = fake.last_name().lower()

    username_formats = [
        f'{first}{last}',
        f'{first}.{last}',
        f'{first}_{last}',
        f'{first}{random.randint(1,999)}',
        f'{first[:1]}{last}',
        f'{last}{random.randint(10,99)}'
    ]

    username = random.choice(username_formats)

    while username in used_usernames:
        username += str(random.randint(1,99))

    used_usernames.add(username)

    domain = random.choice(domains)

    email = f'{username}@{domain}'

    while email in used_emails:
        email = f'{username}{random.randint(1,99)}@{domain}'

    used_emails.add(email)

    password = fake.password(
        length=10,
        special_chars=True,
        digits=True,
        upper_case=True,
        lower_case=True
    )

    users.append([
        i,
        username,
        email,
        password
    ])

users_df = pd.DataFrame(users, columns=[
    'user_id',
    'username',
    'email',
    'password'
])

users_df.to_csv('csv/users.csv', index=False)

# =====================================================
# ADMINS
# =====================================================

administrators = []

administrators.append([
    1
])

admin_df = pd.DataFrame(administrators, columns=[
    'users_user_id'
])

admin_df.to_csv('csv/admin.csv', index=False)

# =====================================================
# LIBRARIANS
# =====================================================

librarians = []


for user_id in range(2, 2 + NUM_LIBRARIANS):

    librarians.append([
        user_id
    ])

librarian_df = pd.DataFrame(librarians, columns=[
    'users_user_id'])

librarian_df.to_csv('csv/librarian.csv', index=False)

# =====================================================
# READERS
# =====================================================

readers = []

reader_start = 2 + NUM_LIBRARIANS

for user_id in range(reader_start, NUM_USERS + 1):

    readers.append([
        user_id
    ])

readers_df = pd.DataFrame(
    readers,
    columns=['users_user_id']
)

readers_df.to_csv('csv/readers.csv', index=False)

# =====================================================
# GENRE
# =====================================================

genres = [
    'Fantasy',
    'Science Fiction',
    'Mystery',
    'Thriller',
    'Romance',
    'Horror',
    'Historical Fiction',
    'Adventure',
    'Drama',
    'Non-Fiction'
]

genre_rows = []

for i, genre_name in enumerate(genres, start=1):

    genre_rows.append([
        i,
        genre_name
    ])

genre_df = pd.DataFrame(genre_rows, columns=[
    'genre_id',
    'name'
])


genre_df.to_csv('csv/genre.csv', index=False)

# =====================================================
# AUTHORS
# =====================================================

authors = []

used_names = set()

nationalities = [
    'english',
    'spanish',
    'portuguese',
    'french',
    'italian'
]

for i in range(1, NUM_AUTHORS + 1):

    nationality = random.choice(nationalities)

    if nationality == 'english':
        fake_author = Faker('en_US')

    elif nationality == 'spanish':
        fake_author = Faker('es_ES')

    elif nationality == 'portuguese':
        fake_author = Faker('pt_PT')

    elif nationality == 'french':
        fake_author = Faker('fr_FR')

    else:
        fake_author = Faker('it_IT')

    author_name = fake_author.name()

    # evitar duplicados
    while author_name in used_names:
        author_name = fake_author.name()

    used_names.add(author_name)

    authors.append([
        i,
        author_name
    ])

author_df = pd.DataFrame(authors, columns=[
    'author_id',
    'author_name'
])

author_df.to_csv('csv/author.csv', index=False)

# =====================================================
# BOOKS
# =====================================================

books = []
book_isbns = []

title_words = [
    'Shadow','Empire','Secret','Dream','Storm','Fire',
    'Moon','Light','Night','Blood','Kingdom','Silence',
    'Memory','Garden','Ocean','Chronicles','Whisper',
    'Path','Legacy','Mirror','Ashes','River','Forest',
    'Star','Echo','Crown','Sword','Glass','Wolf',
    'Throne','Dust','Flame','Rain','Ice','Wind',
    'Heart','Stone','Sky','Song','Code','Machine',
    'Dragon','Hunter','Queen','Prince','City','World',
    'Fate','Fear','Vision','Gate','Void',
    'Phoenix','Dawn','Oblivion','Maze','Tower','Ship',
    'Blade','Soul','Ghost','Dreamer','Keeper','Circle',
    'Mountain','Sea','Island','Oracle','Temple','Labyrinth',
    'Season','Truth','Lies','Battle','Warrior','Guardian',
    'Crystal','Thunder','Lightning','Dust','Rose','Poison',
    'Legend','Beast','Library','Clock','Machine','Signal',
    'Journey','Crossing','Prisoner','Tower','Valley','Bridge',
    'Traveler','Map','Compass','Horizon','Fury','Chain',
    'Candle','Portrait','Mask','Room','Letter','Promise',
    'Game','Lab','Code','Virus','Planet','Satellite',
    'Robot','Network','Cipher','Algorithm','Protocol','Circuit',
    'Archive','Galaxy','Comet','Nebula','Gravity','Orbit',
    'Artifact','Sanctuary','Harbor','Shipwreck','Cemetery','Tunnel',
    'Academy','Diary','Notebook','Monument','Castle','Village',
    'Desert','Jungle','Cathedral','Opera','Museum','Painting',
    'Winter','Summer','Autumn','Spring','Midnight','Sunrise',
    'Sunset','Child','Mother','Father','Brother','Sister',
    'Enemy','Friend','Stranger','Detective','Doctor','Captain',
    'Professor','Scientist','Engineer','Pilot','Commander','General',
    'Agent','Spy','Assassin','Nomad','Merchant','King',
    'Empress','Rebel','Prophet','Priest','Monk','Soldier',
    'Arena','Fortress','Dungeon','Harvester','Farmer','Miner',
    'Merchant','Voyage','Stormbreaker','Nightfall','Dusk','Inferno',
    'Frost','Tempest','Raven','Falcon','Lion','Tiger',
    'Panther','Viper','Scorpion','Serpent','Shark','Eagle',
    'Falconer','Watcher','Wanderer','Scribe','Scholar','Archivist',
    'Myth','Rune','Relic','Portal','Dimension','Spectrum',
    'Pulse','Frequency','Signal','Static','Memory','Infinity',
    'Paradox','Collapse','Mutation','Experiment','Formula','Sequence',
    'Binary','Quantum','Cyber','Nano','Synthetic','Genesis',
    'Awakening','Requiem','Rebellion','Ascension','Revelation','Exile',
    'Redemption','Anomaly','Mirage','Reflection','Symphony','Velocity'
]

title_patterns = [
    'The {a} of {b}',
    '{a} and the {b}',
    'Beyond the {a}',
    'Return to the {a}',
    'The Last {a}',
    '{a} Chronicles',
    '{a} Rising',
    'Under the {a}',
    'The Hidden {a}',
    '{a} of the {b}'
]

descriptions = [
    'A gripping story full of mystery and unexpected twists.',
    'An emotional journey through friendship, ambition, and sacrifice.',
    'A fast-paced adventure set in a dangerous and unpredictable world.',
    'A touching novel about family, love, and difficult choices.',
    'A suspenseful thriller where nothing is what it seems.',
    'A dark fantasy filled with secrets, magic, and betrayal.',
    'An inspiring tale of courage and resilience.',
    'A dramatic story that explores human emotions and relationships.',
    'A compelling narrative with unforgettable characters.',
    'A captivating book that keeps readers engaged until the end.'
]

books = []
book_isbns = []
used_titles = set()

for i in range(1, NUM_BOOKS + 1):


    isbn = f'979-0-{random.randint(100,999)}-{random.randint(100000,999999)}-{random.randint(0,9)}'

    while isbn in book_isbns:
        isbn = f'979-0-{random.randint(100,999)}-{random.randint(100000,999999)}-{random.randint(0,9)}'

    book_isbns.append(isbn)

    pattern = random.choice(title_patterns)

    word1 = random.choice(title_words)
    word2 = random.choice(title_words)
    word3 = random.choice(title_words)

    while word1 == word2:
        word2 = random.choice(title_words)

    title = pattern.format(
        a=word1,
        b=word2
    )

    if random.random() < 0.6:
        title += f' {word3}'

    if random.random() < 0.3:
        title += f' {random.randint(1, 999)}'

    used_titles.add(title)


    description = random.choice(descriptions)


    num_pages = random.choices(
        [120, 180, 250, 320, 450, 600, 850],
        weights=[10, 20, 30, 20, 10, 7, 3]
    )[0]

    if random.random() < 0.15:
        num_copies = random.randint(10, 20)
    else:
        num_copies = random.randint(1, 8)

    registration_date = fake.date_between(
        start_date='-8y',
        end_date='today'
    )

    books.append([
        isbn,
        title,
        description,
        num_pages,
        num_copies,
        registration_date
    ])

book_df = pd.DataFrame(books, columns=[
    'isbn',
    'title',
    'description',
    'num_pages',
    'num_copies',
    'registration_date'
])

book_df.to_csv('csv/book.csv', index=False)


# =====================================================
# BOOK GENRE
# =====================================================
book_genres = []

genre_book_count = {i: 0 for i in range(1, 11)}

shuffled_books = book_isbns.copy()
random.shuffle(shuffled_books)


book_index = 0

for genre_id in range(1, 11):

    for _ in range(100):

        isbn = shuffled_books[book_index]

        book_genres.append([
            isbn,
            genre_id
        ])

        genre_book_count[genre_id] += 1

        book_index += 1

remaining_books = shuffled_books[book_index:]

for isbn in remaining_books:
    main_genre = random.randint(1, 10)

    book_genres.append([
        isbn,
        main_genre
    ])

    genre_book_count[main_genre] += 1
    if random.random() < 0.20:

        extra_genres = random.sample(
            [g for g in range(1, 11) if g != main_genre],
            random.randint(1, 2)
        )

        for g in extra_genres:

            book_genres.append([
                isbn,
                g
            ])

            genre_book_count[g] += 1

book_genre_df = pd.DataFrame(book_genres, columns=[
    'book_isbn',
    'genre_genre_id'
])

book_genre_df = book_genre_df.drop_duplicates()

book_genre_df.to_csv('csv/book_genre.csv', index=False)

# =====================================================
# BOOK AUTHOR
# =====================================================

book_authors = []

popular_authors = random.sample(
    range(1, NUM_AUTHORS + 1),
    25
)

author_book_count = {
    aid: 0 for aid in range(1, NUM_AUTHORS + 1)
}

for isbn in book_isbns:


    num_authors = random.choices(
        [1, 2, 3],
        weights=[75, 20, 5]
    )[0]

    selected_authors = []

    for _ in range(num_authors):

        if random.random() < 0.35:
            author_id = random.choice(popular_authors)
        else:
            author_id = random.randint(1, NUM_AUTHORS)

        while author_id in selected_authors:
            author_id = random.randint(1, NUM_AUTHORS)

        selected_authors.append(author_id)


    for author_id in selected_authors:

        book_authors.append([
            isbn,
            author_id
        ])

        author_book_count[author_id] += 1

book_author_df = pd.DataFrame(book_authors, columns=[
    'book_isbn',
    'author_author_id'
])

book_author_df = book_author_df.drop_duplicates()

book_author_df.to_csv('csv/book_author.csv', index=False)

# =====================================================
# LOANS
# =====================================================
reader_ids = readers_df['users_user_id'].tolist()

book_copy_map = {}

for row in books:
    book_copy_map[row[0]] = row[4]

popular_books = random.sample(book_isbns, 40)

heavy_readers = random.sample(reader_ids, 20)

active_loans_per_book = {isbn: 0 for isbn in book_isbns}
active_loans_per_user = {uid: 0 for uid in reader_ids}

loans = []
loan_id = 1

attempts = 0

while len(loans) < NUM_LOANS and attempts < 800000:

    attempts += 1

    if random.random() < 0.35:
        user_id = random.choice(heavy_readers)
    else:
        user_id = random.choice(reader_ids)

    if random.random() < 0.30:
        isbn = random.choice(popular_books)
    else:
        isbn = random.choice(book_isbns)

    loan_date = fake.date_between(
        start_date='-3y',
        end_date='today'
    )

    user_active_books = {
    l[4]
    for l in loans
    if l[3] == user_id and l[2] is None
}

    if isbn in user_active_books:
        continue

    active = random.random() < 0.05

    if active:

        if active_loans_per_user[user_id] >= 5:
            continue

        if active_loans_per_book[isbn] >= book_copy_map[isbn]:
            continue

        return_date = None

        active_loans_per_user[user_id] += 1
        active_loans_per_book[isbn] += 1

    else:

        duration = random.choices(
            [7, 14, 21, 30, 45, 60],
            weights=[15, 30, 25, 15, 10, 5]
        )[0]

        return_date = loan_date + timedelta(days=duration)

        if return_date > datetime.today().date():
            return_date = datetime.today().date()

    loans.append([
        loan_id,
        loan_date,
        return_date,
        user_id,
        isbn
    ])

    loan_id += 1

loan_df = pd.DataFrame(loans, columns=[
    'loan_id',
    'loan_date',
    'return_date',
    'readers_users_user_id',
    'book_isbn'
])

loan_df.to_csv('csv/loan.csv', index=False)


# =====================================================
# REVIEWS
# =====================================================

review_comments = [
    'I could not stop reading this book.',
    'The characters were very believable.',
    'Not my favorite, but still enjoyable.',
    'The ending surprised me a lot.',
    'Too slow at the beginning.',
    'One of the best books I read this year.',
    'Would definitely recommend it.',
    'Interesting concept but weak execution.',
    'Loved the writing style.',
    'I expected more from the story.',
    'Very emotional and well written.',
    'Great atmosphere and pacing.'
]

reviews = []
used_reviews = set()
review_id = 1

# libros prestados por usuario
user_books = {}

for row in loans:
    uid = row[3]
    isbn = row[4]

    if uid not in user_books:
        user_books[uid] = set()

    user_books[uid].add(isbn)

# asegurar mínimo 2 reviews por reader
for uid in reader_ids:

    if uid not in user_books:
        continue

    possible_books = list(user_books[uid])

    if len(possible_books) == 0:
        continue

    sample_size = min(2, len(possible_books))

    selected = random.sample(possible_books, sample_size)

    for isbn in selected:

        if (uid, isbn) in used_reviews:
            continue

        rating = random.choices(
            [1,2,3,4,5],
            weights=[5,10,20,35,30]
        )[0]

        comment = random.choice(review_comments)

        review_date = fake.date_between(
            start_date='-2y',
            end_date='today'
        )

        reviews.append([
            rating,
            comment,
            review_date,
            uid,
            isbn
        ])

        used_reviews.add((uid, isbn))

# reviews extra
while len(reviews) < NUM_REVIEWS:

    uid = random.choice(reader_ids)

    if uid not in user_books:
        continue

    isbn = random.choice(list(user_books[uid]))

    if (uid, isbn) in used_reviews:
        continue

    rating = random.choices(
        [1,2,3,4,5],
        weights=[5,10,20,35,30]
    )[0]

    comment = random.choice(review_comments)

    review_date = fake.date_between(
        start_date='-2y',
        end_date='today'
    )

    reviews.append([
        rating,
        comment,
        review_date,
        uid,
        isbn
    ])

    used_reviews.add((uid, isbn))

review_df = pd.DataFrame(reviews, columns=[
    'rating',
    'comment',
    'review_date',
    'readers_users_user_id',
    'book_isbn'
])

review_df.to_csv('csv/review.csv', index=False)

# =====================================================
# DONE
# =====================================================

print('CSV GENERATED SUCCESSFULLY')

print('SUMMARY:')
print('users:', len(users_df))
print('readers:', len(readers_df))
print('librarians:', len(librarian_df))
print('admins:', len(admin_df))
print('genres:', len(genre_df))
print('authors:', len(author_df))
print('books:', len(book_df))
print('book_genre:', len(book_genre_df))
print('book_author:', len(book_author_df))
print('loans:', len(loan_df))
print('reviews:', len(review_df))
