from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker

# create a in memory sqlite database
engine = create_engine('sqlite:///:memory:', echo=True)

# create the base for declarative class mapping
Base = declarative_base()


# create classes
# in this example its a library catalog system
class Catalog(Base):
    __tablename__ = 'catalogs'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return '<Catalog (name={name})>'.format(name=self.name)


class Author(Base):
    __tablename__ = 'authors'

    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __repr__(self):
        return '<Author (name={name}>'.format(name=self.name)


class Book(Base):
    __tablename__ = 'books'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    author_id = Column(Integer, ForeignKey('authors.id'))
    catalog_id = Column(Integer, ForeignKey('catalogs.id'))

    author = relationship("Author", back_populates="books")
    catalog = relationship("Catalog", back_populates="books")

    def __repr__(self):
        return '<Book (title={title})>'.format(title=self.title)


# create relationships between tables
Author.books = relationship("Book", order_by=Book.id, back_populates="author")
Catalog.books = relationship("Book", order_by=Book.id, back_populates="catalog")

# create all defined tables
Base.metadata.create_all(engine)

# create examples
examples = [
    {'catalog': 'Science',  'author': 'Richard Feynman', 'title': 'Lectures on Physics Vol 1'},
    {'catalog': 'Science',  'author': 'Richard Feynman', 'title': 'Lectures on Physics Vol 2'},
]

# create a session
Session = sessionmaker(bind=engine)
session = Session()

# add examples to the database
for example in examples:
    # create catalog and add if it doesn't exist yet
    catalog = session.query(Catalog).filter(Catalog.name == example['catalog']).first()
    print('catalog', catalog)

    if not catalog:
        catalog = Catalog(name=example['catalog'])
        session.add(catalog)
        session.commit()

    print('*** catalog id', catalog.id)

    # create author and add if it doesn't exist already
    author = session.query(Author).filter(Author.name == example['author']).first()

    if not author:
        author = Author(name=example['author'])
        session.add(author)
        session.commit()

    print('*** author id', author.id)

    # create a book referenced to author and catalog and add
    book = Book(title=example['title'], author_id=author.id, catalog_id=catalog.id)
    session.add(book)
    session.commit()

for book in session.query(Book).all():
    print(book.title)
    print(book.author)
    print(book.catalog)