from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker
import networkx as nx
import matplotlib.pyplot as plt

# create a in memory sqlite database
engine = create_engine('sqlite:///:memory:', echo=False)

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

    if not catalog:
        catalog = Catalog(name=example['catalog'])
        session.add(catalog)
        session.commit()

    # create author and add if it doesn't exist already
    author = session.query(Author).filter(Author.name == example['author']).first()

    if not author:
        author = Author(name=example['author'])
        session.add(author)
        session.commit()

    # create a book referenced to author and catalog and add
    book = Book(title=example['title'], author_id=author.id, catalog_id=catalog.id)
    session.add(book)
    session.commit()

for book in session.query(Book).all():
    print(book.catalog, book.author, book.title)


"""
To automatically construct joining paths between two tables, a graph of the database and its table relationship is created.
The graph is then used to construct a joining path. 
"""


def create_graph(Base, plot=True):
    """
    Create a graph of the database. The nodes are the view tables and the edges are the relationship between the tables.
    :param Base: declarative base of the database model
    :param plot: if True the constructed graph will be plotted. Default=True
    """
    G = nx.Graph()

    # add nodes
    for t in Base.metadata.sorted_tables:
        # add the table name
        G.add_node(str(t.name))

    # iterate over each table and extract foreignkey relationships and add edge
    for table in Base.metadata.sorted_tables:
        table_name = str(t.name)

        # iterate over each foreignkey
        for fk in table.foreign_keys:
            table_column = fk.constraint.column_keys[0]  # list of columns that are constrained by the foreignkey
            fk_table = str(fk.column.table) # table name of the foreignkey
            fk_column = str(fk.column.name) # column name of the foreignkey

            # create a tuple storing the join relationship
            join_on = ((table_name, table_column), (fk_table, fk_column))

            # add the edge to the graph
            G.add_edge(table_name, fk_table, join_on=join_on)

    if plot:
        plt.figure(figsize=(10, 10))
        nx.draw(G, with_labels=True, font_weight='bold')
        plt.show()

    return G


def resolve_join(graph, table_start, table_end):
    """
    Use the information provided within filters and the graph to infer which tables have to be joined and in which order.

    :param table: the table that you want to select from
    :param filters: a dict with filters (only equal and "and" connected) {'region': ['DEU', 'FRA']}
    """

    # calculate the shortest path between the root table (select) and the filter table
    path = nx.shortest_path(graph, table_start, table_end)

    # resolve the relationship between the tables
    # extract information on which fields to join (edge attribute 'join_on')
    joins = {}

    temp = []
    for index, table in enumerate(path):
        if index < len(path) - 1:
            # information about the join is stored inside the edge of the graph
            join_on = graph.edges[(table, path[index + 1])]['join_on']

            # add a tuple (table_to_join, (table1, field1), (table2, field2))
            # JOIN table_to_join ON table1.field1 == table2.field2
            if path[index + 1] not in joins:
                joins[path[index + 1]] = join_on

    # convert the dictionar to a list of tuples
    joins = [(table, *joins[table]) for table in joins]

    # return a list of joins
    return joins

g = create_graph(Base, plot=True)
joins = resolve_join(g, 'authors', 'catalogs')

print(joins)