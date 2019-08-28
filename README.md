# auto-join
 
This is a small snipped I used in a slightly more complex implementation to resolve joins between arbitrary tables of a database model. The idea I had was to create a graph from the database model with nodes representing the tables and edges the relationship between tables. 

I used it in conjunction with SQLAlchemies autoload functionality. The underlying database schema was a star schema and the script came quite handy when resolving queries of dimension tables. 
