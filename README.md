py2orientdb
===========
This is a work in progress. It will be a very simple client for
interacting with OrientDB from Python 2.7, using OrientDB's REST interface
with Python's requests library. The main goal is to create a client that
does not require other libraries like Tinkerpop, and which enables a user
who is familiar with SQL to create and query an OrientDB graph easily.

Goals
-----
The goals for this project are to create a client that:

+ does not require a complex software stack, such as Tinkerpop (which
  is great, but may not always be available).
+ supports the entire SQL-like language that's built into OrientDB.
+ makes graph operations relatively simple, without requiring external
  libraries or special configuration.
+ fills-in gaps in functionality of OrientDB that SQL (and other) users
  are used to having. For example, lack of a cursor object for paging
  through results; lack of an equivalent to SQL's "INSERT IGNORE" query.
+ makes it painless to import graphs in common formats.
+ supports and encourages creation of classes in definitions of graphs.
+ supports optimization of indices from within the client.

Afte we've got SQL working, we'll start working on suport for Gremlin.

Usage
-----
Import py2orientdb and create an OrientDBConnection object:

~~~~{.python}
import py2orientdb

DATABASE = 'GratefulDeadConcerts'
PASSWORD = 'MY_SUPER_SECRET_PASSWORD'
USER = 'root'
SERVER = 'http://localhost'
PORT = 2480

orient_connection = py2orientdb.OrientDBConnection(
    orientdb_address=SERVER, orientdb_port=PORT,
    user=USER, password=PASSWORD, database=DATABASE)
~~~~

Then use the OrientDBConnection methods to query the database:

~~~~{.python}
for i in orient_connection.select_from('v', "type = 'artist'"):
    document = orient_connection.get_document(i['@rid'])
    print document
~~~~

To do list
----------
+ Unit testing
+ Sphinx documentation
+ Error handling with more informative exceptions
+ Extra authentication for database-level operations (e.g.
  creating a database, listing databases, etc).

Notes
-----
If you look at the main method of py2orientdb.py, you'll see that I've
imported a file called global_config.py, which does nothing but contain
the password, server address, and other configuration information that's
needed to construct the OrientDBConnection object.
