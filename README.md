py2orientdb
===========
This is a work in progress. It will be a very simple, minimal client for
interacting with OrientDB from Python 2.7, using OrientDB's REST interface
with Python's requests library.

If there was a Greek letter before alpha, I'd have used that letter to
describe the current state of this code.

Goals
-----
The goals for this project are to create a client meeting these criteria:

+ It should not require a complex software stack, such as Tinkerpop (which
  is great, but may not always be available).
+ It should support the entire SQL-like language that's built into OrientDB.
+ Relatively simple graph operations should be made easy.
+ There should be simple, intuitive methods to facilitate common SQL commands,
  combined with more general methods for passing arbitrary commands to the
  database.

Usage
-----
Import py2orientdb and create an OrientDBConnection object:

~~~~{.python}
import py2orientdb

DATABASE = 'GratefulDeadConcerts'
PASSWORD = '*******'
USER = 'root'
SERVER = 'http://localhost'
PORT = 2480

orient_connection = py2orientdbOrientDBConnection(
    orientdb_address=SERVER, orientdb_port=PORT,
    user=USER, password=PASSWORD, database=DATABASE)
~~~~

Then use the OrientDBConnection methods to query the database:

~~~~{.python}
for i in orient_connection.select_from('v', "type = 'artist'"):
    document = orient_connection.get_document(i['@rid'])
    print document
~~~~
