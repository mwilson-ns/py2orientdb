py2orientdb
===========
This is a work in progress. It will be a very simple, minimal client for
interacting with OrientDB from Python 2.7, using OrientDB's REST interface
with Python's requests library.

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

