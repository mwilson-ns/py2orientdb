"""
This just contains some global constants for constructing the
OrientDBConnection object. I like to keep my global variables in
their own namespace by importing config files like this one.
"""

ORIENTDB_URL = 'http://localhost:2480'
DATABASE = 'GratefulDeadConcerts'
PASSWORD = 'my_super_secret_password_from_the_orientdb_config_file'
LANGUAGE = 'sql'
USER = 'root'
