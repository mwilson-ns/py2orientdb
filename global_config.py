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
BASE_DIR = '/Users/zernst/projects/py2orientdb'
TTL_DIR = BASE_DIR + '/' + 'ttl'
ARTICLE_CATEGORIES_FILE = TTL_DIR + '/' + 'article_categories_en.ttl.gz'

#article_categories_en.ttl.gz labels_en.ttl.gz
#geo_coordinates_en.ttl.gz    persondata_en.ttl.gz
