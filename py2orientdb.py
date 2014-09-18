"""
Module for interfacing with OrientDB from Python 2.7. The motivation for
this project is to have a very simple client that doesn't rely on
the Tinkerpop stack, and just uses the SQL-like language for OrientDB.
"""

import requests
import urllib2
import gzip
from StringIO import StringIO
import global_config as gc # where I keep my passwords, etc.
import json
import copy


class AuthenticationError(Exception):
    """Error when the server doesn't accept the user's authentication.
       This will contain more information in the future -- just a
       placeholder for now.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class OrientDBResponseError(Exception):
    """Error when the OrientDB server doesn't give us a 2XX response
       code.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def _rid_format(rid):
    """Converts the ``rid`` as specified into a string of the form:
       #<cluster>:<id>.
    """
    if isinstance(rid, str):
        if rid[0] == '#':
            return rid
        else:
            return '#' + rid
    if isinstance(rid, list):
        rid = tuple(rid)
    if isinstance(rid, tuple):
        if len(rid) != 2:
            raise Exception('rid is an iterable of length != 2')
        rid = '#' + ':'.join(str(rid[0]), str(rid[1]))
        return rid
    raise Exception('rid is not a recognized format')


def _paginate(f, *args):
    """Decorator for mimicking a cursor object. OrientDB does not have
       cursor objects for paging through results. This decorator can be
       wrapped around a function which returns one set of results.
       It will cause the function to be executed repeatedly, skipping the
       number of documents that have been yielded already. The function
       decorated must have a 'skip=0' default argument.
    """
    def inner_function(*args):
        counter = 0
        results = f(*args, skip=counter)
        while len(results) > 0:
            for result in results:
                yield result
                counter += 1
            results = f(*args, skip=counter)
    return inner_function


def _check_response_code(f, *args, **kwargs):
    """Decorator that checks the requests response to see if the response
       code is in the 200's, signifying that all is well. If not, raises
       an OrientDBResponseError.
    """
    def inner_function(*args, **kwargs):
        out = f(*args, **kwargs)
        code = out.status_code
        print 'in the decorator...', code
        if str(code)[0] != '2':
            raise OrientDBResponseError
        else:
            return out
    return inner_function


@_check_response_code
def _update_document(db_connection, record_id, payload, update_mode='full'):
    """Updates a document by its record-id. Payload is a dictionary.
       Returns the response from the sever."""
    record_id = record_id.replace('#', '')
    payload = json.dumps(payload)
    request_url = '/'.join([
        db_connection.server_address, 'document',
        db_connection.database, record_id])
    response = requests.post(
        request_url, cookies=db_connection.auth_cookie, data=payload)
    return response


# @_paginate
def _select_from(db_connection, target, where_clause_dict, skip=0):
    """Selects (using the SQL-like language) from the database.

       Using a private method outside the class definition because we
       want to use decorators for pagination, and there's a lot of
       weird voodoo when you apply a decorator inside a class definition.

       In these cases, there will always be a class method that calls
       the private method, and iterates through the results.
    """
    raw_query_text = 'SELECT FROM %s WHERE %s SKIP %s' % (
        target, where_clause_dict, skip)
    # print raw_query_text
    query_text = urllib2.quote(raw_query_text)
    request_url = '/'.join([db_connection.server_address, 'query',
                            db_connection.database, 'sql', query_text])
    response = requests.get(request_url, cookies=db_connection.auth_cookie)
    try:
        result_list = response.json()['result']
    except ValueError:
        result_list = []
    return result_list


@_paginate
def _get_query(db_connection, query_text, language, skip=0):
    """Uses the GET method to execute a query. Note that in the orientDB
       scheme, GET requests are "idempotent", meaning that they don't
       modify the database.

       The ``query_text`` is being passed via the URL, so we
       might choke on ridiculously long queries.
    
       TODO: Include support for an optional fetchPlan.
    """
    query_text = urllib2.quote(query_text)
    query_text += ' skip=%s' % (skip)
    request_url = '/'.join([db_connection.server_address, 'query',
                            db_connection.database, language, query_text])
    # print request_url
    response = requests.get(request_url, cookies=db_connection.auth_cookie)
    result_list = response.json()['result']
    return result_list


def dict_to_where_clause(document, record_separator='.'):
    """Converts a dictionary-like document to a set of constraints suitable
       for matching in the SQL language.
    """
    path_list = []
    def inner_function(d, keypath=''):
        for k, v in d.iteritems():
            if not isinstance(v, dict):
                if len(keypath) > 0:
                    path_list.append(record_separator.join([keypath, k]))
                else:
                    path_list.append(k)
                keypath = ''
            else:
                if len(keypath) > 0:
                    inner_function(
                        v, keypath=record_separator.join([keypath, k]))
                else:
                    inner_function(v, keypath=k)
    inner_function(document)
    return path_list


def flatten_dict(document, record_separator='.'):
    """Creates a new dictionary by replacing the nested key structure
       with dot-delimited keypath strings.
    """
    path_list = dict_to_where_clause(
        document, record_separator=record_separator)
    d = {}
    for key_path in path_list:
        v = copy.deepcopy(document)
        for key in key_path.split(record_separator):
            v = v[key]
        d[key_path] = v
    return d

def where_clause(document):
    """Takes a possibly nested dictionary and returns the "where" clause of
       a SQL command that matches the dictionary's key/value pairs.
    """
    if len(document) == 0:
        raise Exception(
            'tried to create a where clause from empty dictionary.')
    clause = ''
    flattened_dictionary = flatten_dict(document)
    for k, v in flattened_dictionary.iteritems():
        if isinstance(v, str):
            v = '"' + v + '"'
        else:
            v = str(v)
        clause += '%s = %s, ' % (k, v)
    clause = clause[:-2] # get rid of the extra comma and space
    # clause = 'where ' + clause
    return clause


class OrientDBConnection(object):
    """Class for interfacing with OrientDB via REST interface"""
    def __init__(self, orientdb_address='http://localhost',
                 orientdb_port=2480, password='', user='', database=None,
                 to_base64=False, database_type='plocal'):
        if database is None:
            print 'Warning: no database specified.'
            database = ''
        orientdb_url = ':'.join([orientdb_address, str(orientdb_port)])
        if to_base64:
            import base64
            password = base64.b64encode(password)
        auth_url = '/'.join([orientdb_url, 'connect', database])
        auth_response = requests.get(
            auth_url, auth=requests.auth.HTTPBasicAuth(user, password))
        if str(auth_response.status_code)[0] != '2':
            raise AuthenticationError(
                'Authentication failed. Got response %s.' % (str(
                auth_response.status_code)))
        self.auth_cookie = auth_response.cookies
        self.auth_response = auth_response
        self.password = password
        self.user = user
        self.orientdb_address = orientdb_address
        self.orientdb_port = orientdb_port
        self.database = database
        self.to_base64 = to_base64
        self.server_address = (self.orientdb_address + ':' + 
            str(self.orientdb_port))
        self.database_type = database_type

    def database_info(self):
        """Returns information about the current database.
           BROKEN: Might need additional authentication"""
        request_url = '/'.join([
            self.server_address, 'database', self.database])
        response = requests.get(request_url)
        return response

    def list_databases(self):
        """Returns a list of all the databases."""
        request_url = '/'.join([
            self.server_address, 'listDatabases'])
        response = requests.get(request_url, cookies=self.auth_cookie)
        return response.json()['databases']

    def select_from(self, target, where):
        """Calls the _select_from method to retrieve documents using
           the SQL-like language.
        """
        if isinstance(where, dict):
            where = where_clause(where)
        for result in _select_from(self, target, where):
            yield result

    def check_exists(self, graph_class, document):
        """Check whether an edge or vertex exists containing the document.
           Change this so that it returns a boolean"""
        where = where_clause(document)
        return self.select_from(graph_class, where)

    def get_document(self, record_id):
        """Retrieves one document with the given record_id. The record_id
           can be prefixed with '#' or not.
        """
        record_id = record_id.replace('#', '')
        request_url = '/'.join([
            self.server_address, 'document', self.database, record_id])
        response = requests.get(request_url, cookies=self.auth_cookie)
        return response.json()

    def post_command(self, command_text, language='sql'):
        """Executes a command against the database. In OrientDB, POST commands
           are the ones that can modify the database.

           We're putting the command in the URL, so beware of really long
           command strings.
        """
        command_text = urllib2.quote(command_text)
        request_url = '/'.join([self.server_address, 'command',
                                self.database, language, command_text])
        response = requests.post(request_url, cookies=self.auth_cookie)
        return response

    def get_query(self, query_text, language):
        """Executes a GET query against the database."""
        for result in _get_query(self, query_text, language):
            yield result

    def connections(self):
        """This is broken because it requires the user to be authenticated
           in the OrientDB Server realm, whatever that is.
        """
        request_url = '/'.join([self.server_address, 'connections',
                                self.database])
        response = requests.get(request_url, cookies=self.auth_cookie)
        return response
    
    def update_document(self, record_id, payload, update_mode='full'):
        """Updates a document by its record-id. Payload is a dictionary.
           Returns the response from the sever."""
        response = _update_document(
            self, record_id, payload, update_mode='full')
        return response

    def export_database(self, file_name):
        """Exports the database in JSON format to ``file_name``. Uses the
           filename extension to guess what type of file you want to export."""
        request_url = '/'.join([self.server_address, 'export', self.database])
        response = requests.get(request_url, cookies=self.auth_cookie)
        if file_name.lower()[-7:] == 'json.gz':
            # OrientDB responds with gzip'd data by default
            f = open(file_name, 'wb')
            f.write(response.content)
            f.close()
        elif file_name.lower()[-4:] == 'json':
            db_dump = gzip.GzipFile(fileobj=StringIO(response.content)).read()
            f = open(file_name, 'w')
            f.write(db_dump)
            f.close()
        else:
            raise NotImplementedError(
                "Unable to infer output filetype from name.")

    def class_information(self, class_name):
        """Returns information about the requested class."""
        request_url = '/'.join([
            self.server_address, 'class', self.database, class_name])
        response = requests.get(request_url, cookies=self.auth_cookie)
        return response.json()

    def create_vertex_class(self, class_name):
        """Creates a new class that extends the built-in Vertex class.
           Does not check whether it exists already.
        """
        self.post_command('create class %s extends V' % (class_name))

    def create_edge_class(self, class_name):
        """Creates a new class that extends the built-in Edge class
           Does not check whether it exists already.
        """
        self.post_command('create class %s extends E' % (class_name))

    def create_document(self, class_name, document):
        """Creates a new document of type ``class_name``. Returns the
           document with the new @rid added.
        """
        request_url = '/'.join([
            self.server_address, 'document', self.database])
        payload = copy.deepcopy(document)
        payload['@class'] = class_name
        payload = json.dumps(payload)
        response = requests.post(request_url, cookies=self.auth_cookie, data=payload)
        return response

    def create_edge(self, source_id, target_id, subclass='E', content=None):
        """Creates an edge from the vertex with 'source_id' to the vertex
           with 'target_id'. If specified, the edge will in in class
           ``subclass`` and contain the document in the ``content``
           dictionary. As a convenience, we also check whether the ids have
           a "#' in front of them, adding it if they don't.
        """
        if source_id[0] != '#':
            source_id = '#' + source_id
        if target_id[0] != '#':
            target_id = '#' + target_id
        command_text = 'create edge %s from %s to %s' % (
            subclass, source_id, target_id)
        if content is not None:
            content = json.dumps(content)
            command_text = ' '.join([command_text, 'content', content])
        # print 'command:', command_text
        response = self.post_command(command_text)
        return response

    def create_class_property(
        self, class_property, class_name, property_type):
        """Create a class property."""
        request_url = '/'.join([
            self.server_address, 'property', self.database, class_name,
            class_property, property_type.upper()])
        # print request_url
        response = requests.post(request_url, cookies=self.auth_cookie)
        return response

    def create_vertex(self, subclass='V', content=None, ignore=False):
        """Create a vertex with the given content. If ``ignore`` is set, then
           fail silently if there is already a vertex with the same content.
        """
        if ignore:
            pass # check whether vertex with the same content exists
                 # if so, just return
        command_text = 'create vertex %s' % (subclass)
        if content is not None:
            content = json.dumps(content)
            command_text = ' '.join([command_text, 'content', content])
        # print 'command:', command_text
        response = self.post_command(command_text)
        return response

# {u'name': u'Pigpen_Weir', u'in_sung_by': [u'#9:69', u'#9:117', u'#9:227', u'#9:66', u'#9:39', u'#9:378', u'#9:634', u'#9:641'], u'@fieldTypes': u'in_sung_by=g', u'@rid': u'#9:258', u'type': u'artist', u'@version': 11, u'@type': u'd', u'@class': u'V'}
# {u'name': u'Robert_Johnson', u'type': u'artist', u'@rid': u'#9:261', u'in_written_by': u'#9:69', u'@version': 4, u'@type': u'd', u'@class': u'V'}

def main():
    orient_connection = OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database=gc.DATABASE)
    for i in orient_connection.select_from('v', "type = 'artist'"):
        print i
        # document = orient_connection.get_document(i['@rid'])
        # print document
    import pdb; pdb.set_trace()
    experiment_payload = {
        'foo': 'baz', u'name': u'Willie_Cobb', u'type': u'artist',
        u'@rid': u'#9:807', u'in_written_by': u'#9:806', u'@version': 4,
        u'@type': u'd', u'@class': u'V'}
    orient_connection.update_document('#9:806', experiment_payload)
    # select in() from Restaurant where name = 'Dante')
    # def _get_query(db_connection, query_text, language, skip=0):
    for result in orient_connection.get_query("select in() from V where name = 'Willie_Cobb'", 'sql'):
        print result
    # result = orient_connection.create_edge('#9:117', '#9:261')
    # orient_connection.select_from('v', "")
    # result = orient_connection.create_edge('%rid1', '%rid2')
    result = orient_connection.create_vertex(content={'goo': 1, 'goober': 2})
    # orient_connection.create_vertex_class('animal')
    # new_document = {'name': 'party'}
    # response = orient_connection.create_document('animal', new_document)
    # response = orient_connection.select_from('animal', "name = 'drizzle'")

if __name__ == '__main__':
    main()
