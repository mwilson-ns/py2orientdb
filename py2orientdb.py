"""
Class for interfacing with OrientDB from Python 2.7. The motivation for
this project is to have a very simple client that doesn't rely on
the Tinkerpop stack, and just uses the SQL-like language for OrientDB.
"""

import requests
import urllib2
import gzip
from StringIO import StringIO
import global_config as gc
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
def _select_from(db_connection, target, where_clause, skip=0):
    """Selects (using the SQL-like language) from the database.

       Using a private method outside the class definition because we
       want to use decorators for pagination, and there's a lot of
       weird voodoo when you apply a decorator inside a class definition.

       In these cases, there will always be a class method that calls
       the private method, and iterates through the results.
    """
    raw_query_text = 'SELECT FROM %s WHERE %s SKIP %s' % (
        target, where_clause, skip)
    query_text = urllib2.quote(raw_query_text)
    request_url = '/'.join([db_connection.server_address, 'query',
                            db_connection.database, 'sql', query_text])
    response = requests.get(request_url, cookies=db_connection.auth_cookie)
    result_list = response.json()['result']
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
    print request_url
    response = requests.get(request_url, cookies=db_connection.auth_cookie)
    result_list = response.json()['result']
    return result_list


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

    def select_from(self, target, where_clause):
        """Calls the _select_from method to retrieve documents using
           the SQL-like language.
        """
        for result in _select_from(self, target, where_clause):
            yield result

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

def main():
    orient_connection = OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database=gc.DATABASE)
    for i in orient_connection.select_from('v', "type = 'artist'"):
        document = orient_connection.get_document(i['@rid'])
        print document
    experiment_payload = {
        'foo': 'baz', u'name': u'Willie_Cobb', u'type': u'artist',
        u'@rid': u'#9:807', u'in_written_by': u'#9:806', u'@version': 4,
        u'@type': u'd', u'@class': u'V'}
    orient_connection.update_document('#9:806', experiment_payload)
    # orient_connection.create_vertex_class('animal')
    # new_document = {'name': 'party'}
    # response = orient_connection.create_document('animal', new_document)
    # response = orient_connection.select_from('animal', "name = 'drizzle'")

if __name__ == '__main__':
    main()
