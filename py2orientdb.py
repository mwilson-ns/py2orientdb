import requests
import urllib2
import gzip
from StringIO import StringIO
import global_config as gc


class AuthenticationError(Exception):
    """Error when the server doesn't accept the user's authentication.
       This will contain more information in the future -- just a
       placeholder for now.
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def paginate(f, *args):
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


@paginate
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


@paginate
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


def main():
    orient_connection = OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database=gc.DATABASE)

    # info = orient_connection.database_info()
    # import pdb; pdb.set_trace()
    for i in orient_connection.select_from('v', "type = 'artist'"):
        print i
        document = orient_connection.get_document(i['@rid'])
        print document
    # print list(orient_connection.post_command("update Artist set online = false"))
    # orient_connection.post_command("""gremlin('g = new OrientGraph("remote:localhost/GratefulDeadConcerts")')""", '')
    #for i in orient_connection.get_query("SELECT EXPAND(gremlin('g.V.count()'))", 'sql'):
    #    print i
    #for i in orient_connection.get_query("g.V", 'gremlin'):
    #    print i
    # print orient_connection.connections()
    # import pdb; pdb.set_trace()
    # orient_connection.export_database('foobar.json')

if __name__ == '__main__':
    main()
