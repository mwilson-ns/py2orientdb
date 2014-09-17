import gzip
import py2orientdb
import global_config as gc


def main():
    database_connection = py2orientdb.OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database='kb')
    f = gzip.open(gc.ARTICLE_CATEGORIES_FILE, 'r')
    counter = 0
    for line in f:
        counter += 1
        if line[0] == '#':
            continue
        source, edge, target, _ = line.split()


if __name__ == '__main__':
    main()
