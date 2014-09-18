"""Module for importing compressed ttl files."""

import gzip
import py2orientdb
import progressbar

# assumes there is a graph database called "kb"
# create database remote:localhost/kb root D5F8F36BB33B6B3171C7F479743E112235B0E475D4D3781ABF10705277419D55 plocal
# connect remote:localhost/kb root D5F8F36BB33B6B3171C7F479743E112235B0E475D4D3781ABF10705277419D55
# orientdb {kb}> CREATE INDEX article_uri ON article (uri) dictionary_hash_index
# add index creation to py2orientdb as a generic post command

def import_ttl_file(file_name, source_class, target_class, edge_class, test_only=False):
    database_connection = py2orientdb.OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database='kb')
    print 'getting number of lines...'
    f = gzip.open(file_name, 'r')
    total_lines = 0
    for line in f:
        total_lines += 1
    f.close()
    print 'done...'
    widgets = [
        'Getting vertices: ', progressbar.Percentage(), ' ',
        progressbar.Bar('>'), ' ',
        progressbar.ETA(' ')]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=total_lines).start()
    f = gzip.open(file_name, 'r')
    counter = 0
    source_set = set([])
    target_set = set([])
    for line in f:
        counter += 1
        if line[0] == '#':
            continue
        if test_only and counter > 10000:
            break
        source, edge, target, _ = line.split()
        source_set.add(source)
        target_set.add(target)
        if counter % 100 == 0:
            pbar.update(counter)
    f.close()
    pbar.finish()
    counter = 0
    database_connection.create_vertex_class(source_class)
    database_connection.create_vertex_class(target_class)
    database_connection.create_edge_class(edge_class)
    database_connection.create_class_property('uri', source_class, 'string')
    database_connection.create_class_property('uri', target_class, 'string')
    database_connection.create_class_property('uri', edge_class, 'string')
    widgets = [
        'Creating source vertices: ', progressbar.Percentage(), ' ',
        progressbar.Bar('>'), ' ',
        progressbar.ETA(' ')]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(source_set)).start()
    for source in source_set:
        counter += 1
        if counter % 100 == 0:
            pbar.update(counter)
        d = {'uri': source}
        if len(list(database_connection.check_exists(source_class, d))) > 0:
            pass # we already have the vertex
        else:
            database_connection.create_vertex(subclass=source_class, content=d)
    pbar.finish()
    counter = 0
    widgets = [
        'Creating target vertices: ', progressbar.Percentage(), ' ',
        progressbar.Bar('>'), ' ',
        progressbar.ETA(' ')]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=len(target_set)).start()
    for target in target_set:
        counter += 1
        if counter % 100 == 0:
            pbar.update(counter)
        d = {'uri': target}
        if len(list(database_connection.check_exists(target_class, d))) > 0:
            pass
        else:
            database_connection.create_vertex(subclass=target_class, content=d)
    pbar.finish()
    f = gzip.open(file_name, 'r')
    counter = 0
    widgets = [
        'Creating edges: ', progressbar.Percentage(), ' ',
        progressbar.Bar('>'), ' ',
        progressbar.ETA(' ')]
    pbar = progressbar.ProgressBar(widgets=widgets, maxval=total_lines).start()
    for line in f:
        counter += 1
        if counter % 100 == 0:
            pbar.update(counter)
        if line[0] == '#':
            continue
        source, edge, target, _ = line.split()
        try:
            source_rid = list(database_connection.select_from(
                source_class, {'uri': source}))[0]['@rid']
            target_rid = list(database_connection.select_from(
                target_class, {'uri': target}))[0]['@rid']
        except:
            pass
        database_connection.create_edge(
            source_rid, target_rid, subclass=edge_class,
            content={'uri': edge})
    pbar.finish()

if __name__ == '__main__':
    import global_config as gc
    import_ttl_file(gc.ARTICLE_CATEGORIES_FILE, 'article', 'category', 'in_category', test_only=False)

