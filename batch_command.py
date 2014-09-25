"""A little utility for piping POST commands through orientdb.
   The advantage to using this is that it submits the commands
   in batches, reducing the number of round trips necessary.

   usage: cat my_list_of_commands.txt | python ./batch_command.py"""

import py2orientdb
import sys
import global_config as gc
import hashlib


def read_batch(database):
    executed_commands = set([])

    counter = 0
    while 1:
        line = sys.stdin.readline().strip()
        if not line:
            break
        command_hash = hashlib.sha224(line).hexdigest()
        if command_hash in executed_commands:
            continue
        else:
            executed_commands.add(command_hash)
        print counter, line
        counter += 1
        # database.post_command(line)
        database.add_batch_command(line)
    database.flush_batch()



def main():
    database_connection = py2orientdb.OrientDBConnection(
        orientdb_address='http://localhost', orientdb_port=2480,
        user='root', password=gc.PASSWORD, database=gc.DATABASE)
    read_batch(database_connection)
    exit()

if __name__ == '__main__':
    main()
