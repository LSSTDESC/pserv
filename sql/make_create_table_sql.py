from __future__ import absolute_import, print_function
import sys

infile = sys.argv[1]
table_name = sys.argv[2]

print('create table %s (' % table_name)
with open(infile, 'r') as input_data:
    for line in input_data:
        tokens = line.split()
        print('       %s %s,' % tuple(tokens[:2]))
print(')')

