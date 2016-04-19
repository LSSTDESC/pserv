"""
Script to create and load CcdVisit, Object, and ForcedSource tables.
"""
from __future__ import absolute_import, print_function
import os
from warnings import filterwarnings
import MySQLdb as Database
import desc.pserv
import desc.pserv.utils as pserv_utils
from desc.pserv.registry_tools import find_registry, get_visits

# Suppress warnings from database module.
filterwarnings('ignore', category=Database.Warning)

def create_table(connection, table_name, dry_run=False, clobber=False):
    """
    Create the specified table using the create script in the sql
    folder.
    """
    if clobber and not dry_run:
        connection.apply('drop table if exists %s' % table_name)
    create_script = os.path.join(os.environ['PSERV_DIR'], 'sql',
                                 'create_%s.sql' % table_name)
    connection.run_script(create_script, dry_run=dry_run)

if __name__ == '__main__':
    connection = desc.pserv.LsstDbConnection(db='DESC_Twinkles_Level_2',
                                             read_default_file='~/.my.cnf')
    dry_run = False
    repo = '/nfs/slac/kipac/fs1/g/desc/Twinkles/400'

    # Create the CcdVisit, Object, and ForcedSource tables.
    create_table(connection, 'CcdVisit', dry_run=dry_run)
    create_table(connection, 'Object', dry_run=dry_run)
    create_table(connection, 'ForcedSource', dry_run=dry_run)

    # Ingest registry.sqlite3 data.
    print("Ingesting registry data into CcdVisit table.")
    registry_file = find_registry(repo)
    pserv_utils.ingest_registry(connection, registry_file)

    # Ingest the Object data.
    print("Ingesting ref-0-0,0.fits into Object table.")
    object_catalog = os.path.join(repo, 'deepCoadd-results/merged/0/0,0',
                                  'ref-0-0,0.fits')
    pserv_utils.ingest_Object_data(connection, object_catalog)

    # Ingest the forced source data.
    print("Ingesting forced source catalogs into ForcedSource table.")
    visits = get_visits(repo)
    for band, visit_list in visits.items():
        print("Processing band", band, "for", len(visit_list), "visits.")
        for ccdVisitId in visit_list:
            visit_name = 'v%i-f%s' % (ccdVisitId, band)
            #
            # @todo: Generalize this for arbitrary rafts and sensors.
            # This will need the data butler subset method to be fixed
            # first.
            #
            catalog_file = os.path.join(repo, 'forced', '0',
                                        visit_name, 'R22', 'S11.fits')
            print("Processing", visit_name)
            pserv_utils.ingest_ForcedSource_data(connection, catalog_file,
                                                 ccdVisitId)
