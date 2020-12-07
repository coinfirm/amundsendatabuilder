# Requires: pip install fastparquet

import argparse
import csv
import glob
import json
import logging
import os
import traceback
import uuid

from elasticsearch import Elasticsearch
from fastparquet import ParquetFile
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.csv_extractor import CsvTableColumnExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


def get_col_type(parquet_file, col_name):
    column_type = 'string'
    if 'org.apache.spark.sql.parquet.row.metadata' in parquet_file.key_value_metadata:
        metadata_json_str = parquet_file.key_value_metadata['org.apache.spark.sql.parquet.row.metadata']
        metadata = json.loads(metadata_json_str)
        types_dict = {}
        for field in metadata['fields']:
            types_dict[field['name']] = field['type']
        if col_name in types_dict:
            column_type = types_dict[col_name]
    return column_type


def create_parquets_scanner_job(database_name, cluster_name, target_paths):
    logging.info(f"Paths to process: {target_paths}")
    output_parquet_tables_csv_path = '/tmp/amundsen_scanner_parquets_tables.csv'
    output_parquet_columns_csv_path = '/tmp/amundsen_scanner_parquets_columns.csv'
    discovered_tables = []  # Amundsen uses 'tables' and 'columns' for all metadata types
    discovered_columns = []  # Amundsen uses 'tables' and 'columns' for all metadata types

    # Parse the parquet paths
    for path in target_paths:
        logging.info(f"Processing path: {path}")
        try:
            # Determine whether its a file or folder
            if not os.path.exists(path):
                logging.warning(f"Path is unreachable or doesn't exist: {path}")
                continue
            if os.path.isfile(path):
                # Process single Parquet file
                logging.info(f"Target path is a file")
                dataset_name = os.path.basename(path)
                pf = ParquetFile(path)
                discovered_tables.append((dataset_name, path))
                for col in pf.columns:
                    discovered_columns.append((col, get_col_type(pf, col), dataset_name))
            else:
                # Process folder containing multiple partitions of the same parquet dataset
                logging.info(f"Target path is a directory")
                dataset_name = os.path.basename(path)
                parquet_file_paths = glob.glob(path + '/*.parquet')
                logging.info(f"Found {len(parquet_file_paths)} partitions")
                pf = ParquetFile(parquet_file_paths)
                discovered_tables.append((dataset_name, path))
                for col in pf.columns:
                    discovered_columns.append((col, get_col_type(pf, col), dataset_name))
        except:
            logging.error(f"Error while processing path: {path}")
            traceback.print_exc()

    logging.info("Finished processing paths")

    if not discovered_tables:
        logging.info("No Parquet datasets were found")
        return None

    # Write CSVs
    with open(output_parquet_tables_csv_path, 'w', newline='') as csv_tables_file:
        csv_tables_writer = csv.writer(csv_tables_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Header
        csv_tables_writer.writerow(['database', 'cluster', 'schema', 'name', 'description', 'tags', 'is_view', 'description_source'])
        # Data
        for parquet_table in discovered_tables:
            database = database_name
            cluster = cluster_name
            schema = 'parquet'
            name = parquet_table[0]  # dataset_name
            description = parquet_table[1]  # path
            tags = ''
            is_view = 'false'
            description_source = ''
            csv_tables_writer.writerow([database, cluster, schema, name, description, tags, is_view, description_source])
    with open(output_parquet_columns_csv_path, 'w', newline='') as csv_columns_file:
        csv_columns_writer = csv.writer(csv_columns_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Header
        csv_columns_writer.writerow(['name', 'description', 'col_type', 'sort_order', 'database', 'cluster', 'schema', 'table_name'])
        # Data
        for parquet_column in discovered_columns:
            name = parquet_column[0]  # col
            description = ''
            col_type = parquet_column[1]  # get_col_type(pf, col)
            sort_order = 1
            database = database_name
            cluster = cluster_name
            schema = 'parquet'
            table_name = parquet_column[2]  # dataset_name
            csv_columns_writer.writerow([name, description, col_type, sort_order, database, cluster, schema, table_name])

    # Read the produced CSVs and insert them to Neo4j
    tmp_folder = '/var/tmp/amundsen/table_column'
    node_files_folder = '{tmp_folder}/nodes'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships'.format(tmp_folder=tmp_folder)
    extractor = CsvTableColumnExtractor()
    csv_loader = FsNeo4jCSVLoader()
    task = DefaultTask(extractor,
                       loader=csv_loader,
                       transformer=NoopTransformer())
    job_config = ConfigFactory.from_dict({
        'extractor.csvtablecolumn.table_file_location': output_parquet_tables_csv_path,
        'extractor.csvtablecolumn.column_file_location': output_parquet_columns_csv_path,
        'loader.filesystem_csv_neo4j.node_dir_path': node_files_folder,
        'loader.filesystem_csv_neo4j.relationship_dir_path': relationship_files_folder,
        'loader.filesystem_csv_neo4j.delete_created_directories': True,
        'publisher.neo4j.node_files_directory': node_files_folder,
        'publisher.neo4j.relation_files_directory': relationship_files_folder,
        'publisher.neo4j.neo4j_endpoint': neo4j_endpoint,
        'publisher.neo4j.neo4j_user': neo4j_user,
        'publisher.neo4j.neo4j_password': neo4j_password,
        'publisher.neo4j.neo4j_encrypted': False,
        'publisher.neo4j.job_publish_tag': 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=Neo4jCsvPublisher())
    return job


def create_es_publisher_job(elasticsearch_client, 
                            neo4j_endpoint='bolt://localhost:7687', 
                            neo4j_user='neo4j', 
                            neo4j_password='test', 
                            elasticsearch_index_alias='table_search_index',
                            elasticsearch_doc_type_key='table',
                            model_name='databuilder.models.table_elasticsearch_document.TableESDocument',
                            cypher_query=None,
                            elasticsearch_mapping=None):
    """
    :param elasticsearch_index_alias:  alias for Elasticsearch used in
                                       amundsensearchlibrary/search_service/config.py as an index
    :param elasticsearch_doc_type_key: name the ElasticSearch index is prepended with. Defaults to `table` resulting in
                                       `table_search_index`
    :param model_name:                 the Databuilder model class used in transporting between Extractor and Loader
    :param cypher_query:               Query handed to the `Neo4jSearchDataExtractor` class, if None is given (default)
                                       it uses the `Table` query baked into the Extractor
    :param elasticsearch_mapping:      Elasticsearch field mapping "DDL" handed to the `ElasticsearchPublisher` class,
                                       if None is given (default) it uses the `Table` query baked into the Publisher
    """
    # loader saves data to this location and publisher reads it from here
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    task = DefaultTask(loader=FSElasticsearchJSONLoader(),
                       extractor=Neo4jSearchDataExtractor(),
                       transformer=NoopTransformer())

    # elastic search client instance
    # elasticsearch_client = es
    # unique name of new index in Elasticsearch
    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.GRAPH_URL_CONFIG_KEY): neo4j_endpoint,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_USER): neo4j_user,
        'extractor.search_data.extractor.neo4j.{}'.format(Neo4jExtractor.NEO4J_AUTH_PW): neo4j_password,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY):
            elasticsearch_doc_type_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY):
            elasticsearch_index_alias,
    })

    # only optionally add these keys, so need to dynamically `put` them
    if cypher_query:
        job_config.put('extractor.search_data.{}'.format(Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY),
                       cypher_query)
    if elasticsearch_mapping:
        job_config.put('publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY),
                       elasticsearch_mapping)

    job = DefaultJob(conf=job_config,
                     task=task,
                     publisher=ElasticsearchPublisher())
    return job


def init_argparse():
    _parser = argparse.ArgumentParser(description="Coinfirm Amundsen Parquets scanner")
    _parser.add_argument("--elasticsearch_url", help="Elasticsearch URL, ex. 'localhost' or 'https://my-host.net' or 'localhost:9200', default='localhost'", default="localhost")
    _parser.add_argument("--neo4j_url", help="Neo4J URL, ex. 'bolt://my-server:7687', default='bolt://localhost:7687'", default="bolt://localhost:7687")
    _parser.add_argument("--neo4j_username", help="Neo4J username, default='neo4j'", default="neo4j")
    _parser.add_argument("--neo4j_password", help="Neo4J password, default='test'", default="test")
    _parser.add_argument("amundsen_database_name",
                         help="Amundsen database name to be assigned for the discovered files, "
                              "ex. 'mapr', 'local'")
    _parser.add_argument("amundsen_cluster_name",
                         help="Amundsen cluster name to be assigned for the discovered files, "
                              "ex. 'arizona.waw.coinfirm.io', 'analytics_db', 'test', 'prod'")
    _parser.add_argument("target_path",
                         help="One or more paths to parquet data, ex. '/my/data_parquets' folder containing multiple "
                              "partitions of the same parquet dataset or '/my/single_parquet_file.parquet'", nargs='+')
    return _parser


if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

    # Input args
    parser = init_argparse()
    args = parser.parse_args()

    # Neo4j and Elasticsearch config
    es = Elasticsearch([{'host': args.elasticsearch_url}, ], timeout=30, max_retries=10, retry_on_timeout=True)
    DB_FILE = '/tmp/test.db'
    Base = declarative_base()
    NEO4J_ENDPOINT = args.neo4j_url  # Ex. bolt://localhost:7687
    neo4j_endpoint = NEO4J_ENDPOINT
    neo4j_user = args.neo4j_username
    neo4j_password = args.neo4j_password

    # Launch jobs
    logging.info("Running Parquets scanner job")
    parquets_job = create_parquets_scanner_job(args.amundsen_database_name, args.amundsen_cluster_name, args.target_path)
    if parquets_job is not None:
        parquets_job.launch()

        logging.info("Running Elasticsearch publisher job")
        es_publisher_job = create_es_publisher_job(es, neo4j_endpoint, neo4j_user, neo4j_password)
        es_publisher_job.launch()
