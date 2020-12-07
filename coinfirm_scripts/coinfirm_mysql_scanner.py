# MySQL + SQLAlchemy might require specific libraries:
# sudo apt-get install python3.7-dev
# sudo apt-get install mysql-client 
# sudo apt-get install libsqlclient-dev 
# sudo apt-get install libssl-dev
# pip install mysqlclient

import argparse
import logging
import textwrap
import uuid
from typing import Iterator, Union

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory, ConfigTree
from sqlalchemy.ext.declarative import declarative_base

from databuilder import Scoped
from databuilder.extractor.mysql_metadata_extractor import MysqlMetadataExtractor
from databuilder.extractor.neo4j_extractor import Neo4jExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer


class CoinfirmMysqlMetadataExtractor(MysqlMetadataExtractor):
    SQL_STATEMENT = """
            SELECT
            c.col_name,
            c.col_description,
            c.col_type,
            c.col_sort_order,
            {cluster_source} AS cluster,
            c.schema_name AS "schema",
            c.table_name AS name,
            c.table_description AS description,
            c.is_view
            FROM
            sys.amundsen_information_schema AS c
            {where_clause_suffix}
            ORDER by cluster, "schema", name, col_sort_order ;
        """

    def init(self, conf: ConfigTree) -> None:
        LOGGER = logging.getLogger(__name__)

        conf = conf.with_fallback(CoinfirmMysqlMetadataExtractor.DEFAULT_CONFIG)
        self._cluster = '{}'.format(conf.get_string(CoinfirmMysqlMetadataExtractor.CLUSTER_KEY))

        if conf.get_bool(CoinfirmMysqlMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME):
            cluster_source = "c.table_catalog"
        else:
            cluster_source = "'{}'".format(self._cluster)

        self._database = conf.get_string(CoinfirmMysqlMetadataExtractor.DATABASE_KEY, default='mysql')

        self.sql_stmt = CoinfirmMysqlMetadataExtractor.SQL_STATEMENT.format(
            where_clause_suffix=conf.get_string(CoinfirmMysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY),
            cluster_source=cluster_source
        )

        self._alchemy_extractor = SQLAlchemyExtractor()
        sql_alch_conf = Scoped.get_scoped_conf(conf, self._alchemy_extractor.get_scope())\
            .with_fallback(ConfigFactory.from_dict({SQLAlchemyExtractor.EXTRACT_SQL: self.sql_stmt}))

        self.sql_stmt = sql_alch_conf.get_string(SQLAlchemyExtractor.EXTRACT_SQL)

        LOGGER.info('SQL for mysql metadata: {}'.format(self.sql_stmt))

        self._alchemy_extractor.init(sql_alch_conf)
        self._extract_iter: Union[None, Iterator] = None


def create_mysql_scanner_job(cluster_name, connection_string):
    # Optional WHERE clause:
    where_clause_suffix = "WHERE c.schema_name != 'information_schema'"

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    job_config = ConfigFactory.from_dict({
        'extractor.mysql_metadata.{}'.format(CoinfirmMysqlMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
        'extractor.mysql_metadata.{}'.format(CoinfirmMysqlMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME): False,
        'extractor.mysql_metadata.{}'.format(CoinfirmMysqlMetadataExtractor.CLUSTER_KEY): cluster_name,
        'extractor.mysql_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.NODE_DIR_PATH): node_files_folder,
        'loader.filesystem_csv_neo4j.{}'.format(FsNeo4jCSVLoader.RELATION_DIR_PATH): relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NODE_FILES_DIR): node_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.RELATION_FILES_DIR): relationship_files_folder,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_END_POINT_KEY): neo4j_endpoint,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_USER): neo4j_user,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.NEO4J_PASSWORD): neo4j_password,
        'publisher.neo4j.{}'.format(neo4j_csv_publisher.JOB_PUBLISH_TAG): 'unique_tag',  # should use unique tag here like {ds}
    })
    job = DefaultJob(conf=job_config,
                     task=DefaultTask(extractor=CoinfirmMysqlMetadataExtractor(), loader=FsNeo4jCSVLoader()),
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
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY): extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY): extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY): elasticsearch_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY): elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY): elasticsearch_doc_type_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY): elasticsearch_index_alias,
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
    _parser = argparse.ArgumentParser(description="Coinfirm Amundsen MySQL scanner")
    _parser.add_argument("--elasticsearch_url", help="Elasticsearch URL, ex. 'localhost' or 'https://my-host.net' or 'localhost:9200', default='localhost'", default="localhost")
    _parser.add_argument("--neo4j_url", help="Neo4J URL, ex. 'bolt://my-server:7687', default='bolt://localhost:7687'", default="bolt://localhost:7687")
    _parser.add_argument("--neo4j_username", help="Neo4J username, default='neo4j'", default="neo4j")
    _parser.add_argument("--neo4j_password", help="Neo4J password, default='test'", default="test")
    _parser.add_argument("amundsen_cluster_name", help="Amundsen cluster name to be assigned for the discovered tables, ex. 'analytics_db', 'mysql_test', 'prod'")
    _parser.add_argument("mysql_conn_string", help="MySQL connection string, ex. 'mysql://username:password@127.0.0.1:3306'")
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
    logging.info("Running MySQL scanner job")
    mysql_scanner_job = create_mysql_scanner_job(args.amundsen_cluster_name, args.mysql_conn_string)
    mysql_scanner_job.launch()

    logging.info("Running Elasticsearch publisher job")
    es_publisher_job = create_es_publisher_job(es, neo4j_endpoint, neo4j_user, neo4j_password)
    es_publisher_job.launch()
