from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import sqltypes
from sqlalchemy import util, sql
from sqlalchemy.engine import reflection
from .base import BaseDialect, MixedBinary
import logging
import sys
from urllib.parse import urlparse, parse_qs


logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG,
                    stream=sys.stdout)
logger = logging.getLogger('securedJDBC')


class SecuredJDBCDialect(BaseDialect, DefaultDialect):
    jdbc_db_name = "secured:ssl"
    jdbc_driver_name = "com.sotero.jdbc.openaccess.OpenAccessDriver"
    colspecs = DefaultDialect.colspecs

    def initialize(self, connection):
        super(SecuredJDBCDialect, self).initialize(connection)

    def create_connect_args(self, url):
        if url is None:
            return
        # dialects expect jdbc url in the form of
        # "jdbc:secured://example.com:19989?CustomProperties=(dataset=X)&user=xxx&password=xxx"
        # restore original url
        s: str = str(url)
        # get jdbc url
        s = s.split("//")[-1].split("@")
        user_pwd = s[0].split(":")
        jdbc_url: str = s[-1].split("?")[0]
        parsed_url = urlparse(s[-1])
        ds = parse_qs(parsed_url.query)
        # raise Exception(ds)

        # add driver information
        if not jdbc_url.startswith("jdbc"):
            jdbc_url = f"jdbc:{self.jdbc_db_name}://{jdbc_url};CustomProperties={ds['CustomProperties'][0]}"
        drivers = []

        kwargs = {
            "jclassname": self.jdbc_driver_name,
            "url": jdbc_url,
            # pass driver args - username and password via JVM System settings
            "driver_args": [user_pwd[0], user_pwd[1]]
        }
        return (), kwargs

    def _driver_kwargs(self):
        return {}

    @reflection.cache
    def get_unique_constraints(
        self, connection, table_name, schema=None, **kw
    ):
        table_oid = self.get_table_oid(
            connection, table_name, schema, info_cache=kw.get("info_cache")
        )

        UNIQUE_SQL = """
            SELECT
                cons.conname as name,
                cons.conkey as key,
                a.attnum as col_num,
                a.attname as col_name
            FROM
                pg_catalog.pg_constraint cons
                join pg_attribute a
                  on cons.conrelid = a.attrelid AND
                    a.attnum = ANY(cons.conkey)
            WHERE
                cons.conrelid = :table_oid AND
                cons.contype = 'u'
        """

        t = sql.text(UNIQUE_SQL).columns(col_name=sqltypes.Unicode)
        c = connection.execute(t, table_oid=table_oid)

        uniques = defaultdict(lambda: defaultdict(dict))
        for row in c.fetchall():
            uc = uniques[row.name]
            uc["key"] = (
                row.key.getArray() if hasattr(row.key, "getArray") else row.key
            )
            uc["cols"][row.col_num] = row.col_name

        return [
            {"name": name, "column_names": [uc["cols"][i] for i in uc["key"]]}
            for name, uc in uniques.items()
        ]


dialect = SecuredJDBCDialect
