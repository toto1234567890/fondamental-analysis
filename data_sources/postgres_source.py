#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional
import pandas as pd
from psycopg2 import connect as psycopg2Connect, sql as psycopg2Sql


from interfaces.data_source import IDataSource




class PostgresDataSource(IDataSource):
    """PostgreSQL data source implementation for reading data"""
    
    Name = "PostgresDataSource"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        self._connection = None
        self._setup_connection()
    
    #-----------------------------------------------------------------------------------------------
    def _setup_connection(self) -> None:
        """Setup PostgreSQL connection"""
        try:
            self.db_config = {
                'host': self.config.DB_SERVER,
                'database': self.config.DB_NAME,
                'user': self.config.DB_USER,
                'password': self.config.DB_PASSWORD,
                'port': self.config.DB_PORT
            }
        except Exception as e:
            self.logger.error("{0} : failed to setup connection - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def get_connection(self):
        """Get database connection"""
        try:
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2Connect(**self.db_config)
            return self._connection
        except Exception as e:
            self.logger.error("{0} : failed to get connection - {1}".format(self.Name, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def get_data(self, source: str) -> pd.DataFrame:
        """Get data from PostgreSQL table"""
        conn = self.get_connection()
        if conn is None:
            return pd.DataFrame()
            
        try:
            query = psycopg2Sql.SQL("SELECT * FROM {0}").format(psycopg2Sql.Identifier(source))
            df = pd.read_sql(query, conn)
            self.logger.debug("{0} : retrieved data from {1}".format(self.Name, source))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to get data from {1} - {2}".format(self.Name, source, str(e)))
            return pd.DataFrame()
    
    #-----------------------------------------------------------------------------------------------
    def list_sources(self) -> List[str]:
        """List available tables"""
        conn = self.get_connection()
        if conn is None:
            return []
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """)
                return [row[0] for row in cursor.fetchall()]
                
        except Exception as e:
            self.logger.error("{0} : failed to list sources - {1}".format(self.Name, str(e)))
            return []
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if data source is healthy"""
        try:
            conn = self.get_connection()
            if conn is None:
                return False
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception as e:
            self.logger.error("{0} : health check failed - {1}".format(self.Name, str(e)))
            return False