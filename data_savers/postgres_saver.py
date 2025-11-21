#!/usr/bin/env python
# coding:utf-8

from typing import Optional
from datetime import datetime, timezone
import pandas as pd
from psycopg2 import connect as psycopg2Connect, sql as psycopg2Sql

from interfaces.data_saver import IDataSaver




class PostgresDataSaver(IDataSaver):
    """PostgreSQL data saver implementation for any DataFrame"""
    
    Name = "PostgresDataSaver"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        from src import PROJECT_NAME
        
        self.schema_name = PROJECT_NAME  # ← Use PROJECT_NAME as schema name
        self._connection = None
        self._setup_connection()
        self._register_pd_conversion_type()
        self._ensure_schema_exists()  # ← Ensure schema exists on startup
    
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
            self.logger.debug("{0} : PostgreSQL saver connection configured for schema {1}".format(
                self.Name, self.schema_name))
        except Exception as e:
            self.logger.error("{0} : failed to setup saver connection - {1}".format(self.Name, str(e)))
            raise

    #-----------------------------------------------------------------------------------------------
    def _ensure_schema_exists(self) -> None:
        """Ensure the project schema exists in database"""
        conn = self.get_connection()
        if conn is None:
            return
            
        try:
            with conn.cursor() as cursor:
                # Create schema if it doesn't exist
                cursor.execute(
                    psycopg2Sql.SQL("CREATE SCHEMA IF NOT EXISTS {0}").format(
                        psycopg2Sql.Identifier(self.schema_name)
                    )
                )
                conn.commit()
                self.logger.debug("{0} : schema {1} ensured".format(self.Name, self.schema_name))
        except Exception as e:
            self.logger.error("{0} : failed to ensure schema {1} - {2}".format(self.Name, self.schema_name, str(e)))
            if conn:
                conn.rollback()

    #-----------------------------------------------------------------------------------------------
    def _register_pd_conversion_type(self) -> None:
        # (psycopg2.ProgrammingError) can't adapt type 'numpy.int64'
        import numpy as np
        from psycopg2.extensions import register_adapter, AsIs
        
        def adapt_numpy_int64(n):
            return AsIs(int(n))
        
        def adapt_numpy_float64(n):
            return AsIs(float(n))
        
        def adapt_numpy_bool_(b):
            return AsIs(bool(b))
        
        # Register global adapters
        register_adapter(np.int64, adapt_numpy_int64)
        register_adapter(np.int32, adapt_numpy_int64)
        register_adapter(np.float64, adapt_numpy_float64)
        register_adapter(np.float32, adapt_numpy_float64)
        register_adapter(np.bool_, adapt_numpy_bool_)
    
    #-----------------------------------------------------------------------------------------------
    def get_connection(self):
        """Get database connection"""
        try:
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2Connect(**self.db_config)
            return self._connection
        except Exception as e:
            self.logger.error("{0} : failed to get saver connection - {1}".format(self.Name, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def save_data(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data using atomic table swap with PostgreSQL DDL features"""
        conn = self.get_connection()
        if conn is None:
            return False
            
        try:
            # Ensure lowercase column names
            # should be done in scraper 
            # data = data.rename(columns=str.lower)
            
            with conn.cursor() as cursor:
                # Ensure target table exists with correct schema
                self._create_or_update_table(cursor, destination, data)
                
                # Generate unique temp table name
                temp_table = f"temp_{destination}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                
                # Single transaction: create temp, insert data, atomic swap
                self._execute_atomic_swap(cursor, destination, temp_table, data)
                
                conn.commit()
                
                schema_qualified_dest = self._get_schema_qualified_name(destination)
                self.logger.info("{0} : data successfully saved to {1}".format(self.Name, schema_qualified_dest.as_string(cursor)))
                return True
                
        except Exception as e:
            self.logger.error("{0} : failed to save data to {1} - {2}".format(self.Name, destination, str(e)))
            if conn:
                conn.rollback()
            return False
    
    #-----------------------------------------------------------------------------------------------
    def save_with_backup(self, data: pd.DataFrame, destination: str) -> bool:
        """Save data with automatic backup"""
        try:
            # First create backup
            from data_backups.postgres_backup import PostgresBackupService
            data_backup = PostgresBackupService(config=self.config, logger=self.logger)
            
            # Only backup if table exists in our schema
            if self._table_exists(destination):
                success, _ = data_backup.backup_data(destination)
                if not success:
                    self.logger.warning("{0} : backup failed for {1}, but continuing with save".format(self.Name, destination))
            
            # Then save data
            return self.save_data(data, destination)
            
        except Exception as e:
            self.logger.error("{0} : save with backup failed for {1} - {2}".format(self.Name, destination, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def read_data(self, source: str) -> Optional[pd.DataFrame]:
        """Read data from PostgreSQL table in project schema"""
        conn = self.get_connection()
        if conn is None:
            return None
            
        try:
            if not self._table_exists(source):
                self.logger.warning("{0} : table not found in schema {1} - {2}".format(
                    self.Name, self.schema_name, source))
                return None
            
            schema_qualified_source = self._get_schema_qualified_name(source)
            query = psycopg2Sql.SQL("SELECT * FROM {0}").format(schema_qualified_source)
            df = pd.read_sql(query, conn)
            
            self.logger.debug("{0} : read {1} records from {2}".format(
                self.Name, len(df), schema_qualified_source))
            return df
            
        except Exception as e:
            self.logger.error("{0} : failed to read data from {1} - {2}".format(self.Name, source, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if data saver is healthy"""
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
    
    #-----------------------------------------------------------------------------------------------
    # Internal helper methods
    def _get_schema_qualified_name(self, table_name: str) -> psycopg2Sql.Composed:
        """Get schema-qualified table name"""
        return psycopg2Sql.SQL("{0}.{1}").format(
            psycopg2Sql.Identifier(self.schema_name),
            psycopg2Sql.Identifier(table_name)
        )
    
    #-----------------------------------------------------------------------------------------------
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in project schema"""
        conn = self.get_connection()
        if conn is None:
            return False
            
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = %s 
                        AND table_name = %s
                    );
                """, (self.schema_name, table_name))
                return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error("{0} : failed to check table existence for {1}.{2} - {3}".format(
                self.Name, self.schema_name, table_name, str(e)))
            return False
    
    #-----------------------------------------------------------------------------------------------
    def _create_or_update_table(self, cursor, table_name: str, data: pd.DataFrame) -> None:
        """Create table if not exists and synchronize schema"""
        try:
            schema_qualified_table = self._get_schema_qualified_name(table_name)
            
            # Create table if it doesn't exist
            if not self._table_exists(table_name):
                self._create_table_from_dataframe(cursor, table_name, data)
                self.logger.debug("{0} : created new table {1}".format(self.Name, schema_qualified_table.as_string(cursor)))
            else:
                # Synchronize schema for existing table
                self._synchronize_table_schema(cursor, table_name, data)
                
        except Exception as e:
            self.logger.error("{0} : failed to create/update table {1} - {2}".format(self.Name, table_name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def _execute_atomic_swap(self, cursor, destination: str, temp_table: str, data: pd.DataFrame) -> None:
        """Execute atomic table swap in single transaction"""
        schema_qualified_dest = self._get_schema_qualified_name(destination)
        schema_qualified_temp = self._get_schema_qualified_name(temp_table)
        old_table_name = f"old_{temp_table}"
        schema_qualified_old = self._get_schema_qualified_name(old_table_name)
        
        # Single transaction block
        cursor.execute(
            psycopg2Sql.SQL("""
                -- Create temp table with full structure copy (includes permissions, indexes, etc.)
                CREATE TABLE {temp_table} (LIKE {target_table} INCLUDING ALL);
                
                -- Insert data into temp table
                -- Data will be inserted after this transaction
            """).format(
                temp_table=schema_qualified_temp,
                target_table=schema_qualified_dest
            )
        )
        
        # Insert data into temp table
        self._insert_dataframe(cursor, temp_table, data)
        
        # Complete the atomic swap
        cursor.execute(
            psycopg2Sql.SQL("""
                -- Atomic rename operations
                ALTER TABLE {target_table} RENAME TO {old_table};
                ALTER TABLE {temp_table} RENAME TO {target_table_name};
                
                -- Cleanup old table
                DROP TABLE {old_table_cleanup};
            """).format(
                target_table=schema_qualified_dest,
                old_table=psycopg2Sql.Identifier(old_table_name),
                temp_table=schema_qualified_temp,
                target_table_name=psycopg2Sql.Identifier(destination),
                old_table_cleanup=schema_qualified_old
            )
        )
    
    #-----------------------------------------------------------------------------------------------
    def _create_table_from_dataframe(self, cursor, table_name: str, data: pd.DataFrame) -> None:
        """Create table in project schema based on DataFrame structure"""
        try:
            columns_sql = self._generate_table_columns_sql(data)
            schema_qualified_table = self._get_schema_qualified_name(table_name)
            
            create_sql = psycopg2Sql.SQL("CREATE TABLE {table} ({columns})").format(
                table=schema_qualified_table,
                columns=columns_sql
            )
            cursor.execute(create_sql)
            
            self.logger.debug("{0} : created table {1}".format(
                self.Name, schema_qualified_table.as_string(cursor)))        
        except Exception as e:
            self.logger.error("{0} : failed to create table {1} - {2}".format(self.Name, table_name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def _generate_table_columns_sql(self, data: pd.DataFrame) -> psycopg2Sql.Composed:
        """Generate SQL for table columns from DataFrame"""
        columns = []
        
        for col_name, col_type in data.dtypes.items():
            pg_type = self._map_pandas_to_postgres_type(col_type)
            
            # Add NOT NULL constraint if column has no nulls
            # null_constraint = " NOT NULL" if not data[col_name].isnull().any() else ""
            null_constraint = ""
            
            columns.append(psycopg2Sql.SQL('{col_name} {pg_type}{null_constraint}').format(
                col_name=psycopg2Sql.Identifier(col_name),
                pg_type=psycopg2Sql.SQL(pg_type),
                null_constraint=psycopg2Sql.SQL(null_constraint)
            ))
        
        return psycopg2Sql.SQL(", ").join(columns)
    
    #-----------------------------------------------------------------------------------------------
    def _map_pandas_to_postgres_type(self, pandas_type) -> str:
        """Map pandas dtype to PostgreSQL type"""
        if pandas_type in ['int64', 'int32']:
            return 'BIGINT'
        elif pandas_type in ['float64', 'float32']:
            return 'DOUBLE PRECISION'
        elif pandas_type == 'bool':
            return 'BOOLEAN'
        elif pandas_type == 'datetime64[ns]':
            return 'TIMESTAMP'
        elif pandas_type in ['object', 'string']:
            return 'TEXT'
        else:
            return 'TEXT'
    
    #-----------------------------------------------------------------------------------------------
    def _synchronize_table_schema(self, cursor, table_name: str, data: pd.DataFrame) -> None:
        """Add missing columns to existing table"""
        try:
            # Get existing columns
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s
            """, (self.schema_name, table_name))
            
            existing_columns = {row[0] for row in cursor.fetchall()}
            new_columns = set(data.columns) - existing_columns
            
            # Add missing columns
            for column in new_columns:
                pg_type = self._map_pandas_to_postgres_type(data[column].dtype)
                cursor.execute(
                    psycopg2Sql.SQL("ALTER TABLE {table} ADD COLUMN {column} {type}").format(
                        table=self._get_schema_qualified_name(table_name),
                        column=psycopg2Sql.Identifier(column),
                        type=psycopg2Sql.SQL(pg_type)
                    )
                )
                self.logger.debug("{0} : added column {1} to table {2}".format(
                    self.Name, column, table_name))
                    
        except Exception as e:
            self.logger.warning("{0} : failed to synchronize schema for {1} - {2}".format(
                self.Name, table_name, str(e)))
    
    #-----------------------------------------------------------------------------------------------
    def _insert_dataframe(self, cursor, table_name: str, data: pd.DataFrame) -> None:
        """Insert DataFrame data into table in project schema"""
        try:
            if data.empty:
                self.logger.warning("{0} : no data to insert into {1}".format(self.Name, table_name))
                return
                
            records = [tuple(row) for row in data.values]
            columns = [psycopg2Sql.Identifier(col) for col in data.columns]
            
            # Build INSERT statement with schema qualification
            schema_qualified_table = self._get_schema_qualified_name(table_name)
            
            insert_sql = psycopg2Sql.SQL("INSERT INTO {table} ({columns}) VALUES ({placeholders})").format(
                table=schema_qualified_table,
                columns=psycopg2Sql.SQL(", ").join(columns),
                placeholders=psycopg2Sql.SQL(", ").join([psycopg2Sql.Placeholder()] * len(data.columns))
            )
            
            # Execute batch insert
            cursor.executemany(insert_sql, records)
            self.logger.debug("{0} : inserted {1} records into {2}".format(
                self.Name, len(records), table_name))
            
        except Exception as e:
            self.logger.error("{0} : failed to insert data into {1} - {2}".format(self.Name, table_name, str(e)))
            raise