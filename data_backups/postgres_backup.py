#!/usr/bin/env python
# coding:utf-8

from typing import List, Optional, Tuple
from datetime import datetime, timezone
from psycopg2 import connect as psycopg2Connect, sql as psycopg2Sql


from interfaces.data_backup import IDataBackup





class PostgresBackupService(IDataBackup):
    """PostgreSQL backup service implementation for any table"""
    
    Name = "PostgresBackupService"
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config: object, logger: object, name: Optional[str] = None):
        super().__init__(config, logger, name)
        
        from src import PROJECT_NAME
        self.schema_name = PROJECT_NAME
        self._connection = None
        self.db_config = {}
        self._setup_connection()
        self._ensure_backup_tables_exist()
    
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
            self.logger.debug("{0} : PostgreSQL backup connection configured for schema {1}".format(
                self.Name, self.schema_name))
        except Exception as e:
            self.logger.error("{0} : failed to setup backup connection - {1}".format(self.Name, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def _ensure_backup_tables_exist(self) -> None:
        """Ensure backup tables exist with proper structure"""
        conn = self.get_connection()
        if conn is None:
            return
            
        try:
            with conn.cursor() as cursor:
                # Get all tables in our schema that don't end with _backup
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                      AND table_type = 'BASE TABLE'
                      AND table_name NOT LIKE '%%_backup'
                """, (self.schema_name,))

                rows = cursor.fetchall()
                if not rows:
                    self.logger.debug("{0} : no tables found in schema {1}".format(
                        self.Name, self.schema_name))
                    return
                
                tables = [row[0] for row in rows]
                for table in tables:
                    self._create_backup_table_if_not_exists(cursor, table)
                    
                conn.commit()
                self.logger.debug("{0} : ensured backup tables for {1} tables".format(
                    self.Name, len(tables)))                 
                    
        except Exception as e:
            self.logger.error("{0} : failed to ensure backup tables - {1}".format(self.Name, str(e)))
            if conn:
                conn.rollback()
    
    #-----------------------------------------------------------------------------------------------
    def get_connection(self):
        """Get database connection"""
        try:
            if self._connection is None or self._connection.closed:
                self._connection = psycopg2Connect(**self.db_config)
                # Set schema search path
                with self._connection.cursor() as cursor:
                    cursor.execute("SET search_path TO %s", (self.schema_name,))
            return self._connection
        except Exception as e:
            self.logger.error("{0} : failed to get backup connection - {1}".format(self.Name, str(e)))
            return None
    
    #-----------------------------------------------------------------------------------------------
    def backup_data(self, source: str) -> Tuple[bool, Optional[str]]:
        """Backup PostgreSQL table using append-only operations"""
        conn = self.get_connection()
        if conn is None:
            return False, "No database connection"
        
        try:
            with conn.cursor() as cursor:
                # Check if source table exists
                if not self._table_exists(source):
                    return False, f"Source table {source} does not exist"
                
                # Ensure backup table exists
                self._create_backup_table_if_not_exists(cursor, source)
                
                # Execute append-only backup operation
                self._execute_backup_operation(cursor, source)
                
                conn.commit()
                self.logger.info("{0} : backup completed for {1}".format(self.Name, source))
                return True, None
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup failed for {1} - {2}".format(self.Name, source, error_msg))
            if conn:
                conn.rollback()
            return False, error_msg
    
    #-----------------------------------------------------------------------------------------------
    def backup_all(self) -> List[Tuple[str, str]]:
        """Backup all tables in schema using single transaction"""
        conn = self.get_connection()
        if conn is None:
            return [("all", "No database connection")]
        
        errors = []
        
        try:
            with conn.cursor() as cursor:
                # Get all tables in our schema that don't end with _backup
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s 
                      AND table_type = 'BASE TABLE'
                      AND table_name NOT LIKE '%_backup'
                    ORDER BY table_name;
                """, (self.schema_name,))
                
                tables = [row[0] for row in cursor.fetchall()]
                self.logger.info("{0} : starting backup for {1} tables".format(self.Name, len(tables)))
                
                for table in tables:
                    try:
                        # Ensure backup table exists and perform backup
                        self._create_backup_table_if_not_exists(cursor, table)
                        self._execute_backup_operation(cursor, table)
                        self.logger.debug("{0} : backed up table {1}".format(self.Name, table))
                        
                    except Exception as e:
                        error_msg = str(e)
                        errors.append((table, error_msg))
                        self.logger.error("{0} : backup failed for {1} - {2}".format(self.Name, table, error_msg))
                        # Continue with next table
                
                conn.commit()
                
                if errors:
                    self.logger.error("{0} : backup completed with {1} errors".format(self.Name, len(errors)))
                else:
                    self.logger.info("{0} : all backups completed successfully at {1}".format(
                        self.Name, datetime.now(timezone.utc).isoformat()))
                
                return errors
                
        except Exception as e:
            error_msg = str(e)
            self.logger.error("{0} : backup all failed - {1}".format(self.Name, error_msg))
            if conn:
                conn.rollback()
            return [("all", error_msg)]
    
    #-----------------------------------------------------------------------------------------------
    def health_check(self) -> bool:
        """Check if backup service is healthy"""
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
    def _create_backup_table_if_not_exists(self, cursor, source_table: str) -> None:
        """Create backup table if it doesn't exist with proper structure"""
        backup_table = f"{source_table}_backup"
        schema_qualified_source = self._get_schema_qualified_name(source_table)
        schema_qualified_backup = self._get_schema_qualified_name(backup_table)
        
        # Check if backup table already exists
        if self._table_exists(backup_table):
            return
        
        try:
            # Create backup table with same structure as source + backup metadata
            cursor.execute(
                psycopg2Sql.SQL("""
                    CREATE TABLE {backup_table} (
                        LIKE {source_table} INCLUDING ALL,
                        backup_timestamp TIMESTAMPTZ NOT NULL,
                        backup_year INTEGER NOT NULL,
                        backup_week INTEGER NOT NULL
                    )
                """).format(
                    backup_table=schema_qualified_backup,
                    source_table=schema_qualified_source
                )
            )

            cursor.execute(
                psycopg2Sql.SQL("CREATE INDEX IF NOT EXISTS {idx_year_week} ON {backup_table} (backup_year, backup_week)").format(
                    backup_table=schema_qualified_backup,
                    idx_year_week=psycopg2Sql.Identifier(f"idx_{backup_table}_year_week")
                )
            )

            cursor.execute(
                psycopg2Sql.SQL("CREATE INDEX IF NOT EXISTS {idx_timestamp} ON {backup_table} (backup_timestamp)").format(
                    backup_table=schema_qualified_backup,
                    idx_timestamp=psycopg2Sql.Identifier(f"idx_{backup_table}_timestamp")
                )
            )

            
            self.logger.debug("{0} : created backup table {1}".format(self.Name, schema_qualified_backup.as_string(cursor)))
            
        except Exception as e:
            self.logger.error("{0} : failed to create backup table {1} - {2}".format(
                self.Name, backup_table, str(e)))
            raise
    
    #-----------------------------------------------------------------------------------------------
    def _execute_backup_operation(self, cursor, source_table: str) -> None:
        """Execute append-only backup operation for a table"""
        backup_table = f"{source_table}_backup"
        schema_qualified_source = self._get_schema_qualified_name(source_table)
        schema_qualified_backup = self._get_schema_qualified_name(backup_table)
        
        current_timestamp = datetime.now(timezone.utc)
        current_year = current_timestamp.year
        current_week = current_timestamp.isocalendar()[1]  # ISO week number
        
        # Append-only backup operation - never delete previous backups
        cursor.execute(
            psycopg2Sql.SQL("""
                WITH inserted_backups AS (
                    -- Insert current data with backup metadata
                    INSERT INTO {backup_table} 
                    SELECT 
                        *,
                        %s as _backup_timestamp,
                        %s as _backup_year,
                        %s as _backup_week
                    FROM {source_table}
                    RETURNING 1
                )
                SELECT COUNT(*) as inserted_count FROM inserted_backups
            """).format(
                backup_table=schema_qualified_backup,
                source_table=schema_qualified_source
            ),
            (current_timestamp, current_year, current_week)
        )
        
        result = cursor.fetchone()
        inserted_count = result[0] if result else 0
        
        self.logger.debug("{0} : append backup for {1} - inserted: {2}, year: {3}, week: {4}".format(
            self.Name, source_table, inserted_count, current_year, current_week))
    
    #-----------------------------------------------------------------------------------------------
    def _get_table_columns(self, cursor, table_name: str) -> List[str]:
        """Get column names for a table (excluding backup metadata columns)"""
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
            AND column_name NOT IN ('_backup_timestamp', '_backup_year', '_backup_week')
            ORDER BY ordinal_position
        """, (self.schema_name, table_name))
        
        return [row[0] for row in cursor.fetchall()]
