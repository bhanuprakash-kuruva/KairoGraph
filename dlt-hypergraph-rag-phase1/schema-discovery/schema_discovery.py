
"""
Comprehensive Schema Discovery Module for PostgreSQL
Discovers tables, columns, primary keys, foreign keys, indexes, stored procedures, and functions
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import __version__ as sa_version

import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SchemaDiscovery:
    """Main class for discovering database schema information"""
    
    def __init__(self, database_url: str):
        """Initialize with database connection URL"""
        self.database_url = database_url
        self.engine: Optional[Engine] = None
        self.inspector = None
        self.schema_info = {
            "database_info": {},
            "tables": {},
            "views": {},
            "functions": [],
            "procedures": [],
            "sequences": [],
            "enums": [],
            "discovered_at": None
        }
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.engine = create_engine(
                self.database_url,
                echo=False,
                pool_pre_ping=True
            )
            self.engine.connect()
            self.inspector = inspect(self.engine)
            logger.info(f"Successfully connected to database: {self.database_url}")
            
            # Get database info
            self._get_database_info()
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def _get_database_info(self):
        """Get basic database information"""
        with self.engine.connect() as conn:
            # PostgreSQL version
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            
            # Current database
            result = conn.execute(text("SELECT current_database()"))
            db_name = result.scalar()
            
            self.schema_info["database_info"] = {
                "name": db_name,
                "version": version,
                "connection_url": self.database_url.split('@')[-1] if '@' in self.database_url else self.database_url
            }
    
    def discover_tables(self) -> Dict[str, Any]:
        """Discover all tables with their columns, constraints, and indexes"""
        tables_info = {}
        
        for table_name in self.inspector.get_table_names():
            if config.EXCLUDE_SCHEMAS and any(schema in table_name for schema in config.EXCLUDE_SCHEMAS):
                continue
            
            logger.info(f"Discovering table: {table_name}")
            
            # Get columns
            columns = []
            for column in self.inspector.get_columns(table_name):
                columns.append({
                    "name": column["name"],
                    "type": str(column["type"]),
                    "nullable": column["nullable"],
                    "default": str(column["default"]) if column["default"] else None,
                    "autoincrement": column.get("autoincrement", False)
                })
            
            # Get primary keys
            pk_constraint = self.inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get("constrained_columns", [])
            
            # Get foreign keys
            foreign_keys = []
            for fk in self.inspector.get_foreign_keys(table_name):
                foreign_keys.append({
                    "name": fk["name"],
                    "constrained_columns": fk["constrained_columns"],
                    "referred_table": fk["referred_table"],
                    "referred_columns": fk["referred_columns"]
                })
            
            # Get indexes
            indexes = []
            for idx in self.inspector.get_indexes(table_name):
                indexes.append({
                    "name": idx["name"],
                    "columns": idx["column_names"],
                    "unique": idx["unique"],
                    "primary_key": idx.get("primary_key", False)
                })
            
            # Get check constraints
            check_constraints = []
            if hasattr(self.inspector, 'get_check_constraints'):
                check_constraints = self.inspector.get_check_constraints(table_name)
            
            tables_info[table_name] = {
                "name": table_name,
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
                "indexes": indexes,
                "check_constraints": check_constraints,
                "total_columns": len(columns)
            }
        
        self.schema_info["tables"] = tables_info
        return tables_info
    
    def discover_views(self) -> Dict[str, Any]:
        """Discover all views in the database"""
        views_info = {}
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT table_name, view_definition 
                    FROM information_schema.views 
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                """))
                
                for row in result:
                    view_name = row[0]
                    view_definition = row[1] if len(row) > 1 else None
                    
                    views_info[view_name] = {
                        "name": view_name,
                        "definition": view_definition[:500] if view_definition else None,  # Truncate
                        "columns": self.inspector.get_columns(view_name) if view_name in self.inspector.get_view_names() else []
                    }
        except Exception as e:
            logger.warning(f"Could not discover views: {e}")
        
        self.schema_info["views"] = views_info
        return views_info
    
    def discover_functions_and_procedures(self) -> Dict[str, Any]:
        """Discover stored procedures and functions"""
        functions = []
        procedures = []
        
        try:
            with self.engine.connect() as conn:
                # Fixed query to exclude aggregate functions
                result = conn.execute(text("""
                    SELECT 
                        n.nspname as schema,
                        p.proname as name,
                        pg_get_functiondef(p.oid) as definition,
                        pg_get_function_arguments(p.oid) as arguments,
                        pg_get_function_result(p.oid) as return_type,
                        CASE 
                            WHEN p.prokind = 'f' THEN 'function'
                            WHEN p.prokind = 'p' THEN 'procedure'
                            WHEN p.prokind = 'a' THEN 'aggregate'
                            ELSE 'unknown'
                        END as type
                    FROM pg_proc p
                    LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
                        AND p.prokind IN ('f', 'p')  -- Only functions and procedures, not aggregates
                    ORDER BY n.nspname, p.proname
                """))
                
                for row in result:
                    func_info = {
                        "name": f"{row[0]}.{row[1]}" if row[0] else row[1],
                        "schema": row[0],
                        "function_name": row[1],
                        "arguments": row[3] if len(row) > 3 else '',
                        "return_type": row[4] if len(row) > 4 else '',
                        "type": row[5] if len(row) > 5 else 'function',
                        "definition": row[2][:1000] if row[2] else None  # Truncate long definitions
                    }
                    
                    if func_info["type"] == 'function':
                        functions.append(func_info)
                    elif func_info["type"] == 'procedure':
                        procedures.append(func_info)
                        
        except Exception as e:
            logger.warning(f"Could not discover functions/procedures: {e}")
            # Try alternative query without pg_get_functiondef for older PostgreSQL versions
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT 
                            n.nspname as schema,
                            p.proname as name,
                            pg_get_function_arguments(p.oid) as arguments,
                            CASE 
                                WHEN p.prokind = 'f' THEN 'function'
                                WHEN p.prokind = 'p' THEN 'procedure'
                                ELSE 'unknown'
                            END as type
                        FROM pg_proc p
                        LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
                        WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
                            AND p.prokind IN ('f', 'p')
                        ORDER BY n.nspname, p.proname
                    """))
                    
                    for row in result:
                        func_info = {
                            "name": f"{row[0]}.{row[1]}" if row[0] else row[1],
                            "schema": row[0],
                            "function_name": row[1],
                            "arguments": row[2] if len(row) > 2 else '',
                            "return_type": 'unknown',
                            "type": row[3] if len(row) > 3 else 'function',
                            "definition": None
                        }
                        
                        if func_info["type"] == 'function':
                            functions.append(func_info)
                        elif func_info["type"] == 'procedure':
                            procedures.append(func_info)
            except Exception as e2:
                logger.warning(f"Alternative query also failed: {e2}")
        
        self.schema_info["functions"] = functions
        self.schema_info["procedures"] = procedures
        
        return {"functions": functions, "procedures": procedures}
    
    def discover_sequences(self) -> List[Dict[str, Any]]:
        """Discover sequences in the database"""
        sequences = []
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        sequence_schema,
                        sequence_name,
                        data_type,
                        start_value,
                        minimum_value,
                        maximum_value,
                        increment
                    FROM information_schema.sequences
                    WHERE sequence_schema NOT IN ('information_schema', 'pg_catalog')
                """))
                
                for row in result:
                    sequences.append({
                        "schema": row[0],
                        "name": row[1],
                        "data_type": row[2],
                        "start_value": row[3],
                        "minimum_value": row[4],
                        "maximum_value": row[5],
                        "increment": row[6]
                    })
        except Exception as e:
            logger.warning(f"Could not discover sequences: {e}")
        
        self.schema_info["sequences"] = sequences
        return sequences
    
    def discover_enums(self) -> List[Dict[str, Any]]:
        """Discover enum types"""
        enums = []
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        t.typname as enum_name,
                        e.enumlabel as enum_value
                    FROM pg_type t
                    JOIN pg_enum e ON t.oid = e.enumtypid
                    ORDER BY t.typname, e.enumsortorder
                """))
                
                enum_dict = {}
                for row in result:
                    enum_name = row[0]
                    enum_value = row[1]
                    
                    if enum_name not in enum_dict:
                        enum_dict[enum_name] = []
                    enum_dict[enum_name].append(enum_value)
                
                for name, values in enum_dict.items():
                    enums.append({
                        "name": name,
                        "values": values
                    })
        except Exception as e:
            logger.warning(f"Could not discover enums: {e}")
        
        self.schema_info["enums"] = enums
        return enums
    
    def get_table_statistics(self) -> Dict[str, Any]:
        """Get table statistics (row counts, size, etc.)"""
        statistics = {}
        
        try:
            with self.engine.connect() as conn:
                # Fixed query - use relname instead of tablename
                result = conn.execute(text("""
                    SELECT 
                        schemaname,
                        relname as tablename,
                        n_live_tup as row_count,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) as total_size
                    FROM pg_stat_user_tables
                    ORDER BY n_live_tup DESC
                """))
                
                for row in result:
                    table_name = row[1]
                    statistics[table_name] = {
                        "row_count": row[2],
                        "total_size": row[3],
                        "schema": row[0]
                    }
        except Exception as e:
            logger.warning(f"Could not get table statistics: {e}")
            # Try alternative query without statistics
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("""
                        SELECT 
                            tablename,
                            schemaname
                        FROM pg_tables
                        WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                    """))
                    
                    for row in result:
                        table_name = row[0]
                        statistics[table_name] = {
                            "row_count": "unknown",
                            "total_size": "unknown",
                            "schema": row[1]
                        }
            except Exception as e2:
                logger.warning(f"Alternative query also failed: {e2}")
        
        self.schema_info["table_statistics"] = statistics
        return statistics
    
    def discover_all(self) -> Dict[str, Any]:
        """Run all discovery methods"""
        logger.info("Starting comprehensive schema discovery...")
        
        self.schema_info["discovered_at"] = datetime.now().isoformat()
        
        # Run all discoveries
        self.discover_tables()
        self.discover_views()
        self.discover_functions_and_procedures()
        self.discover_sequences()
        self.discover_enums()
        self.get_table_statistics()
        
        logger.info(f"Schema discovery completed. Found {len(self.schema_info['tables'])} tables, "
                   f"{len(self.schema_info['functions'])} functions, "
                   f"{len(self.schema_info['procedures'])} procedures")
        
        return self.schema_info
    
    def generate_report(self, output_format: str = "json") -> str:
        """Generate a report of the discovered schema"""
        if output_format == "json":
            output_file = config.OUTPUT_DIR / f"schema_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(output_file, 'w') as f:
                json.dump(self.schema_info, f, indent=2, default=str)
            
            logger.info(f"JSON report generated: {output_file}")
            return str(output_file)
        
        elif output_format == "markdown":
            output_file = config.OUTPUT_DIR / f"schema_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"# Database Schema Report\n\n")
                f.write(f"**Generated:** {self.schema_info['discovered_at']}\n\n")
                f.write(f"## Database Information\n\n")
                f.write(f"- **Name:** {self.schema_info['database_info']['name']}\n")
                f.write(f"- **Version:** {self.schema_info['database_info']['version']}\n\n")
                
                f.write(f"## Tables ({len(self.schema_info['tables'])})\n\n")
                for table_name, table_info in self.schema_info['tables'].items():
                    f.write(f"### {table_name}\n\n")
                    f.write(f"- **Columns:** {table_info['total_columns']}\n")
                    f.write(f"- **Primary Keys:** {', '.join(table_info['primary_keys']) if table_info['primary_keys'] else 'None'}\n")
                    f.write(f"- **Foreign Keys:** {len(table_info['foreign_keys'])}\n\n")
                    
                    if table_info['foreign_keys']:
                        f.write("#### Foreign Keys\n\n")
                        for fk in table_info['foreign_keys']:
                            f.write(f"- `{', '.join(fk['constrained_columns'])}` → `{fk['referred_table']}.{', '.join(fk['referred_columns'])}`\n")
                        f.write("\n")
                    
                    f.write("#### Columns\n\n")
                    f.write("| Column | Type | Nullable | Default |\n")
                    f.write("|--------|------|----------|---------|\n")
                    for col in table_info['columns']:
                        default_val = col['default'] if col['default'] else '-'
                        if default_val and len(str(default_val)) > 50:
                            default_val = str(default_val)[:50] + '...'
                        f.write(f"| {col['name']} | {col['type']} | {col['nullable']} | {default_val} |\n")
                    f.write("\n")
                
                if self.schema_info['functions']:
                    f.write(f"## Functions ({len(self.schema_info['functions'])})\n\n")
                    for func in self.schema_info['functions']:
                        f.write(f"### {func['name']}\n\n")
                        f.write(f"- **Arguments:** {func['arguments']}\n")
                        f.write(f"- **Return Type:** {func['return_type']}\n")
                        if func['definition']:
                            f.write(f"- **Definition:**\n```sql\n{func['definition']}\n```\n\n")
                
                if self.schema_info['procedures']:
                    f.write(f"## Procedures ({len(self.schema_info['procedures'])})\n\n")
                    for proc in self.schema_info['procedures']:
                        f.write(f"### {proc['name']}\n\n")
                        f.write(f"- **Arguments:** {proc['arguments']}\n")
                        if proc['definition']:
                            f.write(f"- **Definition:**\n```sql\n{proc['definition']}\n```\n\n")
            
            logger.info(f"Markdown report generated: {output_file}")
            return str(output_file)
        
        else:
            raise ValueError(f"Unsupported output format: {output_format}")


def main():
    """Main function to run schema discovery"""
    discovery = SchemaDiscovery(config.DATABASE_URL)
    
    if discovery.connect():
        discovery.discover_all()
        
        # Generate reports
        json_file = discovery.generate_report("json")
        md_file = discovery.generate_report("markdown")
        
        print(f"\n✅ Schema Discovery Complete!")
        print(f"📄 JSON Report: {json_file}")
        print(f"📝 Markdown Report: {md_file}")
        
        # Print summary
        print("\n📊 Summary:")
        print(f"   - Tables: {len(discovery.schema_info['tables'])}")
        print(f"   - Functions: {len(discovery.schema_info['functions'])}")
        print(f"   - Procedures: {len(discovery.schema_info['procedures'])}")
        print(f"   - Views: {len(discovery.schema_info['views'])}")
        print(f"   - Sequences: {len(discovery.schema_info['sequences'])}")
        print(f"   - Enums: {len(discovery.schema_info['enums'])}")
        
        # Print tables with foreign keys
        print("\n🔗 Tables with Foreign Keys:")
        for table_name, table_info in discovery.schema_info['tables'].items():
            if table_info['foreign_keys']:
                print(f"   - {table_name}: {len(table_info['foreign_keys'])} FK(s)")
        
        # Print functions
        if discovery.schema_info['functions']:
            print("\n📦 Functions:")
            for func in discovery.schema_info['functions']:
                print(f"   - {func['name']}({func['arguments']}) → {func['return_type']}")
    else:
        print("❌ Failed to connect to database. Please check your connection settings.")


if __name__ == "__main__":
    main()
