
#!/usr/bin/env python3
"""
Simple runner for schema discovery
"""

import sys
import argparse
from schema_discovery import main as standard_discovery
from discover_all_schema import DLTSchemaDiscovery
import config

def main():
    parser = argparse.ArgumentParser(description='Database Schema Discovery Tool')
    parser.add_argument('--type', choices=['standard', 'complete', 'both'], 
                       default='both', help='Type of discovery to run')
    parser.add_argument('--format', choices=['json', 'markdown'], 
                       default='json', help='Output format')
    
    args = parser.parse_args()
    
    print("🔍 Starting Database Schema Discovery...")
    print(f"📊 Target Database: {config.DATABASE_URL}")
    print("-" * 50)
    
    if args.type in ['standard', 'both']:
        print("\n📋 Running Standard Schema Discovery...")
        standard_discovery()
    
    if args.type in ['complete', 'both']:
        print("\n🚀 Running Complete Discovery (with dlt & FK graph)...")
        discovery = DLTSchemaDiscovery(config.DATABASE_URL)
        discovery.generate_complete_report()
    
    print("\n✨ Discovery Complete!")

if __name__ == "__main__":
    main()
