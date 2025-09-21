#!/usr/bin/env python3
"""
SQL File Cleanup Script

This script removes hardcoded USE DATABASE and USE SCHEMA statements from all SQL files
in the backend_functions directory to prepare them for dynamic deployment.

Usage:
    python clean_sql_files.py [--dry-run]
"""

import os
import re
import sys
from pathlib import Path
import logging
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SQLCleaner:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.project_root = Path(__file__).resolve().parent.parent
        self.backend_functions_dir = self.project_root / "tenants" / "dbt_williamgrant" / "backend_functions"
        self.backup_dir = self.project_root / "scripts" / "deploy" / "backups"

    def find_sql_files(self) -> List[Path]:
        """Find all SQL files in the backend_functions directory"""
        sql_files = list(self.backend_functions_dir.rglob("*.sql"))
        logger.info(f"Found {len(sql_files)} SQL files")
        return sql_files

    def clean_sql_content(self, content: str) -> str:
        """Remove hardcoded USE DATABASE and USE SCHEMA statements and database/schema qualifications"""
        lines = content.split('\n')
        cleaned_lines = []
        removed_lines = []
        modifications = []

        for line_num, line in enumerate(lines, 1):
            original_line = line

            # Check for USE DATABASE or USE SCHEMA statements
            if re.match(r'^USE\s+DATABASE\s+', line, re.IGNORECASE) or \
               re.match(r'^USE\s+SCHEMA\s+', line, re.IGNORECASE):
                removed_lines.append(f"Line {line_num}: {original_line}")
                continue  # Skip this line

            # Remove database/schema qualifications from object names
            modified_line = original_line
            changes_in_line = []

            # Pattern: APOLLO_WILLIAMGRANT.FORECAST.object_name → FORECAST.object_name
            if 'APOLLO_WILLIAMGRANT.FORECAST.' in modified_line:
                modified_line = modified_line.replace('APOLLO_WILLIAMGRANT.FORECAST.', 'FORECAST.')
                changes_in_line.append('APOLLO_WILLIAMGRANT.FORECAST. → FORECAST.')

            # Pattern: APOLLO_DEVELOPMENT.FORECAST.object_name → FORECAST.object_name
            if 'APOLLO_DEVELOPMENT.FORECAST.' in modified_line:
                modified_line = modified_line.replace('APOLLO_DEVELOPMENT.FORECAST.', 'FORECAST.')
                changes_in_line.append('APOLLO_DEVELOPMENT.FORECAST. → FORECAST.')

            # Pattern: APOLLO_WILLIAMGRANT.MASTER_DATA.object_name → MASTER_DATA.object_name
            if 'APOLLO_WILLIAMGRANT.MASTER_DATA.' in modified_line:
                modified_line = modified_line.replace('APOLLO_WILLIAMGRANT.MASTER_DATA.', 'MASTER_DATA.')
                changes_in_line.append('APOLLO_WILLIAMGRANT.MASTER_DATA. → MASTER_DATA.')

            # Pattern: APOLLO_DEVELOPMENT.MASTER_DATA.object_name → MASTER_DATA.object_name
            if 'APOLLO_DEVELOPMENT.MASTER_DATA.' in modified_line:
                modified_line = modified_line.replace('APOLLO_DEVELOPMENT.MASTER_DATA.', 'MASTER_DATA.')
                changes_in_line.append('APOLLO_DEVELOPMENT.MASTER_DATA. → MASTER_DATA.')

            cleaned_lines.append(modified_line)

            # Track modifications
            if changes_in_line and modified_line != original_line:
                modifications.append(f"Line {line_num}: {', '.join(changes_in_line)}")

        return '\n'.join(cleaned_lines), removed_lines + modifications

    def create_backup(self, file_path: Path) -> Path:
        """Create a backup of the original file"""
        if not self.backup_dir.exists():
            self.backup_dir.mkdir(parents=True)

        # Create a relative path for backup filename
        relative_path = file_path.relative_to(self.project_root)
        backup_filename = str(relative_path).replace('/', '_').replace('\\', '_')
        backup_path = self.backup_dir / f"{backup_filename}.backup"

        # Copy the original file to backup
        import shutil
        shutil.copy2(file_path, backup_path)

        return backup_path

    def clean_file(self, file_path: Path) -> bool:
        """Clean a single SQL file"""
        try:
            logger.info(f"Processing {file_path.relative_to(self.project_root)}")

            # Read the original content
            content = file_path.read_text()

            # Clean the content
            cleaned_content, removed_lines = self.clean_sql_content(content)

            if not removed_lines:
                logger.info(f"  ℹ️ No changes needed for {file_path.name}")
                return True

            # Show what will be cleaned
            logger.info(f"  🧹 Cleaning {len(removed_lines)} items:")
            for removed_line in removed_lines:
                logger.info(f"    {removed_line}")

            if self.dry_run:
                logger.info(f"  🔍 DRY RUN: Would modify {file_path.name}")
                return True

            # Create backup
            backup_path = self.create_backup(file_path)
            logger.info(f"  💾 Backup created: {backup_path}")

            # Write cleaned content
            file_path.write_text(cleaned_content)
            logger.info(f"  ✅ Cleaned {file_path.name}")

            return True

        except Exception as e:
            logger.error(f"  ❌ Error processing {file_path.name}: {str(e)}")
            return False

    def clean_all_files(self) -> bool:
        """Clean all SQL files"""
        logger.info(f"🧹 Starting SQL cleanup (dry_run={self.dry_run})")
        logger.info(f"Target directory: {self.backend_functions_dir}")

        sql_files = self.find_sql_files()

        if not sql_files:
            logger.warning("No SQL files found")
            return True

        success_count = 0
        failure_count = 0

        for sql_file in sql_files:
            if self.clean_file(sql_file):
                success_count += 1
            else:
                failure_count += 1

        # Summary
        logger.info(f"\n📊 Cleanup Summary:")
        logger.info(f"   ✅ Successful: {success_count}")
        logger.info(f"   ❌ Failed: {failure_count}")
        logger.info(f"   📁 Total: {len(sql_files)}")

        if not self.dry_run and success_count > 0:
            logger.info(f"\n💾 Backups stored in: {self.backup_dir}")

        if failure_count > 0:
            logger.error(f"❌ Cleanup completed with {failure_count} failures")
            return False
        else:
            logger.info("🎉 All files cleaned successfully!")
            return True

def main():
    dry_run = "--dry-run" in sys.argv

    try:
        cleaner = SQLCleaner(dry_run=dry_run)
        success = cleaner.clean_all_files()

        if dry_run:
            print("\n🔍 This was a dry run. No files were actually modified.")
            print("Run without --dry-run to apply the changes.")

        sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"❌ Cleanup failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()