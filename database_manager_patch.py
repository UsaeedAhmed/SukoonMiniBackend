#!/usr/bin/env python3

import os

# Read the original file
with open('database_manager.py', 'r') as file:
    content = file.read()

# Replace the database path in the __init__ method
if 'def __init__(self, db_path: str = "smart_home_energy.db"):' in content:
    content = content.replace(
        'def __init__(self, db_path: str = "smart_home_energy.db"):',
        'def __init__(self, db_path: str = "/data/smart_home_energy.db"):'
    )
else:
    print("WARNING: Could not find the target line in database_manager.py")

# Add more debug information to the connection method
if 'def _get_connection' in content:
    connection_method = '''
    def _get_connection(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """
        Get a connection to the SQLite database.
        
        Returns:
            Tuple of (connection, cursor)
        """
        try:
            print(f"Attempting to connect to database at: {self.db_path}")
            print(f"Database file exists: {os.path.exists(self.db_path)}")
            print(f"Database file directory is writable: {os.access(os.path.dirname(self.db_path), os.W_OK)}")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Try to connect
            conn = sqlite3.connect(self.db_path)
            
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            # Return dictionary-like results
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            print(f"Successfully connected to database: {self.db_path}")
            return conn, cursor
        except Exception as e:
            print(f"Error connecting to database: {e}")
            raise
    '''
    content = content.replace('    def _get_connection(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:', 
                              '    def _get_connection(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:')
    content = content.replace('        """', '        """')
    content = content.replace('        conn = sqlite3.connect(self.db_path)', 
                              '        # Try to connect\n        conn = sqlite3.connect(self.db_path)')

# Write the modified content back
with open('database_manager.py', 'w') as file:
    file.write(content)

print("database_manager.py has been patched successfully!")
