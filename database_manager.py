import os
import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Union

class DatabaseManager:
    """
    Manager for the SQLite database operations.
    """
    
    def __init__(self, db_path: str = "smart_home_energy.db"):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._create_database()
    
    def _get_connection(self) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
        """
        Get a connection to the SQLite database.
        
        Returns:
            Tuple of (connection, cursor)
        """
        conn = sqlite3.connect(self.db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        # Return dictionary-like results
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        return conn, cursor
    
    def _create_database(self):
        """Create the database tables if they don't exist."""
        conn, cursor = self._get_connection()
        
        try:
            # Users table (unchanged)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Hubs table (unchanged)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS hubs (
                hub_id TEXT PRIMARY KEY,
                hub_code TEXT UNIQUE,
                user_id TEXT,
                home_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            ''')
            
            # Devices table (unchanged)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                hub_code TEXT,
                device_type TEXT,
                status INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            # Rooms table (new)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS rooms (
                room_id TEXT PRIMARY KEY,
                room_name TEXT,
                hub_code TEXT,
                device_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            # Room Devices mapping table (new)
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS room_devices (
                room_id TEXT,
                device_id TEXT,
                PRIMARY KEY (room_id, device_id),
                FOREIGN KEY (room_id) REFERENCES rooms(room_id) ON DELETE CASCADE,
                FOREIGN KEY (device_id) REFERENCES devices(device_id) ON DELETE CASCADE
            )
            ''')
            
            # Existing energy tables (unchanged)
            # ... (previous energy table creation code remains the same)
            
            # ... existing tables code ...
            
            # Add the missing energy tables
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                user_id TEXT,
                hub_code TEXT,
                device_id TEXT,
                device_type TEXT,
                energy_kwh REAL,
                usage_hours REAL,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_weekly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                week INTEGER,
                user_id TEXT,
                hub_code TEXT,
                device_id TEXT,
                device_type TEXT,
                energy_kwh REAL,
                usage_hours REAL,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_monthly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                month INTEGER,
                user_id TEXT,
                hub_code TEXT,
                device_id TEXT,
                device_type TEXT,
                energy_kwh REAL,
                usage_hours REAL,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_yearly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year INTEGER,
                user_id TEXT,
                hub_code TEXT,
                device_id TEXT,
                device_type TEXT,
                energy_kwh REAL,
                usage_hours REAL,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS hub_summary (
                hub_code TEXT PRIMARY KEY,
                user_id TEXT,
                daily_energy REAL DEFAULT 0,
                weekly_energy REAL DEFAULT 0,
                monthly_energy REAL DEFAULT 0,
                yearly_energy REAL DEFAULT 0,
                device_count INTEGER DEFAULT 0,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (hub_code) REFERENCES hubs(hub_code) ON DELETE CASCADE
            )
            ''')
            
            conn.commit()
            print("Database schema created successfully")
            
        except Exception as e:
            print(f"Error creating database schema: {e}")
            
        finally:
            conn.close()

    def add_hub(self, hub_id: str, hub_code: str, user_id: Optional[str] = None, home_type: str = "") -> bool:
        """
        Add a new hub to the database or update if exists.
        """
        conn, cursor = self._get_connection()
        
        try:
            # If no user_id is provided or it's empty, create a generic one
            if user_id is None or user_id == "":
                user_id = f"user_{hub_code}"
            
            # Ensure the user exists before adding the hub
            self.add_user(user_id)
            
            # Check if hub exists
            cursor.execute("SELECT hub_id FROM hubs WHERE hub_id = ?", (hub_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Update hub
                cursor.execute(
                    """
                    UPDATE hubs 
                    SET hub_code = ?, user_id = ?, home_type = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE hub_id = ?
                    """, 
                    (hub_code, user_id, home_type, hub_id)
                )
            else:
                # Insert new hub
                cursor.execute(
                    """
                    INSERT INTO hubs (hub_id, hub_code, user_id, home_type) 
                    VALUES (?, ?, ?, ?)
                    """,
                    (hub_id, hub_code, user_id, home_type)
                )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error adding hub: {e}")
            
            # Try again with a more lenient approach
            try:
                # Temporarily disable foreign key constraint
                conn.execute("PRAGMA foreign_keys = OFF")
                
                if exists:
                    cursor.execute(
                        """
                        UPDATE hubs 
                        SET hub_code = ?, user_id = ?, home_type = ?, last_updated = CURRENT_TIMESTAMP 
                        WHERE hub_id = ?
                        """, 
                        (hub_code, user_id, home_type, hub_id)
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO hubs (hub_id, hub_code, user_id, home_type) 
                        VALUES (?, ?, ?, ?)
                        """,
                        (hub_id, hub_code, user_id, home_type)
                    )
                
                conn.commit()
                # Re-enable foreign key constraint
                conn.execute("PRAGMA foreign_keys = ON")
                return True
                
            except Exception as e2:
                conn.rollback()
                print(f"Second attempt to add hub failed: {e2}")
                return False
            
        finally:
            conn.close()


    def add_room(
        self, 
        room_id: str, 
        room_name: str, 
        hub_code: str, 
        devices: Optional[List[str]] = None
    ) -> bool:
        """
        Add a new room to the database or update if exists.
        
        Args:
            room_id: Unique identifier for the room
            room_name: Name of the room
            hub_code: Hub code this room belongs to
            devices: Optional list of device IDs in this room
            
        Returns:
            True if successful, False otherwise
        """
        conn, cursor = self._get_connection()
        
        try:
            # Ensure hub exists
            cursor.execute("SELECT hub_code FROM hubs WHERE hub_code = ?", (hub_code,))
            if not cursor.fetchone():
                print(f"Hub {hub_code} does not exist")
                return False
            
            # Insert or replace room record
            cursor.execute(
                """
                INSERT OR REPLACE INTO rooms 
                (room_id, room_name, hub_code, device_count, last_updated) 
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (room_id, room_name, hub_code, len(devices) if devices else 0)
            )
            
            # If devices are provided, update room devices
            if devices:
                # Remove existing device mappings
                cursor.execute(
                    "DELETE FROM room_devices WHERE room_id = ?", 
                    (room_id,)
                )
                
                # Insert new device mappings
                for device_id in devices:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO room_devices 
                        (room_id, device_id) 
                        VALUES (?, ?)
                        """,
                        (room_id, device_id)
                    )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error adding room: {e}")
            return False
            
        finally:
            conn.close()
    
    def get_rooms_by_hub_code(self, hub_code: str) -> List[Dict[str, Any]]:
        """
        Get all rooms for a specific hub with their devices.
        
        Args:
            hub_code: Hub code to filter rooms by
            
        Returns:
            List of room dictionaries with device information
        """
        conn, cursor = self._get_connection()
        
        try:
            # Fetch rooms
            cursor.execute(
                """
                SELECT r.room_id, r.room_name, r.hub_code, r.device_count,
                       GROUP_CONCAT(rd.device_id) as device_ids
                FROM rooms r
                LEFT JOIN room_devices rd ON r.room_id = rd.room_id
                WHERE r.hub_code = ?
                GROUP BY r.room_id, r.room_name, r.hub_code, r.device_count
                """, 
                (hub_code,)
            )
            
            rooms = []
            for row in cursor.fetchall():
                # Get device details
                device_details = []
                device_ids = row['device_ids'].split(',') if row['device_ids'] else []
                
                for device_id in device_ids:
                    cursor.execute(
                        """
                        SELECT device_type 
                        FROM devices 
                        WHERE device_id = ?
                        """, 
                        (device_id,)
                    )
                    device_info = cursor.fetchone()
                    if device_info:
                        device_details.append({
                            "device_type": device_info['device_type']
                        })
                
                room = {
                    "room_id": row['room_id'],
                    "room_name": row['room_name'],
                    "hub_code": row['hub_code'],
                    "device_count": row['device_count'],
                    "devices": device_details
                }
                rooms.append(room)
            
            return rooms
            
        except Exception as e:
            print(f"Error getting rooms: {e}")
            return []
            
        finally:
            conn.close()
    
    def get_rooms_for_hub_energy_data(self, hub_code: str, date_str: Optional[str] = None) -> Dict[str, Any]:
        """
        Get rooms with their energy data for a specific hub and date.
        
        Args:
            hub_code: Hub code to get rooms for
            date_str: Date string in YYYY-MM-DD format (defaults to today)
            
        Returns:
            Dictionary of rooms with their energy data
        """
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        
        conn, cursor = self._get_connection()
        
        try:
            # Fetch rooms with their devices and energy data
            cursor.execute(
                """
                SELECT r.room_id, r.room_name, r.device_count,
                       COALESCE(SUM(ed.energy_kwh), 0) as total_energy
                FROM rooms r
                LEFT JOIN room_devices rd ON r.room_id = rd.room_id
                LEFT JOIN energy_daily ed ON rd.device_id = ed.device_id AND ed.date = ?
                WHERE r.hub_code = ?
                GROUP BY r.room_id, r.room_name, r.device_count
                """, 
                (date_str, hub_code)
            )
            
            rooms_data = {}
            for row in cursor.fetchall():
                # Get detailed device information for this room
                cursor.execute(
                    """
                    SELECT d.device_id, d.device_type, 
                           COALESCE(ed.energy_kwh, 0) as energy_value
                    FROM room_devices rd
                    JOIN devices d ON rd.device_id = d.device_id
                    LEFT JOIN energy_daily ed ON d.device_id = ed.device_id AND ed.date = ?
                    WHERE rd.room_id = ?
                    """,
                    (date_str, row['room_id'])
                )
                
                devices = []
                for device_row in cursor.fetchall():
                    devices.append({
                        "device_type": device_row['device_type']
                    })
                
                rooms_data[row['room_name']] = {
                    "energy_value": row['total_energy'],
                    "unit": "kWh",
                    "device_count": row['device_count'],
                    "devices": devices
                }
            
            return rooms_data
            
        except Exception as e:
            print(f"Error getting rooms energy data: {e}")
            return {}
            
        finally:
            conn.close()
    
    def get_devices_for_hub(self, hub_code: str) -> List[Dict[str, Any]]:
        """
        Get all devices for a specific hub.
        
        Args:
            hub_code: Hub code to get devices for
            
        Returns:
            List of device dictionaries
        """
        conn, cursor = self._get_connection()
        
        try:
            cursor.execute(
                """
                SELECT * FROM devices 
                WHERE hub_code = ?
                """,
                (hub_code,)
            )
            
            devices = []
            for row in cursor.fetchall():
                device = dict(row)
                # Convert status to boolean
                device['status'] = bool(device['status'])
                devices.append(device)
                
            return devices
            
        except Exception as e:
            print(f"Error getting devices for hub: {e}")
            return []
            
        finally:
            conn.close()
    
    def get_energy_summary(self, user_id: str) -> Dict[str, Any]:
        """
        Get energy summary for a user across all time periods.
        
        Args:
            user_id: User ID to get summary for
            
        Returns:
            Dictionary with energy summary data
        """
        conn, cursor = self._get_connection()
        
        try:
            # Get current date components
            now = datetime.now()
            current_date = now.strftime("%Y-%m-%d")
            current_year = now.year
            current_month = now.month
            
            # Calculate week number (1-52)
            current_week = int(now.strftime("%U"))
            
            # Get daily totals
            cursor.execute(
                """
                SELECT SUM(energy_kwh) as daily_total
                FROM energy_daily
                WHERE user_id = ? AND date = ? AND device_id IS NULL
                """,
                (user_id, current_date)
            )
            daily_result = cursor.fetchone()
            daily_total = daily_result['daily_total'] if daily_result and daily_result['daily_total'] else 0.0
            
            # Get weekly totals
            cursor.execute(
                """
                SELECT SUM(energy_kwh) as weekly_total
                FROM energy_weekly
                WHERE user_id = ? AND year = ? AND week = ?
                AND device_id IS NULL
                """,
                (user_id, current_year, current_week)
            )
            weekly_result = cursor.fetchone()
            weekly_total = weekly_result['weekly_total'] if weekly_result and weekly_result['weekly_total'] else 0.0
            
            # Get monthly totals
            cursor.execute(
                """
                SELECT SUM(energy_kwh) as monthly_total
                FROM energy_monthly
                WHERE user_id = ? AND year = ? AND month = ?
                AND device_id IS NULL
                """,
                (user_id, current_year, current_month)
            )
            monthly_result = cursor.fetchone()
            monthly_total = monthly_result['monthly_total'] if monthly_result and monthly_result['monthly_total'] else 0.0
            
            # Get yearly totals
            cursor.execute(
                """
                SELECT SUM(energy_kwh) as yearly_total
                FROM energy_yearly
                WHERE user_id = ? AND year = ?
                AND device_id IS NULL
                """,
                (user_id, current_year)
            )
            yearly_result = cursor.fetchone()
            yearly_total = yearly_result['yearly_total'] if yearly_result and yearly_result['yearly_total'] else 0.0
            
            # Get user hubs
            cursor.execute(
                """
                SELECT hub_code, home_type
                FROM hubs
                WHERE user_id = ?
                """,
                (user_id,)
            )
            
            hubs = []
            for row in cursor.fetchall():
                hubs.append({
                    'hub_code': row['hub_code'],
                    'home_type': row['home_type']
                })
            
            return {
                'user_id': user_id,
                'date': current_date,
                'week': current_week,
                'month': current_month,
                'year': current_year,
                'daily_total': daily_total,
                'weekly_total': weekly_total,
                'monthly_total': monthly_total,
                'yearly_total': yearly_total,
                'unit': 'kWh',
                'hubs': hubs,
                'hub_count': len(hubs)
            }
            
        except Exception as e:
            print(f"Error getting energy summary: {e}")
            return {}
            
        finally:
            conn.close()
    
    def get_top_consumers(
        self, 
        user_id: str, 
        time_period: str = 'daily',
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get top energy consuming devices for a user.
        
        Args:
            user_id: User ID to get data for
            time_period: One of 'daily', 'weekly', 'monthly', 'yearly'
            limit: Number of top devices to return
            
        Returns:
            List of dictionaries with top device data
        """
        conn, cursor = self._get_connection()
        
        try:
            # Get current date components
            now = datetime.now()
            current_date = now.strftime("%Y-%m-%d")
            current_year = now.year
            current_month = now.month
            current_week = int(now.strftime("%U"))
            
            if time_period == 'daily':
                query = """
                SELECT ed.device_id, ed.device_type, ed.hub_code, ed.energy_kwh,
                       d.status, h.home_type
                FROM energy_daily ed
                JOIN devices d ON ed.device_id = d.device_id
                JOIN hubs h ON ed.hub_code = h.hub_code
                WHERE ed.user_id = ? AND ed.date = ? AND ed.device_id IS NOT NULL
                ORDER BY ed.energy_kwh DESC
                LIMIT ?
                """
                params = (user_id, current_date, limit)
                
            elif time_period == 'weekly':
                query = """
                SELECT ew.device_id, ew.device_type, ew.hub_code, ew.energy_kwh,
                       d.status, h.home_type
                FROM energy_weekly ew
                JOIN devices d ON ew.device_id = d.device_id
                JOIN hubs h ON ew.hub_code = h.hub_code
                WHERE ew.user_id = ? AND ew.year = ? AND ew.week = ? 
                AND ew.device_id IS NOT NULL
                ORDER BY ew.energy_kwh DESC
                LIMIT ?
                """
                params = (user_id, current_year, current_week, limit)
                
            elif time_period == 'monthly':
                query = """
                SELECT em.device_id, em.device_type, em.hub_code, em.energy_kwh,
                       d.status, h.home_type
                FROM energy_monthly em
                JOIN devices d ON em.device_id = d.device_id
                JOIN hubs h ON em.hub_code = h.hub_code
                WHERE em.user_id = ? AND em.year = ? AND em.month = ? 
                AND em.device_id IS NOT NULL
                ORDER BY em.energy_kwh DESC
                LIMIT ?
                """
                params = (user_id, current_year, current_month, limit)
                
            elif time_period == 'yearly':
                query = """
                SELECT ey.device_id, ey.device_type, ey.hub_code, ey.energy_kwh,
                       d.status, h.home_type
                FROM energy_yearly ey
                JOIN devices d ON ey.device_id = d.device_id
                JOIN hubs h ON ey.hub_code = h.hub_code
                WHERE ey.user_id = ? AND ey.year = ? 
                AND ey.device_id IS NOT NULL
                ORDER BY ey.energy_kwh DESC
                LIMIT ?
                """
                params = (user_id, current_year, limit)
                
            else:
                return []
            
            cursor.execute(query, params)
            
            result = []
            for row in cursor.fetchall():
                device = dict(row)
                device['status'] = bool(device['status'])
                device['unit'] = 'kWh'
                result.append(device)
                
            return result
            
        except Exception as e:
            print(f"Error getting top consumers: {e}")
            return []
            
        finally:
            conn.close()
    def add_user(self, user_id: str) -> bool:
        """
        Add a new user to the database or update if exists.
        
        Args:
            user_id: Unique identifier for the user
            
        Returns:
            True if successful, False otherwise
        """
        conn, cursor = self._get_connection()
        
        try:
            # Check if user exists
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone()
            
            if exists:
                # Update last_updated timestamp
                cursor.execute(
                    "UPDATE users SET last_updated = CURRENT_TIMESTAMP WHERE user_id = ?", 
                    (user_id,)
                )
            else:
                # Insert new user
                cursor.execute(
                    "INSERT INTO users (user_id) VALUES (?)",
                    (user_id,)
                )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error adding user: {e}")
            return False
            
        finally:
            conn.close()


    def store_hub_daily_total(
    self, 
    date_str: str, 
    user_id: str, 
    hub_code: str, 
    energy_kwh: float,
    usage_hours: float = 24.0
) -> bool:
        """
        Store daily energy consumption for a hub (aggregated).
        """
        conn, cursor = self._get_connection()
        
        try:
            # Ensure user exists
            self.add_user(user_id)
            
            # Insert or replace hub daily energy record
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_daily 
                (date, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours) 
                VALUES (?, ?, ?, NULL, 'hub_total', ?, ?)
                """,
                (date_str, user_id, hub_code, energy_kwh, usage_hours)
            )
            
            # Update hub summary table
            cursor.execute(
                """
                INSERT OR REPLACE INTO hub_summary 
                (hub_code, user_id, daily_energy, last_updated) 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (hub_code, user_id, energy_kwh)
            )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error storing hub daily total: {e}")
            
            # Try again with a more lenient approach
            try:
                # Temporarily disable foreign key constraint
                conn.execute("PRAGMA foreign_keys = OFF")
                
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO energy_daily 
                    (date, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours) 
                    VALUES (?, ?, ?, NULL, 'hub_total', ?, ?)
                    """,
                    (date_str, user_id, hub_code, energy_kwh, usage_hours)
                )
                
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO hub_summary 
                    (hub_code, user_id, daily_energy, last_updated) 
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (hub_code, user_id, energy_kwh)
                )
                
                conn.commit()
                # Re-enable foreign key constraint
                conn.execute("PRAGMA foreign_keys = ON")
                return True
                
            except Exception as e2:
                conn.rollback()
                print(f"Second attempt to store hub daily total failed: {e2}")
                return False
            
        finally:
            conn.close()



    def add_device(self, device_id: str, hub_code: str, device_type: str, status: bool = False) -> bool:
        """
        Add a new device to the database or update if exists.
        
        Args:
            device_id: Unique identifier for the device
            hub_code: Hub code this device belongs to
            device_type: Type of device (light, fan, etc.)
            status: Whether the device is on (True) or off (False)
            
        Returns:
            True if successful, False otherwise
        """
        conn, cursor = self._get_connection()
        
        try:
            # Check if device exists
            cursor.execute("SELECT device_id FROM devices WHERE device_id = ?", (device_id,))
            exists = cursor.fetchone()
            
            status_int = 1 if status else 0
            
            if exists:
                # Update device
                cursor.execute(
                    """
                    UPDATE devices 
                    SET hub_code = ?, device_type = ?, status = ?, last_updated = CURRENT_TIMESTAMP 
                    WHERE device_id = ?
                    """, 
                    (hub_code, device_type, status_int, device_id)
                )
            else:
                # Insert new device
                cursor.execute(
                    """
                    INSERT INTO devices (device_id, hub_code, device_type, status) 
                    VALUES (?, ?, ?, ?)
                    """,
                    (device_id, hub_code, device_type, status_int)
                )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error adding device: {e}")
            return False
            
        finally:
            conn.close()

    def store_daily_energy(
    self, 
    date_str: str, 
    user_id: str, 
    hub_code: str, 
    device_id: str, 
    device_type: str,
    energy_kwh: float,
    usage_hours: float
) -> bool:
        """
        Store daily energy consumption for a device.
        
        Args:
            date_str: Date string in YYYY-MM-DD format
            user_id: User ID
            hub_code: Hub code
            device_id: Device ID
            device_type: Device type
            energy_kwh: Energy consumption in kWh
            usage_hours: Usage hours
            
        Returns:
            True if successful, False otherwise
        """
        conn, cursor = self._get_connection()
        
        try:
            # Insert or replace daily energy record
            cursor.execute(
                """
                INSERT OR REPLACE INTO energy_daily 
                (date, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (date_str, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours)
            )
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error storing daily energy: {e}")
            return False
            
        finally:
            conn.close()

# # Example usage
# if __name__ == "__main__":
#     db = DatabaseManager()
    
#     # Add a sample user
#     user_id = "user123"
#     db.add_user(user_id)
    
#     # Add a sample hub
#     hub_id = "hub-589"
#     hub_code = "HUB589"
#     db.add_hub(hub_id, hub_code, user_id, "apartment")
    
#     # Add sample devices
#     db.add_device("dev-1001", hub_code, "light", True)
#     db.add_device("dev-1002", hub_code, "fan", False)
    
#     # Store some sample energy data
#     today = datetime.now().strftime("%Y-%m-%d")
    
#     # Store device energy
#     db.store_daily_energy(today, user_id, hub_code, "dev-1001", "light", 0.6, 10)
#     db.store_daily_energy(today, user_id, hub_code, "dev-1002", "fan", 0.3, 10)
    
#     # Store hub total
#     db.store_hub_daily_total(today, user_id, hub_code, 0.9)
    
#     # Get energy data
#     energy_data = db.get_daily_energy_by_hub(hub_code)
#     print(f"Daily energy for hub {hub_code}:")
#     print(json.dumps(energy_data, indent=2))
    
#     # Get energy summary
#     summary = db.get_energy_summary(user_id)
#     print("\nEnergy summary for user:")
#     print(json.dumps(summary, indent=2))
#  e:
#             conn.rollback()
#             print(f"Error creating database schema: {e}")
            
#         finally:
#             conn.close()
    
#     def add_user(self, user_id: str) -> bool:
#         """
#         Add a new user to the database or update if exists.
        
#         Args:
#             user_id: Unique identifier for the user
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Check if user exists
#             cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
#             exists = cursor.fetchone()
            
#             if exists:
#                 # Update last_updated timestamp
#                 cursor.execute(
#                     "UPDATE users SET last_updated = CURRENT_TIMESTAMP WHERE user_id = ?", 
#                     (user_id,)
#                 )
#             else:
#                 # Insert new user
#                 cursor.execute(
#                     "INSERT INTO users (user_id) VALUES (?)",
#                     (user_id,)
#                 )
            
#             conn.commit()
#             return True
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error adding user: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def add_hub(self, hub_id: str, hub_code: str, user_id: str, home_type: str = "") -> bool:
#         """
#         Add a new hub to the database or update if exists.
        
#         Args:
#             hub_id: Unique identifier for the hub
#             hub_code: Hub code used to link devices
#             user_id: User ID that owns this hub
#             home_type: Type of home (apartment, house, etc.)
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Ensure user exists
#             self.add_user(user_id)
            
#             # Check if hub exists
#             cursor.execute("SELECT hub_id FROM hubs WHERE hub_id = ?", (hub_id,))
#             exists = cursor.fetchone()
            
#             if exists:
#                 # Update hub
#                 cursor.execute(
#                     """
#                     UPDATE hubs 
#                     SET hub_code = ?, user_id = ?, home_type = ?, last_updated = CURRENT_TIMESTAMP 
#                     WHERE hub_id = ?
#                     """, 
#                     (hub_code, user_id, home_type, hub_id)
#                 )
#             else:
#                 # Insert new hub
#                 cursor.execute(
#                     """
#                     INSERT INTO hubs (hub_id, hub_code, user_id, home_type) 
#                     VALUES (?, ?, ?, ?)
#                     """,
#                     (hub_id, hub_code, user_id, home_type)
#                 )
            
#             conn.commit()
#             return True
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error adding hub: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def add_device(self, device_id: str, hub_code: str, device_type: str, status: bool = False) -> bool:
#         """
#         Add a new device to the database or update if exists.
        
#         Args:
#             device_id: Unique identifier for the device
#             hub_code: Hub code this device belongs to
#             device_type: Type of device (light, fan, etc.)
#             status: Whether the device is on (True) or off (False)
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Check if device exists
#             cursor.execute("SELECT device_id FROM devices WHERE device_id = ?", (device_id,))
#             exists = cursor.fetchone()
            
#             status_int = 1 if status else 0
            
#             if exists:
#                 # Update device
#                 cursor.execute(
#                     """
#                     UPDATE devices 
#                     SET hub_code = ?, device_type = ?, status = ?, last_updated = CURRENT_TIMESTAMP 
#                     WHERE device_id = ?
#                     """, 
#                     (hub_code, device_type, status_int, device_id)
#                 )
#             else:
#                 # Insert new device
#                 cursor.execute(
#                     """
#                     INSERT INTO devices (device_id, hub_code, device_type, status) 
#                     VALUES (?, ?, ?, ?)
#                     """,
#                     (device_id, hub_code, device_type, status_int)
#                 )
            
#             conn.commit()
#             return True
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error adding device: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def store_daily_energy(
#         self, 
#         date_str: str, 
#         user_id: str, 
#         hub_code: str, 
#         device_id: str, 
#         device_type: str,
#         energy_kwh: float,
#         usage_hours: float
#     ) -> bool:
#         """
#         Store daily energy consumption for a device.
        
#         Args:
#             date_str: Date string in YYYY-MM-DD format
#             user_id: User ID
#             hub_code: Hub code
#             device_id: Device ID
#             device_type: Device type
#             energy_kwh: Energy consumption in kWh
#             usage_hours: Usage hours
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Insert or replace daily energy record
#             cursor.execute(
#                 """
#                 INSERT OR REPLACE INTO energy_daily 
#                 (date, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours) 
#                 VALUES (?, ?, ?, ?, ?, ?, ?)
#                 """,
#                 (date_str, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours)
#             )
            
#             conn.commit()
#             return True
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error storing daily energy: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def store_hub_daily_total(
#         self, 
#         date_str: str, 
#         user_id: str, 
#         hub_code: str, 
#         energy_kwh: float,
#         usage_hours: float = 24.0
#     ) -> bool:
#         """
#         Store daily energy consumption for a hub (aggregated).
        
#         Args:
#             date_str: Date string in YYYY-MM-DD format
#             user_id: User ID
#             hub_code: Hub code
#             energy_kwh: Total energy consumption in kWh
#             usage_hours: Usage hours (default 24)
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Insert or replace hub daily energy record
#             cursor.execute(
#                 """
#                 INSERT OR REPLACE INTO energy_daily 
#                 (date, user_id, hub_code, device_id, device_type, energy_kwh, usage_hours) 
#                 VALUES (?, ?, ?, NULL, 'hub_total', ?, ?)
#                 """,
#                 (date_str, user_id, hub_code, energy_kwh, usage_hours)
#             )
            
#             # Update hub_summary table
#             cursor.execute(
#                 """
#                 INSERT OR REPLACE INTO hub_summary 
#                 (hub_code, user_id, daily_energy, last_updated) 
#                 VALUES (
#                     ?, ?, ?,
#                     CURRENT_TIMESTAMP
#                 )
#                 """,
#                 (hub_code, user_id, energy_kwh)
#             )
            
#             conn.commit()
#             return True
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error storing hub daily total: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def get_daily_energy_by_hub(
#         self, 
#         hub_code: str, 
#         date_str: Optional[str] = None
#     ) -> Dict[str, Any]:
#         """
#         Get daily energy consumption for a specific hub.
        
#         Args:
#             hub_code: Hub code to get data for
#             date_str: Date string in YYYY-MM-DD format (defaults to today)
            
#         Returns:
#             Dictionary with energy data
#         """
#         if date_str is None:
#             date_str = datetime.now().strftime("%Y-%m-%d")
            
#         conn, cursor = self._get_connection()
        
#         try:
#             # Get hub total
#             cursor.execute(
#                 """
#                 SELECT energy_kwh, usage_hours, user_id 
#                 FROM energy_daily 
#                 WHERE hub_code = ? AND date = ? AND device_id IS NULL
#                 """,
#                 (hub_code, date_str)
#             )
            
#             hub_total = cursor.fetchone()
            
#             if not hub_total:
#                 return {
#                     "hub_code": hub_code,
#                     "date": date_str,
#                     "total_energy": 0.0,
#                     "unit": "kWh",
#                     "devices": {}
#                 }
            
#             # Get all devices for this hub and date
#             cursor.execute(
#                 """
#                 SELECT device_id, device_type, energy_kwh, usage_hours
#                 FROM energy_daily 
#                 WHERE hub_code = ? AND date = ? AND device_id IS NOT NULL
#                 """,
#                 (hub_code, date_str)
#             )
            
#             devices = {}
#             for row in cursor.fetchall():
#                 devices[row['device_id']] = {
#                     "device_id": row['device_id'],
#                     "device_type": row['device_type'],
#                     "energy_value": row['energy_kwh'],
#                     "unit": "kWh",
#                     "usage_hours": row['usage_hours']
#                 }
            
#             return {
#                 "hub_code": hub_code,
#                 "date": date_str,
#                 "total_energy": hub_total['energy_kwh'],
#                 "unit": "kWh",
#                 "usage_hours": hub_total['usage_hours'],
#                 "user_id": hub_total['user_id'],
#                 "devices": devices
#             }
            
#         except Exception as e:
#             print(f"Error getting daily energy: {e}")
#             return {}
            
#         finally:
#             conn.close()
    
#     def delete_user_data(self, user_id: str) -> bool:
#         """
#         Delete all data for a specific user (cascades to all related data).
        
#         Args:
#             user_id: User ID to delete
            
#         Returns:
#             True if successful, False otherwise
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             # Delete the user (will cascade delete all related data)
#             cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
            
#             deleted = cursor.rowcount > 0
#             conn.commit()
            
#             if deleted:
#                 print(f"Deleted all data for user {user_id}")
#             else:
#                 print(f"No user found with ID {user_id}")
                
#             return deleted
            
#         except Exception as e:
#             conn.rollback()
#             print(f"Error deleting user data: {e}")
#             return False
            
#         finally:
#             conn.close()
    
#     def get_user_hubs(self, user_id: str) -> List[Dict[str, Any]]:
#         """
#         Get all hubs for a specific user.
        
#         Args:
#             user_id: User ID to get hubs for
            
#         Returns:
#             List of hub dictionaries
#         """
#         conn, cursor = self._get_connection()
        
#         try:
#             cursor.execute(
#                 """
#                 SELECT h.*, 
#                        hs.daily_energy, hs.weekly_energy, 
#                        hs.monthly_energy, hs.yearly_energy,
#                        hs.device_count
#                 FROM hubs h
#                 LEFT JOIN hub_summary hs ON h.hub_code = hs.hub_code
#                 WHERE h.user_id = ?
#                 """,
#                 (user_id,)
#             )
            
#             hubs = []
#             for row in cursor.fetchall():
#                 hub = dict(row)
#                 hubs.append(hub)
                
#             return hubs
            
#         except Exception as