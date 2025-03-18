import time
import datetime
import logging
from typing import Dict, List, Any, Optional

from firestore_connection import FirestoreConnection
from device_data_manager import DeviceDataManager
from database_manager import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("energy_calculator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class EnergyCalculator:
    """
    Main class for calculating and storing energy data.
    """
    
    def __init__(self, poll_interval: int = 60):
        """
        Initialize the energy calculator.
        
        Args:
            poll_interval: Interval in minutes to poll Firestore for data
        """
        self.poll_interval = poll_interval  # minutes
        self.device_manager = DeviceDataManager()
        self.db = DatabaseManager()
        logger.info("Energy Calculator initialized")
        
    def fetch_and_store_all_data(self):
        """Fetch all data from Firestore and store in SQLite."""
        try:
            logger.info("Starting data fetch and store operation...")
            
            # Get all hubs
            hubs = self.device_manager.get_all_hubs()
            logger.info(f"Found {len(hubs)} hubs")
            
            # For each hub, store in database and get its devices
            for hub in hubs:
                # Safely extract hub information with fallback values
                hub_id = hub.get('hubId', 'unknown')
                hub_code = hub.get('hubCode', hub_id)  # Use hubId as fallback if hubCode is missing
                user_id = hub.get('userId', '')
                home_type = hub.get('homeType', '')
                
                # Skip hubs with null or empty userId
                if not user_id:
                    logger.info(f"Skipping hub: {hub_code} - No user ID assigned (dormant hub)")
                    continue
                    
                logger.info(f"Processing hub: {hub_code} for user: {user_id}")
                
                # Store hub in database
                self.db.add_hub(hub_id, hub_code, user_id, home_type)
                
                # Get devices for this hub
                devices = self.device_manager.get_devices_by_hub_code(hub_code)
                logger.info(f"Found {len(devices)} devices for hub {hub_code}")
                
                # For each device, store in database
                for device in devices:
                    device_id = device.get('deviceId', 'unknown')
                    device_type = device.get('deviceType', 'unknown').lower()
                    status = device.get('on', False)
                    
                    logger.info(f"Processing device: {device_id}, type: {device_type}, status: {status}")
                    
                    # Store device in database
                    self.db.add_device(device_id, hub_code, device_type, status)
                
                # Get rooms for this hub
                rooms = self.device_manager.get_rooms_by_hub_code(hub_code)
                logger.info(f"Found {len(rooms)} rooms for hub {hub_code}")
                
                # Store each room with proper handling of device IDs
                for room in rooms:
                    room_id = room.get('roomId', 'unknown')
                    room_name = room.get('roomName', room_id)
                    
                    # Handle different device list formats
                    device_ids = []
                    devices_data = room.get('devices', [])
                    
                    for device_item in devices_data:
                        if isinstance(device_item, str):
                            device_ids.append(device_item)
                        elif isinstance(device_item, dict):
                            device_id = device_item.get('deviceId')
                            if device_id:
                                device_ids.append(device_id)
                    
                    # Store room in database
                    self.db.add_room(room_id, room_name, hub_code, device_ids)
                
                # Calculate and store daily energy
                self._calculate_and_store_daily_energy(hub, devices)
            
            logger.info("Data fetch and store operation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error in fetch_and_store_all_data: {e}")
            return False
    
    def _calculate_and_store_daily_energy(self, hub: Dict[str, Any], devices: List[Dict[str, Any]]):
        """
        Calculate and store daily energy for a hub and its devices.
        
        Args:
            hub: Hub data dictionary
            devices: List of device dictionaries for this hub
        """
        hub_code = hub.get('hubCode', 'unknown')
        
        # Use a default user ID if not provided
        user_id = hub.get('userId', f'user_{hub_code}')
        if not user_id:
            user_id = f'user_{hub_code}'
        
        # Today's date in YYYY-MM-DD format
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        
        # Calculate daily energy for each device
        total_energy = 0.0
        
        for device in devices:
            device_id = device.get('deviceId', 'unknown')
            device_type = device.get('deviceType', 'unknown').lower()
            status = device.get('on', False)
            
            # Skip if device is off
            if not status:
                logger.info(f"Device {device_id} is off, energy is 0")
                energy = 0.0
                hours = 0.0
            else:
                # Get energy rate for this device type
                rate = self.device_manager.ENERGY_RATES.get(device_type, 0.0)
                
                # Assume device has been on for X hours (can be adjusted)
                # For demo purposes, using 10 hours for most devices, 24 for thermostat-like devices
                if device_type in ['thermostat', 'door', 'smartdoor']:
                    hours = 24.0
                else:
                    hours = 10.0
                
                # Calculate energy in kWh
                energy = rate * hours
                logger.info(f"Device {device_id} energy: {energy} kWh (Rate: {rate}, Hours: {hours})")
            
            try:
                # Store in database
                self.db.store_daily_energy(today, user_id, hub_code, device_id, device_type, energy, hours)
                
                # Add to total
                total_energy += energy
            except Exception as e:
                logger.error(f"Error storing energy for device {device_id}: {e}")
        
        # Store hub total
        logger.info(f"Hub {hub_code} total energy: {total_energy} kWh")
        try:
            self.db.store_hub_daily_total(today, user_id, hub_code, total_energy)
        except Exception as e:
            logger.error(f"Error storing hub total energy: {e}")
    
    def run_scheduler(self):
        """Run the scheduler to periodically fetch and store data."""
        logger.info(f"Starting scheduler with {self.poll_interval} minute interval")
        
        try:
            while True:
                # Run the data fetch and store operation
                success = self.fetch_and_store_all_data()
                if success:
                    logger.info("Data refresh completed successfully")
                else:
                    logger.warning("Data refresh completed with errors")
                
                # Sleep for the poll interval
                logger.info(f"Sleeping for {self.poll_interval} minutes...")
                time.sleep(self.poll_interval * 60)
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
    
    def run_once(self):
        """Run the data fetch and store operation once and exit."""
        logger.info("Running one-time data fetch and store operation")
        return self.fetch_and_store_all_data()

# Demo usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Smart Home Energy Calculator')
    parser.add_argument(
        '--scheduler', 
        action='store_true', 
        help='Run as a scheduler instead of one-time execution'
    )
    parser.add_argument(
        '--interval', 
        type=int, 
        default=60, 
        help='Polling interval in minutes (default: 60)'
    )
    
    args = parser.parse_args()
    
    calculator = EnergyCalculator(poll_interval=args.interval)
    
    if args.scheduler:
        calculator.run_scheduler()
    else:
        calculator.run_once()