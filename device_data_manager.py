from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from firestore_connection import get_firestore, FirestoreConnection

class DeviceDataManager:
    """
    Manager for fetching and processing device and hub data from Firestore.
    """
    
    # Energy consumption rates for different device types in kWh/hour
    ENERGY_RATES = {
        "ac": 1.5,  # Air Conditioner
        "airconditioner": 1.5,  # Alternative name
        "dishwasher": 1.0,
        "tv": 0.1,
        "light": 0.06,
        "thermostat": 0.05,
        "fan": 0.03,
        "door": 0.01,  # Smart Door
        "smartdoor": 0.01,  # Alternative name
        "heatconvector": 1.2,
        "washingmachine": 0.5,
        "speaker": 0.1
    }
    
    def __init__(self):
        """Initialize the DeviceDataManager with a Firestore connection."""
        self.db = get_firestore()
        self.firestore = FirestoreConnection()
    
    def get_all_hubs(self) -> List[Dict[str, Any]]:
        """
        Fetch all hubs from Firestore.
        
        Returns:
            List of hub dictionaries with hub data
        """
        hubs_collection = self.firestore.get_collection("userHubs")
        hubs = []
        
        for hub_doc in hubs_collection.stream():
            hub_data = hub_doc.to_dict()
            hub_data['hubId'] = hub_doc.id  # Add the document ID as hubId
            hubs.append(hub_data)
        
        return hubs
    
    def get_hub_by_id(self, hub_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific hub by its ID.
        
        Args:
            hub_id: The ID of the hub to fetch
            
        Returns:
            Hub data dictionary or None if not found
        """
        hub_data = self.firestore.get_document("userHubs", hub_id)
        if hub_data:
            hub_data['hubId'] = hub_id
        return hub_data
    
    def get_devices_by_hub_code(self, hub_code: str) -> List[Dict[str, Any]]:
        """
        Get all devices associated with a specific hub code.
        
        Args:
            hub_code: The hub code to filter devices by
            
        Returns:
            List of device dictionaries
        """
        devices = self.firestore.query_collection("devices", "hubCode", "==", hub_code)
        
        # Add the document ID as deviceId if not already present
        for device in devices:
            if 'deviceId' not in device:
                # This would require modifying the query to include document IDs
                # For now, we assume deviceId is already in the document
                pass
        
        return devices
    
    def get_all_devices(self) -> List[Dict[str, Any]]:
        """
        Fetch all devices from Firestore.
        
        Returns:
            List of device dictionaries
        """
        devices_collection = self.firestore.get_collection("devices")
        devices = []
        
        for device_doc in devices_collection.stream():
            device_data = device_doc.to_dict()
            
            # Ensure deviceId is present
            if 'deviceId' not in device_data:
                device_data['deviceId'] = device_doc.id
                
            devices.append(device_data)
        
        return devices
    
    def calculate_device_energy(self, device: Dict[str, Any], hours: float = 1.0) -> float:
        """
        Calculate energy consumption for a device.
        
        Args:
            device: Device data dictionary
            hours: Number of hours the device has been active
            
        Returns:
            Energy consumption in kWh
        """
        device_type = device.get('deviceType', '').lower()
        
        # Skip calculation if device is off
        if device.get('on') is False:
            return 0.0
            
        # Get the energy rate for this device type
        rate = self.ENERGY_RATES.get(device_type, 0.0)
        
        # Calculate energy consumption
        energy_consumption = rate * hours
        
        return energy_consumption
    
    def get_devices_with_energy(
        self, 
        hub_code: Optional[str] = None, 
        hours: float = 1.0
    ) -> List[Dict[str, Any]]:
        """
        Get devices with calculated energy consumption.
        
        Args:
            hub_code: Optional hub code to filter by
            hours: Hours to calculate energy for
            
        Returns:
            List of devices with energy consumption added
        """
        # Get devices, filtered by hub_code if provided
        if hub_code:
            devices = self.get_devices_by_hub_code(hub_code)
        else:
            devices = self.get_all_devices()
        
        # Add energy calculation to each device
        for device in devices:
            device['energyConsumption'] = self.calculate_device_energy(device, hours)
            device['unit'] = 'kWh'
            device['calculatedFor'] = f"{hours} hour(s)"
        
        return devices
    
    def get_energy_by_hub(self, hours: float = 24.0) -> Dict[str, Any]:
        """
        Calculate total energy consumption grouped by hub.
        
        Args:
            hours: Hours to calculate energy for (default 24 hours/1 day)
            
        Returns:
            Dictionary with hub codes as keys and energy data as values
        """
        # Get all hubs
        hubs = self.get_all_hubs()
        
        # Initialize results dict
        results = {}
        
        # For each hub, calculate total energy from its devices
        for hub in hubs:
            hub_code = hub.get('hubCode')
            if not hub_code:
                continue
                
            # Get devices for this hub
            devices = self.get_devices_with_energy(hub_code, hours)
            
            # Calculate total energy for this hub
            total_energy = sum(device.get('energyConsumption', 0.0) for device in devices)
            
            # Group devices by type for breakdown
            device_types = {}
            for device in devices:
                device_type = device.get('deviceType', '').lower()
                if device_type not in device_types:
                    device_types[device_type] = 0.0
                    
                device_types[device_type] += device.get('energyConsumption', 0.0)
            
            # Store hub data
            results[hub_code] = {
                'hubId': hub.get('hubId'),
                'hubCode': hub_code,
                'homeType': hub.get('homeType'),
                'userId': hub.get('userId'),
                'totalEnergy': total_energy,
                'unit': 'kWh',
                'deviceCount': len(devices),
                'deviceTypes': device_types,
                'calculatedFor': f"{hours} hour(s)",
                'timestamp': datetime.now().isoformat()
            }
        
        return results
    
    def get_rooms_by_hub_code(self, hub_code: str) -> List[Dict[str, Any]]:
        """
        Get all rooms associated with a specific hub code.
        """
        rooms = self.firestore.query_collection("rooms", "hubCode", "==", hub_code)
        
        # Ensure each room has its devices details populated
        for room in rooms:
            # Get device details for this room
            room_devices = []
            # Check if devices is a list of strings or list of dicts
            devices_list = room.get('devices', [])
            
            for device_item in devices_list:
                # Handle both cases - string device ID or device dict
                if isinstance(device_item, str):
                    device_id = device_item
                else:
                    device_id = device_item.get('deviceId', '')
                    
                if device_id:
                    device = self.get_device_by_id(device_id)
                    if device:
                        room_devices.append({
                            "device_type": device.get('deviceType', 'unknown')
                        })
            
            room['device_details'] = room_devices
            room['device_count'] = len(room_devices)
        
        return rooms

    def get_device_by_id(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific device by its ID.
        
        Args:
            device_id: The ID of the device to fetch
            
        Returns:
            Device data dictionary or None if not found
        """
        device_collection = self.firestore.get_collection("devices")
        device_doc = device_collection.document(device_id).get()
        
        if device_doc.exists:
            device_data = device_doc.to_dict()
            device_data['deviceId'] = device_id
            return device_data
        
        return None 

# Example usage
if __name__ == "__main__":
    manager = DeviceDataManager()
    
    print("Fetching hubs...")
    hubs = manager.get_all_hubs()
    print(f"Found {len(hubs)} hubs")
    
    print("\nFetching devices...")
    devices = manager.get_all_devices()
    print(f"Found {len(devices)} devices")
    
    print("\nCalculating daily energy consumption by hub...")
    hub_energy = manager.get_energy_by_hub(24.0)
    for hub_code, data in hub_energy.items():
        print(f"\nHub {hub_code}:")
        print(f"  Total Energy: {data['totalEnergy']} {data['unit']}")
        print(f"  Device Count: {data['deviceCount']}")
        print("  Device Type Breakdown:")
        for device_type, energy in data.get('deviceTypes', {}).items():
            print(f"    - {device_type}: {energy} {data['unit']}")
