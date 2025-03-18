import logging
import datetime
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from device_data_manager import DeviceDataManager
from database_manager import DatabaseManager
from energy_calculator import EnergyCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Smart Home Energy API",
    description="API for smart home energy consumption data",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins in development
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Initialize managers
db = DatabaseManager()
device_manager = DeviceDataManager()

# Define Pydantic models for responses
class DeviceBase(BaseModel):
    device_id: str
    device_type: str
    status: bool

class DeviceEnergy(DeviceBase):
    energy_value: float
    unit: str
    usage_hours: float

class HubBase(BaseModel):
    hub_id: str
    hub_code: str
    home_type: str
    user_id: str

class HubEnergy(HubBase):
    total_energy: float
    unit: str
    device_count: int
    devices: Dict[str, DeviceEnergy]

class EnergySummary(BaseModel):
    user_id: str
    daily_total: float
    weekly_total: float
    monthly_total: float
    yearly_total: float
    unit: str
    hub_count: int

class TopConsumer(BaseModel):
    device_id: str
    device_type: str
    hub_code: str
    energy_kwh: float
    unit: str
    status: bool
    home_type: str

# Dependency for common operations
def get_calculator():
    return EnergyCalculator()

def calculate_room_energy(room_devices, device_manager, time_multiplier=1.0):
    """
    Calculate energy consumption for a room based on its devices.
    
    Args:
        room_devices: List of device dictionaries or IDs in the room
        device_manager: DeviceDataManager instance for energy rates
        time_multiplier: Multiplier for time period (1.0 for daily, 7.0 for weekly, etc.)
        
    Returns:
        Calculated energy value in kWh
    """
    total_energy = 0.0
    
    for device in room_devices:
        # Handle different device formats
        if isinstance(device, dict):
            device_type = device.get('device_type', '').lower()
            
            # Calculate energy based on device type
            rate = device_manager.ENERGY_RATES.get(device_type, 0.0)
            
            # Default usage hours based on device type
            if device_type in ['thermostat', 'door', 'smartdoor']:
                hours = 24.0
            else:
                hours = 10.0
            
            # Calculate energy
            device_energy = rate * hours * time_multiplier
            total_energy += device_energy
    
    return total_energy

# Routes
@app.get("/")
async def root():
    return {"message": "Smart Home Energy API is running"}

@app.get("/refresh", summary="Refresh data from Firestore")
async def refresh_data(calculator: EnergyCalculator = Depends(get_calculator)):
    """Manually trigger a refresh of data from Firestore."""
    success = calculator.run_once()
    if success:
        return {"message": "Data refreshed successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to refresh data")

@app.get("/users/{user_id}/summary", response_model=EnergySummary)
async def get_user_summary(user_id: str):
    """Get energy summary for a user."""
    summary = db.get_energy_summary(user_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"No data found for user {user_id}")
    return summary

@app.get("/users/{user_id}/hubs")
async def get_user_hubs(user_id: str):
    """Get all hubs for a user."""
    hubs = db.get_user_hubs(user_id)
    if not hubs:
        raise HTTPException(status_code=404, detail=f"No hubs found for user {user_id}")
    return hubs

@app.get("/hubs/{hub_code}/energy/daily")
async def get_hub_daily_energy(hub_code: str, date: Optional[str] = None):
    """
    Get daily energy for a hub.
    
    Date format: YYYY-MM-DD (defaults to today)
    """
    if not date:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        
    energy_data = db.get_daily_energy_by_hub(hub_code, date)
    if not energy_data:
        raise HTTPException(status_code=404, detail=f"No energy data found for hub {hub_code} on {date}")
    return energy_data

@app.get("/users/{user_id}/top-consumers", response_model=List[TopConsumer])
async def get_top_consumers(
    user_id: str, 
    period: str = Query("daily", enum=["daily", "weekly", "monthly", "yearly"]),
    limit: int = Query(5, ge=1, le=20)
):
    """
    Get top energy consuming devices for a user.
    
    Period: daily, weekly, monthly, yearly
    Limit: Number of devices to return (1-20)
    """
    consumers = db.get_top_consumers(user_id, period, limit)
    if not consumers:
        raise HTTPException(
            status_code=404, 
            detail=f"No top consumers found for user {user_id} in {period} period"
        )
    return consumers

@app.get("/devices/{hub_code}")
async def get_hub_devices(hub_code: str):
    """Get all devices for a hub."""
    devices = db.get_devices_for_hub(hub_code)
    if not devices:
        raise HTTPException(status_code=404, detail=f"No devices found for hub {hub_code}")
    return devices

@app.get("/firestore/hubs")
async def get_firestore_hubs():
    """Get all hubs directly from Firestore."""
    hubs = device_manager.get_all_hubs()
    return hubs

@app.get("/firestore/devices")
async def get_firestore_devices(hub_code: Optional[str] = None):
    """
    Get devices directly from Firestore.
    
    Optionally filter by hub_code.
    """
    if hub_code:
        devices = device_manager.get_devices_by_hub_code(hub_code)
    else:
        devices = device_manager.get_all_devices()
    return devices
@app.get("/hubs/{hub_code}/rooms")
async def get_hub_rooms(hub_code: str):
    """Get all rooms for a specific hub."""
    rooms = device_manager.get_rooms_by_hub_code(hub_code)
    if not rooms:
        raise HTTPException(status_code=404, detail=f"No rooms found for hub {hub_code}")
    return rooms

@app.get("/hub/{hub_code}/energy", summary="Get hub energy data in the standard format")
async def get_hub_energy_data(hub_code: str):
    """
    Get energy data for a hub in the standard format.
    This endpoint is only for tenant hubs.
    Includes daily, weekly, monthly, and yearly data.
    """
    try:
        # First, get the hub details to check if it's a tenant hub
        hub_details = None
        
        # Get hub from Firestore
        hubs = device_manager.firestore.query_collection("userHubs", "hubCode", "==", hub_code)
        if hubs and len(hubs) > 0:
            hub_details = hubs[0]
        
        # If no hub found or not a tenant hub, return 404
        if not hub_details:
            raise HTTPException(status_code=404, detail=f"Hub {hub_code} not found")
            
        # Get the hub type (tenant or admin)
        home_type = hub_details.get('homeType', '').lower()
        
        # If this is not a tenant hub, return 403 Forbidden
        if home_type != 'tenant':
            raise HTTPException(
                status_code=403, 
                detail=f"This endpoint is only for tenant hubs. Hub {hub_code} is of type {home_type}."
            )
            
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_week = str(int(now.strftime("%U")))
        current_month = now.strftime("%m")
        current_year = now.strftime("%Y")
        
        # Get list of all rooms for this hub
        rooms = device_manager.get_rooms_by_hub_code(hub_code)
        if not rooms:
            raise HTTPException(status_code=404, detail=f"No rooms found for hub {hub_code}")
            
        # Get all devices for this hub
        devices = device_manager.get_devices_by_hub_code(hub_code)
        if not devices:
            raise HTTPException(status_code=404, detail=f"No devices found for hub {hub_code}")
        
        # Create a mapping of device types
        device_types = {}
        for device in devices:
            device_id = device.get('deviceId', '')
            device_type = device.get('deviceType', '').capitalize()
            device_types[device_id] = device_type
        
        # Construct the response based on the hub-rooms.json format
        response = {
            "hub_id": hub_code,
            "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
            "hub_type": home_type,  # Add hub type to response
            "energy_data": {
                "daily": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "date": current_date,
                    "rooms": {}
                },
                "weekly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "week": current_week,
                    "year": current_year,
                    "rooms": {}
                },
                "monthly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "month": current_month,
                    "year": current_year,
                    "rooms": {}
                },
                "yearly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "year": current_year,
                    "rooms": {}
                }
            }
        }
        
        # For each room, add its data to each time period
        for room in rooms:
            room_id = room.get('roomId', '')
            room_name = room.get('roomName', f"Room {room_id}")
            
            # Get devices in this room
            room_devices = []
            # This will vary based on how your room devices are stored
            devices_list = room.get('devices', [])
            
            for device_item in devices_list:
                if isinstance(device_item, str):
                    device_id = device_item
                elif isinstance(device_item, dict):
                    device_id = device_item.get('deviceId', '')
                else:
                    continue
                    
                # Get device type from our mapping
                device_type = device_types.get(device_id, 'Unknown')
                room_devices.append({"device_type": device_type})
            
            # If room has device_details, use that instead
            if 'device_details' in room and room['device_details']:
                room_devices = room['device_details']
            
            # For each time period, add this room data
            for period in ["daily", "weekly", "monthly", "yearly"]:
                # Calculate time multiplier based on period
                if period == "daily":
                    time_multiplier = 1.0
                elif period == "weekly":
                    time_multiplier = 7.0  # 7 days
                elif period == "monthly":
                    time_multiplier = 30.0  # ~30 days
                else:  # yearly
                    time_multiplier = 365.0  # 365 days
                
                # Calculate energy using our helper function
                energy_value = calculate_room_energy(room_devices, device_manager, time_multiplier)
                
                # Add room data to this period
                # Using room_name as the key but including room_id in the value
                response["energy_data"][period]["rooms"][room_name] = {
                    "energy_value": energy_value,
                    "unit": "kWh",
                    "device_count": len(room_devices),
                    "room_id": room_id,  # Added room_id to the value
                    "devices": room_devices
                }
                
                # Add to total energy for this period
                response["energy_data"][period]["total_energy"] += energy_value
        
        # Try to get actual data from database if available
        try:
            # Get daily energy data from database
            daily_data = db.get_daily_energy_by_hub(hub_code)
            if daily_data and "total_energy" in daily_data:
                response["energy_data"]["daily"]["total_energy"] = daily_data["total_energy"]
                
                # If we have room data, update that too
                if "rooms" in daily_data:
                    for room_name, room_data in daily_data["rooms"].items():
                        if room_name in response["energy_data"]["daily"]["rooms"]:
                            response["energy_data"]["daily"]["rooms"][room_name]["energy_value"] = room_data.get("energy_value", 0)
        except Exception as e:
            logger.warning(f"Could not get actual daily energy data: {e}")
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting hub energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Similar endpoint for admin hubs
@app.get("/admin-hub/{hub_code}/energy", summary="Get energy data for admin hub")
async def get_admin_hub_energy_data(hub_code: str):
    """
    Get energy data for an admin hub in the standard format.
    This endpoint is only for admin hubs.
    Includes daily, weekly, monthly, and yearly data.
    """
    try:
        # First, get the hub details to check if it's an admin hub
        hub_details = None
        
        # Get hub from Firestore
        hubs = device_manager.firestore.query_collection("userHubs", "hubCode", "==", hub_code)
        if hubs and len(hubs) > 0:
            hub_details = hubs[0]
        
        # If no hub found or not an admin hub, return 404
        if not hub_details:
            raise HTTPException(status_code=404, detail=f"Hub {hub_code} not found")
            
        # Get the hub type (tenant or admin)
        home_type = hub_details.get('homeType', '').lower()
        
        # If this is not an admin hub, return 403 Forbidden
        if home_type != 'admin':
            raise HTTPException(
                status_code=403, 
                detail=f"This endpoint is only for admin hubs. Hub {hub_code} is of type {home_type}."
            )
        
        # For now, we'll use the same implementation as the tenant hub
        # But you can customize this for admin hubs as needed
        # ... rest of the implementation similar to tenant hub
        
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        
        # Placeholder response - you can implement the full response similar to the tenant hub
        return {
            "hub_id": hub_code,
            "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
            "hub_type": home_type,
            "message": "Admin hub energy data endpoint - to be implemented"
        }
        
    except Exception as e:
        logger.error(f"Error getting admin hub energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/hub/{hub_code}/real-energy", summary="Get real hub energy data without simulations")
async def get_hub_real_energy_data(hub_code: str):
    """
    Get energy data for a hub using only real data from the database.
    This endpoint is only for tenant hubs and only returns data that actually exists.
    """
    try:
        # First, get the hub details to check if it's a tenant hub
        hub_details = None
        
        # Get hub from Firestore
        hubs = device_manager.firestore.query_collection("userHubs", "hubCode", "==", hub_code)
        if hubs and len(hubs) > 0:
            hub_details = hubs[0]
        
        # If no hub found or not a tenant hub, return 404
        if not hub_details:
            raise HTTPException(status_code=404, detail=f"Hub {hub_code} not found")
            
        # Get the hub type (tenant or admin)
        home_type = hub_details.get('homeType', '').lower()
        
        # If this is not a tenant hub, return 403 Forbidden
        if home_type != 'tenant':
            raise HTTPException(
                status_code=403, 
                detail=f"This endpoint is only for tenant hubs. Hub {hub_code} is of type {home_type}."
            )
            
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_week = str(int(now.strftime("%U")))
        current_month = now.strftime("%m")
        current_year = now.strftime("%Y")
        
        # Get list of all rooms for this hub
        rooms = device_manager.get_rooms_by_hub_code(hub_code)
        if not rooms:
            raise HTTPException(status_code=404, detail=f"No rooms found for hub {hub_code}")
        
        # Construct the response structure
        response = {
            "hub_id": hub_code,
            "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
            "hub_type": home_type,
            "energy_data": {
                "daily": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "date": current_date,
                    "rooms": {}
                },
                "weekly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "week": current_week,
                    "year": current_year,
                    "rooms": {}
                },
                "monthly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "month": current_month,
                    "year": current_year,
                    "rooms": {}
                },
                "yearly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "year": current_year,
                    "rooms": {}
                }
            }
        }
        
        # Try to get actual daily energy data from database
        daily_data = None
        try:
            daily_data = db.get_daily_energy_by_hub(hub_code)
        except Exception as e:
            logger.warning(f"Could not get daily energy data: {e}")
        
        # If we have daily data, use it
        if daily_data and "total_energy" in daily_data:
            response["energy_data"]["daily"]["total_energy"] = daily_data["total_energy"]
            
            # Get devices from daily data
            devices_map = {}
            if "devices" in daily_data:
                for device_id, device_data in daily_data["devices"].items():
                    devices_map[device_id] = {
                        "device_type": device_data.get("device_type", "unknown")
                    }
            
            # Process rooms for daily data
            for room in rooms:
                room_id = room.get('roomId', '')
                room_name = room.get('roomName', f"Room {room_id}")
                
                # Get devices in this room
                room_devices = []
                room_energy = 0
                
                # This will vary based on how your room devices are stored
                devices_list = room.get('devices', [])
                
                for device_item in devices_list:
                    if isinstance(device_item, str):
                        device_id = device_item
                    elif isinstance(device_item, dict):
                        device_id = device_item.get('deviceId', '')
                    else:
                        continue
                    
                    # If we have this device in our daily data, add its energy
                    if device_id in daily_data.get("devices", {}):
                        device_data = daily_data["devices"][device_id]
                        room_energy += device_data.get("energy_value", 0)
                        
                        room_devices.append({
                            "device_type": device_data.get("device_type", "unknown")
                        })
                    elif device_id in devices_map:
                        # Use the device type we already know
                        room_devices.append(devices_map[device_id])
                
                # If room has device_details, use that for device types
                if 'device_details' in room and room['device_details']:
                    room_devices = room['device_details']
                
                # Add room data to daily period
                response["energy_data"]["daily"]["rooms"][room_name] = {
                    "energy_value": room_energy,
                    "unit": "kWh",
                    "device_count": len(room_devices),
                    "room_id": room_id,
                    "devices": room_devices
                }
        
        # For weekly, monthly, yearly - we'll just use empty data structures
        # but with the correct room names and device types
        for period in ["weekly", "monthly", "yearly"]:
            for room in rooms:
                room_id = room.get('roomId', '')
                room_name = room.get('roomName', f"Room {room_id}")
                
                # Get devices in this room
                room_devices = []
                if 'device_details' in room and room['device_details']:
                    room_devices = room['device_details']
                else:
                    # Use devices from daily data if available
                    devices_list = room.get('devices', [])
                    for device_item in devices_list:
                        if isinstance(device_item, str):
                            device_id = device_item
                        elif isinstance(device_item, dict):
                            device_id = device_item.get('deviceId', '')
                        else:
                            continue
                            
                        if device_id in devices_map:
                            room_devices.append(devices_map[device_id])
                
                # Add room with zero energy for other periods
                response["energy_data"][period]["rooms"][room_name] = {
                    "energy_value": 0,
                    "unit": "kWh",
                    "device_count": len(room_devices),
                    "room_id": room_id,
                    "devices": room_devices
                }
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting real hub energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))   

# Run the app using uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
