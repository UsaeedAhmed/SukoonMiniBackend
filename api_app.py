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


@app.get("/health", summary="API Health Check")
async def health_check():
    """
    Simple health check endpoint to verify the API is running.
    Returns a 200 OK status with basic API information.
    """
    return {
        "status": "ok",
        "timestamp": datetime.datetime.now().isoformat(),
        "version": app.version,
        "service": "Smart Home Energy API"
    }

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

@app.get("/hub/{hub_code}/energy", summary="Get hub energy data with simulations")
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
# @app.get("/admin-hub/{hub_code}/energy", summary="Get energy data for admin hub")
# async def get_admin_hub_energy_data(hub_code: str):
#     """
#     Get energy data for an admin hub in the standard format.
#     This endpoint is only for admin hubs.
#     Includes daily, weekly, monthly, and yearly data.
#     """
#     try:
#         # First, get the hub details to check if it's an admin hub
#         hub_details = None
        
#         # Get hub from Firestore
#         hubs = device_manager.firestore.query_collection("userHubs", "hubCode", "==", hub_code)
#         if hubs and len(hubs) > 0:
#             hub_details = hubs[0]
        
#         # If no hub found or not an admin hub, return 404
#         if not hub_details:
#             raise HTTPException(status_code=404, detail=f"Hub {hub_code} not found")
            
#         # Get the hub type (tenant or admin)
#         home_type = hub_details.get('homeType', '').lower()
        
#         # If this is not an admin hub, return 403 Forbidden
#         if home_type != 'admin':
#             raise HTTPException(
#                 status_code=403, 
#                 detail=f"This endpoint is only for admin hubs. Hub {hub_code} is of type {home_type}."
#             )
        
#         # For now, we'll use the same implementation as the tenant hub
#         # But you can customize this for admin hubs as needed
#         # ... rest of the implementation similar to tenant hub
        
#         # Get current date
#         now = datetime.datetime.now()
#         current_date = now.strftime("%Y-%m-%d")
        
#         # Placeholder response - you can implement the full response similar to the tenant hub
#         return {
#             "hub_id": hub_code,
#             "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
#             "hub_type": home_type,
#             "message": "Admin hub energy data endpoint - to be implemented"
#         }
        
#     except Exception as e:
#         logger.error(f"Error getting admin hub energy data: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


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
@app.get("/room/{room_id}/real-energy", summary="Get real room energy data without simulations")
async def get_room_real_energy_data(room_id: str):
    """
    Get energy data for a specific room using only real data from the database.
    This endpoint only returns data that actually exists in the database,
    without simulating missing values.
    """
    try:
        # First, get the room details to verify it exists
        room_details = None
        
        # Find the room in the database
        conn, cursor = db._get_connection()
        try:
            cursor.execute(
                "SELECT room_id, room_name, hub_code FROM rooms WHERE room_id = ?",
                (room_id,)
            )
            room_row = cursor.fetchone()
            
            if room_row:
                room_details = dict(room_row)
            
        except Exception as e:
            logger.error(f"Database error when fetching room: {e}")
        finally:
            conn.close()
        
        # If room not found in database, try Firestore
        if not room_details:
            # Get all rooms from Firestore and find the one with matching room_id
            all_rooms = []
            
            hubs = device_manager.get_all_hubs()
            for hub in hubs:
                hub_code = hub.get('hubCode')
                if hub_code:
                    rooms = device_manager.get_rooms_by_hub_code(hub_code)
                    all_rooms.extend(rooms)
            
            for room in all_rooms:
                if room.get('roomId') == room_id:
                    room_details = {
                        'room_id': room.get('roomId'),
                        'room_name': room.get('roomName', f"Room {room_id}"),
                        'hub_code': room.get('hubCode')
                    }
                    break
        
        # If room still not found, return 404
        if not room_details:
            raise HTTPException(status_code=404, detail=f"Room {room_id} not found")
        
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_week = now.strftime("%U")
        current_month = now.strftime("%m")
        current_year = now.strftime("%Y")
        
        # Prepare the basic response structure
        response = {
            "room_id": room_id,
            "room_name": room_details.get('room_name', f"Room {room_id}"),
            "hub_id": room_details.get('hub_code', ''),
            "energy_data": {
                "daily": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "date": current_date,
                    "devices": {}
                },
                "weekly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "week": current_week,
                    "year": current_year,
                    "devices": {}
                },
                "monthly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "month": current_month,
                    "year": current_year,
                    "devices": {}
                },
                "yearly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "year": current_year,
                    "devices": {}
                }
            }
        }
        
        # Get device IDs for this room
        device_ids = []
        conn, cursor = db._get_connection()
        try:
            cursor.execute(
                "SELECT device_id FROM room_devices WHERE room_id = ?",
                (room_id,)
            )
            for row in cursor.fetchall():
                device_ids.append(row['device_id'])
        except Exception as e:
            logger.error(f"Error getting device IDs for room: {e}")
        finally:
            conn.close()
        
        # If no devices found, return the empty structure
        if not device_ids:
            return response
        
        # Try to get actual data from database
        conn, cursor = db._get_connection()
        try:
            # For each time period, get the energy data if available
            periods = [
                {"table": "energy_daily", "date_field": "date", "date_value": current_date, "period": "daily"},
                {"table": "energy_weekly", "date_field": "week", "date_value": current_week, "year_field": "year", "year_value": current_year, "period": "weekly"},
                {"table": "energy_monthly", "date_field": "month", "date_value": current_month, "year_field": "year", "year_value": current_year, "period": "monthly"},
                {"table": "energy_yearly", "date_field": "year", "date_value": current_year, "period": "yearly"}
            ]
            
            for period_info in periods:
                period = period_info["period"]
                table = period_info["table"]
                
                # Build query based on period
                if period == "daily":
                    query = f"""
                    SELECT e.device_id, e.device_type, e.energy_kwh, e.usage_hours, d.status
                    FROM {table} e
                    JOIN devices d ON e.device_id = d.device_id
                    WHERE e.device_id IN ({','.join(['?'] * len(device_ids))})
                    AND e.date = ?
                    """
                    params = device_ids + [period_info["date_value"]]
                    
                elif period in ["weekly", "monthly"]:
                    query = f"""
                    SELECT e.device_id, e.device_type, e.energy_kwh, e.usage_hours, d.status
                    FROM {table} e
                    JOIN devices d ON e.device_id = d.device_id
                    WHERE e.device_id IN ({','.join(['?'] * len(device_ids))})
                    AND e.{period_info["date_field"]} = ?
                    AND e.{period_info["year_field"]} = ?
                    """
                    params = device_ids + [period_info["date_value"], period_info["year_value"]]
                    
                else:  # yearly
                    query = f"""
                    SELECT e.device_id, e.device_type, e.energy_kwh, e.usage_hours, d.status
                    FROM {table} e
                    JOIN devices d ON e.device_id = d.device_id
                    WHERE e.device_id IN ({','.join(['?'] * len(device_ids))})
                    AND e.{period_info["date_field"]} = ?
                    """
                    params = device_ids + [period_info["date_value"]]
                
                try:
                    cursor.execute(query, params)
                    
                    for row in cursor.fetchall():
                        device_data = dict(row)
                        device_id = device_data.get('device_id')
                        
                        # Get device name from devices table
                        device_name = f"{device_data.get('device_type')} {device_id}"
                        try:
                            cursor.execute(
                                "SELECT device_type FROM devices WHERE device_id = ?",
                                (device_id,)
                            )
                            device_info = cursor.fetchone()
                            if device_info:
                                device_name = f"{room_details.get('room_name')} {device_info['device_type']}"
                        except Exception as e:
                            logger.warning(f"Could not get device name: {e}")
                        
                        # Calculate hourly rate
                        hourly_rate = 0
                        usage_hours = device_data.get('usage_hours', 0)
                        if usage_hours > 0:
                            hourly_rate = round(device_data.get('energy_kwh', 0) / usage_hours, 2)
                        
                        # Add device to response
                        response["energy_data"][period]["devices"][device_id] = {
                            "device_id": device_id,
                            "device_name": device_name,
                            "device_type": device_data.get('device_type', 'Unknown'),
                            "energy_value": device_data.get('energy_kwh', 0),
                            "unit": "kWh",
                            "usage_hours": usage_hours,
                            "hourly_rate": hourly_rate
                        }
                        
                        # Add to total energy
                        response["energy_data"][period]["total_energy"] += device_data.get('energy_kwh', 0)
                        
                except Exception as e:
                    logger.warning(f"Error getting {period} energy data: {e}")
            
        except Exception as e:
            logger.error(f"Database error: {e}")
        finally:
            conn.close()
        
        # Round all energy values for cleaner response
        for period in ["daily", "weekly", "monthly", "yearly"]:
            for device_id, device in response["energy_data"][period]["devices"].items():
                device["energy_value"] = round(device["energy_value"], 2)
                device["hourly_rate"] = round(device["hourly_rate"], 2)
            response["energy_data"][period]["total_energy"] = round(response["energy_data"][period]["total_energy"], 2)
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting real room energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/room/{room_id}/energy", summary="Get room energy data with simulated values")
async def get_room_energy_data(room_id: str):
    """
    Get energy data for a specific room.
    The response follows the format of kitchen.json and home-office.json files,
    with the room as the main object and devices as sub-sections.
    """
    try:
        # First, get the room details to verify it exists
        room_details = None
        
        # Find the room in the database
        conn, cursor = db._get_connection()
        try:
            cursor.execute(
                "SELECT room_id, room_name, hub_code FROM rooms WHERE room_id = ?",
                (room_id,)
            )
            room_row = cursor.fetchone()
            
            if room_row:
                room_details = dict(room_row)
            
        except Exception as e:
            logger.error(f"Database error when fetching room: {e}")
        finally:
            conn.close()
        
        # If room not found in database, try Firestore
        if not room_details:
            # Get all rooms from Firestore and find the one with matching room_id
            all_rooms = []
            
            hubs = device_manager.get_all_hubs()
            for hub in hubs:
                hub_code = hub.get('hubCode')
                if hub_code:
                    rooms = device_manager.get_rooms_by_hub_code(hub_code)
                    all_rooms.extend(rooms)
            
            for room in all_rooms:
                if room.get('roomId') == room_id:
                    room_details = {
                        'room_id': room.get('roomId'),
                        'room_name': room.get('roomName', f"Room {room_id}"),
                        'hub_code': room.get('hubCode')
                    }
                    break
            
        # If room still not found, return 404
        if not room_details:
            raise HTTPException(status_code=404, detail=f"Room {room_id} not found")
        
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_week = now.strftime("%U")
        current_month = now.strftime("%m")
        current_year = now.strftime("%Y")
        
        # Get device details for this room
        room_devices = []
        
        # Try to get devices from database first
        conn, cursor = db._get_connection()
        try:
            cursor.execute(
                """
                SELECT d.device_id, d.device_type, d.status
                FROM room_devices rd
                JOIN devices d ON rd.device_id = d.device_id
                WHERE rd.room_id = ?
                """,
                (room_id,)
            )
            
            for device_row in cursor.fetchall():
                device_data = dict(device_row)
                room_devices.append({
                    "device_id": device_data.get('device_id'),
                    "device_type": device_data.get('device_type', 'Unknown'),
                    "status": bool(device_data.get('status', 0))
                })
                
        except Exception as e:
            logger.error(f"Database error when fetching room devices: {e}")
        finally:
            conn.close()
        
        # If no devices found, we'll just continue with an empty list
        # This means the response will have the correct structure but no device data
        if len(room_devices) == 0:
            logger.warning(f"No devices found for room {room_id}")
            # We'll just use the devices we have (empty list)
        
        # Prepare the response structure (following kitchen.json/home-office.json format)
        response = {
            "room_id": room_id,
            "room_name": room_details.get('room_name', f"Room {room_id}"),
            "hub_id": room_details.get('hub_code', ''),
            "energy_data": {
                "daily": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "date": current_date,
                    "devices": {}
                },
                "weekly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "week": current_week,
                    "year": current_year,
                    "devices": {}
                },
                "monthly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "month": current_month,
                    "year": current_year,
                    "devices": {}
                },
                "yearly": {
                    "total_energy": 0,
                    "unit": "kWh",
                    "year": current_year,
                    "devices": {}
                }
            }
        }
        
        # For each device and time period, generate energy data
        for device in room_devices:
            device_id = device.get('device_id', '')
            device_type = device.get('device_type', 'Unknown')
            
            # Get a nice device name if one doesn't exist
            if 'device_name' in device and device['device_name']:
                device_name = device['device_name']
            else:
                # Try to create a descriptive name based on room and device type
                room_name = room_details.get('room_name', '')
                device_name = f"{room_name} {device_type}"
                
                # Special handling for common device types
                if device_type.lower() == 'thermostat':
                    device_name = f"{room_name} Thermostat"
                elif device_type.lower() == 'light':
                    device_name = f"{room_name} Ceiling Light"
                elif device_type.lower() in ['tv', 'television']:
                    device_name = f"{room_name} TV"
                elif device_type.lower() in ['air conditioner', 'airconditioner', 'ac']:
                    device_name = f"{room_name} Air Conditioner"
            
            # Use device manager's energy rates to calculate consumption
            # Convert device type to a key that matches the device_manager's ENERGY_RATES dictionary
            device_type_key = device_type.lower().replace(' ', '')
            hourly_rate = device_manager.ENERGY_RATES.get(device_type_key, 0.05)
            
            # If we don't find a match, try some alternative mappings
            if hourly_rate == 0.05 and device_type.lower() in ['air conditioner', 'air-conditioner']:
                hourly_rate = device_manager.ENERGY_RATES.get('airconditioner', 0.05)
            elif hourly_rate == 0.05 and device_type.lower() in ['smart door']:
                hourly_rate = device_manager.ENERGY_RATES.get('door', 0.05)
            
            # Calculate usage hours based on device type
            daily_hours = 0
            if device_type.lower() in ['thermostat', 'smartdoor', 'door']:
                daily_hours = 24
            elif device_type.lower() in ['light', 'fan']:
                daily_hours = 10
            elif device_type.lower() in ['tv', 'airconditioner', 'ac', 'air conditioner']:
                daily_hours = 8
            elif device_type.lower() == 'dishwasher':
                daily_hours = 2
            else:
                daily_hours = 6  # Default value
            
            # Daily data
            daily_energy = hourly_rate * daily_hours
            response["energy_data"]["daily"]["devices"][device_id] = {
                "device_id": device_id,
                "device_name": device_name,
                "device_type": device_type,
                "energy_value": daily_energy,
                "unit": "kWh",
                "usage_hours": daily_hours,
                "hourly_rate": hourly_rate
            }
            response["energy_data"]["daily"]["total_energy"] += daily_energy
            
            # Weekly data (7x daily)
            weekly_hours = daily_hours * 7
            weekly_energy = hourly_rate * weekly_hours
            response["energy_data"]["weekly"]["devices"][device_id] = {
                "device_id": device_id,
                "device_name": device_name,
                "device_type": device_type,
                "energy_value": weekly_energy,
                "unit": "kWh",
                "usage_hours": weekly_hours,
                "hourly_rate": hourly_rate
            }
            response["energy_data"]["weekly"]["total_energy"] += weekly_energy
            
            # Monthly data (~30x daily)
            monthly_hours = daily_hours * 30
            monthly_energy = hourly_rate * monthly_hours
            response["energy_data"]["monthly"]["devices"][device_id] = {
                "device_id": device_id,
                "device_name": device_name,
                "device_type": device_type,
                "energy_value": monthly_energy,
                "unit": "kWh",
                "usage_hours": monthly_hours,
                "hourly_rate": hourly_rate
            }
            response["energy_data"]["monthly"]["total_energy"] += monthly_energy
            
            # Yearly data (365x daily)
            yearly_hours = daily_hours * 365
            yearly_energy = hourly_rate * yearly_hours
            response["energy_data"]["yearly"]["devices"][device_id] = {
                "device_id": device_id,
                "device_name": device_name,
                "device_type": device_type,
                "energy_value": yearly_energy,
                "unit": "kWh",
                "usage_hours": yearly_hours,
                "hourly_rate": hourly_rate
            }
            response["energy_data"]["yearly"]["total_energy"] += yearly_energy
        
        # Round total energy values for cleaner numbers
        for period in ["daily", "weekly", "monthly", "yearly"]:
            response["energy_data"][period]["total_energy"] = round(response["energy_data"][period]["total_energy"], 2)
        
        # Try to get actual data from database if available
        try:
            # Get daily energy data from database
            conn, cursor = db._get_connection()
            
            try:
                # Query for actual device energy data
                cursor.execute(
                    """
                    SELECT d.device_id, d.device_type, ed.energy_kwh, ed.usage_hours 
                    FROM devices d
                    JOIN room_devices rd ON d.device_id = rd.device_id
                    JOIN energy_daily ed ON d.device_id = ed.device_id
                    WHERE rd.room_id = ? AND ed.date = ?
                    """,
                    (room_id, current_date)
                )
                
                real_devices = cursor.fetchall()
                
                if real_devices:
                    # Reset the daily values
                    response["energy_data"]["daily"]["total_energy"] = 0
                    
                    # Update with real data
                    for device_row in real_devices:
                        device_data = dict(device_row)
                        device_id = device_data.get('device_id')
                        
                        if device_id in response["energy_data"]["daily"]["devices"]:
                            # Update existing device
                            response["energy_data"]["daily"]["devices"][device_id]["energy_value"] = device_data.get('energy_kwh', 0)
                            response["energy_data"]["daily"]["devices"][device_id]["usage_hours"] = device_data.get('usage_hours', 0)
                            
                            # Recalculate hourly rate
                            usage_hours = device_data.get('usage_hours', 0)
                            if usage_hours > 0:
                                response["energy_data"]["daily"]["devices"][device_id]["hourly_rate"] = round(
                                    device_data.get('energy_kwh', 0) / usage_hours, 2
                                )
                            
                            # Add to total
                            response["energy_data"]["daily"]["total_energy"] += device_data.get('energy_kwh', 0)
            
            except Exception as e:
                logger.warning(f"Could not get actual daily energy data: {e}")
            finally:
                conn.close()
                
        except Exception as e:
            logger.warning(f"Database connection error: {e}")
        
        # Round all energy values for cleaner response
        for period in ["daily", "weekly", "monthly", "yearly"]:
            for device_id, device in response["energy_data"][period]["devices"].items():
                device["energy_value"] = round(device["energy_value"], 2)
                device["hourly_rate"] = round(device["hourly_rate"], 2)
            response["energy_data"][period]["total_energy"] = round(response["energy_data"][period]["total_energy"], 2)
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting room energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/admin-hub/{hub_code}/energy", summary="Get energy data for admin hub")
async def get_admin_hub_energy_data(hub_code: str):
    """
    Get energy data for an admin hub in the standard format.
    This endpoint is only for admin hubs.
    Includes daily, weekly, monthly, and yearly data with connected tenant hubs' energy consumption.
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
        
        # Get current date
        now = datetime.datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_week = str(int(now.strftime("%U")))
        current_month = now.strftime("%m")
        current_year = now.strftime("%Y")
        
        # Create response structure based on admin-hub.json
        response = {
            "hub_id": hub_code,
            "hub_name": hub_details.get('hubName', "Central Admin Hub"),
            "hub_type": "admin",
            "energy_data": {
                "daily": {
                    "total_energy": 0.0,
                    "unit": "kWh",
                    "date": current_date,
                    "tenant_hubs": {}
                },
                "weekly": {
                    "total_energy": 0.0,
                    "unit": "kWh",
                    "week": current_week,
                    "year": current_year,
                    "tenant_hubs": {}
                },
                "monthly": {
                    "total_energy": 0.0,
                    "unit": "kWh",
                    "month": current_month,
                    "year": current_year,
                    "tenant_hubs": {}
                },
                "yearly": {
                    "total_energy": 0.0,
                    "unit": "kWh",
                    "year": current_year,
                    "tenant_hubs": {}
                }
            }
        }
        
        # Get tenant hubs associated with this admin hub
        tenant_hub_codes = []
        if hub_details and 'units' in hub_details:
            tenant_hub_codes = hub_details.get('units', [])
            
        if not tenant_hub_codes:
            logger.warning(f"Admin hub {hub_code} has no tenant hubs associated with it")
            
        # For each tenant hub in the units array, fetch their energy data
        for tenant_hub_code in tenant_hub_codes:
            # Try to get hub details from Firestore
            tenant_hub_details = None
            try:
                tenant_hubs_from_db = device_manager.firestore.query_collection("userHubs", "hubCode", "==", tenant_hub_code)
                if tenant_hubs_from_db and len(tenant_hubs_from_db) > 0:
                    tenant_hub_details = tenant_hubs_from_db[0]
            except Exception as e:
                logger.warning(f"Error fetching tenant hub details for {tenant_hub_code}: {e}")
            
            # Get tenant hub name - this would typically come from the tenant hub's details
            # For the demo, we'll use names similar to those in admin-hub.json
            property_types = {
                "apartment": "Apartment Building",
                "house": "House",
                "office": "Commercial Office",
                "retail": "Retail Space"
            }
            
                        # Get a display name for the tenant hub
            property_type = 'apartment'
            if tenant_hub_details and 'propertyType' in tenant_hub_details:
                property_type = tenant_hub_details.get('propertyType', 'apartment').lower()
                
            property_name = property_types.get(property_type, "Apartment Building")
            tenant_name = None
            
            if tenant_hub_details and 'hubName' in tenant_hub_details:
                tenant_name = tenant_hub_details.get('hubName')
            
            # If no name in hub details, create one based on property type
            if not tenant_name:
                # Generate a letter suffix (A, B, C, etc.) based on position in list
                idx = tenant_hub_codes.index(tenant_hub_code)
                letter_suffix = chr(65 + (idx % 26))  # 65 is ASCII for 'A'
                tenant_name = f"{property_name} {letter_suffix}"
                
            # Now get energy data for the tenant hub
            # Try to use the hub energy endpoint we already have
            try:
                # We'll make an internal request to our own endpoint
                # This is a simplified approach - in a real app, you might use a more direct method
                tenant_data = None
                
                # First try the real energy data endpoint
                try:
                    tenant_data = await get_hub_real_energy_data(tenant_hub_code)
                except Exception:
                    # If real energy fails, try the simulated endpoint
                    try:
                        tenant_data = await get_hub_energy_data(tenant_hub_code)
                    except Exception as e:
                        logger.warning(f"Could not get energy data for tenant hub {tenant_hub_code}: {e}")
                
                if tenant_data and "energy_data" in tenant_data:
                    # Extract the energy data from the tenant hub response
                    # For each time period, extract total energy
                    for period in ["daily", "weekly", "monthly", "yearly"]:
                        if period in tenant_data["energy_data"]:
                            tenant_period_data = tenant_data["energy_data"][period]
                            tenant_energy = tenant_period_data.get("total_energy", 0.0)
                            
                            # Add to admin hub total
                            response["energy_data"][period]["total_energy"] += tenant_energy
                            
                            # Add tenant hub to period data
                            response["energy_data"][period]["tenant_hubs"][tenant_name] = {
                                "hub_id": tenant_hub_code,
                                "energy_value": tenant_energy,
                                "unit": "kWh"
                            }
                
            except Exception as e:
                logger.error(f"Error processing tenant hub {tenant_hub_code}: {e}")
                
                    # Generate simulated data since we couldn't get real data
                logger.info(f"Using simulated data for tenant hub {tenant_hub_code}")
                import random
                
                daily_energy = round(random.uniform(20.0, 70.0), 2)
                weekly_energy = daily_energy * 7
                monthly_energy = daily_energy * 30
                yearly_energy = daily_energy * 365
                
                # Add to admin hub totals
                response["energy_data"]["daily"]["total_energy"] += daily_energy
                response["energy_data"]["weekly"]["total_energy"] += weekly_energy
                response["energy_data"]["monthly"]["total_energy"] += monthly_energy
                response["energy_data"]["yearly"]["total_energy"] += yearly_energy
                
                # Add tenant hub entries
                for period, energy_value in [
                    ("daily", daily_energy),
                    ("weekly", weekly_energy),
                    ("monthly", monthly_energy),
                    ("yearly", yearly_energy)
                ]:
                    response["energy_data"][period]["tenant_hubs"][tenant_name] = {
                        "hub_id": tenant_hub_code,
                        "energy_value": energy_value,
                        "unit": "kWh"
                    }
        
        # Round all energy values for cleaner response
        for period in ["daily", "weekly", "monthly", "yearly"]:
            response["energy_data"][period]["total_energy"] = round(response["energy_data"][period]["total_energy"], 2)
            for hub_name, hub_data in response["energy_data"][period]["tenant_hubs"].items():
                hub_data["energy_value"] = round(hub_data["energy_value"], 2)
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting admin hub energy data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add this endpoint to api_app.py

# Add this endpoint to api_app.py

@app.get("/hubs/{hub_code}/live-energy", summary="Get real-time energy consumption for a hub")
async def get_hub_live_energy(hub_code: str):
    """
    Calculate and return the real-time energy consumption for a hub based on currently active devices.
    This endpoint returns the instantaneous power consumption in kilowatts (kW) rather than 
    energy consumption over time (kWh).
    """
    try:
        # Check if hub exists
        hub_details = None
        hubs = device_manager.firestore.query_collection("userHubs", "hubCode", "==", hub_code)
        if hubs and len(hubs) > 0:
            hub_details = hubs[0]
        
        if not hub_details:
            raise HTTPException(status_code=404, detail=f"Hub {hub_code} not found")
            
        # Get all devices for this hub
        devices = device_manager.get_devices_by_hub_code(hub_code)
        if not devices:
            # Return zero consumption if no devices found
            return {
                "hub_id": hub_code,
                "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
                "timestamp": datetime.datetime.now().isoformat(),
                "total_consumption": 0.0,
                "unit": "kW",
                "active_devices": 0,
                "devices": []
            }
        
        # Filter for devices that are currently on
        active_devices = [device for device in devices if device.get('on', False)]
        
        # Calculate real-time consumption for each active device
        total_consumption = 0.0
        device_consumption = []
        
        for device in active_devices:
            device_id = device.get('deviceId', 'unknown')
            device_type = device.get('deviceType', 'unknown').lower()
            
            # Get the hourly energy rate for this device type (in kWh)
            hourly_rate = device_manager.ENERGY_RATES.get(device_type, 0.05)
            
            # Convert hourly energy consumption (kWh) to instantaneous power (kW)
            # Since the rates are already in kW (energy per hour), we can use them directly
            power_consumption = hourly_rate
            
            # Add to total
            total_consumption += power_consumption
            
            # Get device name or create a descriptive one
            device_name = device.get('name', f"{device_type.capitalize()} {device_id[-4:]}")
            
            # Add device to result
            device_consumption.append({
                "device_id": device_id,
                "device_name": device_name,
                "device_type": device_type,
                "consumption": power_consumption,
                "unit": "kW"
            })
        
        # No need for room-based calculations since we don't need that data
        
        # Just round the total consumption for better readability
        total_consumption = round(total_consumption, 3)
        
        # Prepare simplified response with user ID
        response = {
            "hub_id": hub_code,
            "hub_name": hub_details.get('hubName', f"Hub {hub_code}"),
            "user_id": hub_details.get('userId', ''),
            "timestamp": datetime.datetime.now().isoformat(),
            "total_consumption": total_consumption,
            "unit": "kW",
            "active_devices": len(active_devices),
            "total_devices": len(devices)
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error calculating live energy consumption: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Run the app using uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
