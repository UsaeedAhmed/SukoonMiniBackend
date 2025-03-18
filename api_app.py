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

# Run the app using uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
