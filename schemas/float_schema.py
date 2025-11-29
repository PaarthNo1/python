from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from enum import Enum

# Re-using Enums (or redefining if we want to decouple from ORM)
class ArgoType(str, Enum):
    core_argo = "core_argo"
    bgc_argo = "bgc_argo"
    deep_argo = "deep_argo"
    custom = "custom"

class FloatStatus(str, Enum):
    active = "active"
    inactive = "inactive"
    maintenance = "maintenance"
    deployed = "deployed"

class FloatResponse(BaseModel):
    id: int
    float_id: str
    type: Optional[str] = None
    argo_type: ArgoType
    status: FloatStatus
    
    current_location: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    previous_locations: Optional[List[str]] = None
    
    cycle_history: Optional[Dict[str, Any]] = None
    dac_id: Optional[str] = None
    dac_last_updated: Optional[datetime] = None
    
    assigned_personnel: Optional[str] = None
    usage_count: int = 0
    next_maintenance_due: Optional[date] = None

    deployment_date: Optional[datetime] = None
    launch_latitude: Optional[float] = None
    launch_longitude: Optional[float] = None
    
    sensors: Optional[List[str]] = None
    
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    remarks: Optional[str] = None

    class Config:
        from_attributes = True
