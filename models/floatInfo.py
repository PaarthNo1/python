from sqlalchemy import Column, Integer, String, Date, TIMESTAMP, JSON, ARRAY, Enum, func, Float
import enum
from core.database import Base

# 1️⃣ ENUM Types for Validation
class ArgoType(enum.Enum):
    core_argo = "core_argo"
    bgc_argo = "bgc_argo"
    deep_argo = "deep_argo"
    custom = "custom"

class FloatStatus(enum.Enum):
    active = "active"
    inactive = "inactive"
    maintenance = "maintenance"
    deployed = "deployed"

# 2️⃣ Database Table Schema
class FloatDetails(Base):
    __tablename__ = "float_details"

    id = Column(Integer, primary_key=True, autoincrement=True)
    float_id = Column(String(50), unique=True, nullable=False)
    type = Column(String(50))
    argo_type = Column(Enum(ArgoType), nullable=False)
    status = Column(Enum(FloatStatus), default=FloatStatus.active)
    
    current_location = Column(String(100))
    latitude = Column(Float)  # New
    longitude = Column(Float) # New
    previous_locations = Column(ARRAY(String))   # TEXT[]
    
    cycle_history = Column(JSON)    # JSONB
    dac_id = Column(String(50))
    dac_last_updated = Column(TIMESTAMP)
    
    assigned_personnel = Column(String(100))
    usage_count = Column(Integer, default=0)
    next_maintenance_due = Column(Date)

    # Deployment Info
    deployment_date = Column(TIMESTAMP) # New
    launch_latitude = Column(Float)     # New
    launch_longitude = Column(Float)    # New
    
    # Sensors
    sensors = Column(ARRAY(String))     # New
    
    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    remarks = Column(String)
