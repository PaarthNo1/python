from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from core.database import get_db
from typing import List
from models.floatInfo import FloatDetails, Traj, Tech, MetaKV
from schemas.float_schema import FloatResponse, TrajResponse, TechResponse, MetaKVResponse

router = APIRouter()

@router.get("/float_fullinfo/{float_id}", response_model=FloatResponse)
def get_float_full_info(float_id: str, db: Session = Depends(get_db)):
    """
    Get full details of a float by its float_id.
    """
    float_info = db.query(FloatDetails).filter(FloatDetails.float_id == float_id).first()
    if not float_info:
        raise HTTPException(status_code=404, detail="Float not found")
    
    # Generate Links
    base_url = f"/float/{float_id}"
    float_info.links = {
        "trajectory": f"{base_url}/trajectory",
        "technical": f"{base_url}/tech",
        "metadata": f"{base_url}/metadata",
        "cycles": f"{base_url}/cycles" # Placeholder if we implement cycles later
    }
    
    return float_info

@router.get("/float/{float_id}/trajectory", response_model=List[TrajResponse])
def get_float_trajectory(float_id: str, db: Session = Depends(get_db)):
    """Get trajectory data for a float."""
    traj_data = db.query(Traj).filter(Traj.float_id == float_id).all()
    return traj_data

@router.get("/float/{float_id}/tech", response_model=List[TechResponse])
def get_float_tech(float_id: str, db: Session = Depends(get_db)):
    """Get technical data for a float."""
    tech_data = db.query(Tech).filter(Tech.float_id == float_id).all()
    return tech_data

@router.get("/float/{float_id}/metadata", response_model=List[MetaKVResponse])
def get_float_metadata(float_id: str, db: Session = Depends(get_db)):
    """Get metadata key-value pairs for a float."""
    meta_data = db.query(MetaKV).filter(MetaKV.float_id == float_id).all()
    return meta_data
