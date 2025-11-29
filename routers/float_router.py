from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from core.database import get_db
from models.floatInfo import FloatDetails
from schemas.float_schema import FloatResponse

router = APIRouter()

@router.get("/float_fullinfo/{float_id}", response_model=FloatResponse)
def get_float_full_info(float_id: str, db: Session = Depends(get_db)):
    """
    Get full details of a float by its float_id.
    """
    float_info = db.query(FloatDetails).filter(FloatDetails.float_id == float_id).first()
    if not float_info:
        raise HTTPException(status_code=404, detail="Float not found")
    return float_info
