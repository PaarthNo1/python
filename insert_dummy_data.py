from sqlalchemy.orm import Session
from core.database import SessionLocal
from models.floatInfo import FloatDetails, ArgoType, FloatStatus
from datetime import datetime, date

def insert_dummy_data():
    db: Session = SessionLocal()
    
    try:
        # Check if data already exists
        existing = db.query(FloatDetails).filter(FloatDetails.float_id == "1902043").first()
        if existing:
            print("⚠ Dummy data for 1902043 already exists.")
            return

        dummy_float = FloatDetails(
            float_id="1902043",
            type="APEX",
            argo_type=ArgoType.core_argo,
            status=FloatStatus.active,
            current_location="12.34, 56.78",
            latitude=12.34,
            longitude=56.78,
            previous_locations=["10.00, 50.00", "11.00, 53.00"],
            cycle_history={"cycles": [1, 2, 3], "last_cycle": 3},
            dac_id="AOML",
            dac_last_updated=datetime.now(),
            assigned_personnel="Dr. Ocean",
            usage_count=15,
            next_maintenance_due=date(2025, 12, 31),
            deployment_date=datetime(2023, 1, 15, 10, 0, 0),
            launch_latitude=10.0,
            launch_longitude=50.0,
            sensors=["CTD", "Oxygen", "Nitrate"],
            remarks="Test float for API verification"
        )
        
        db.add(dummy_float)
        db.commit()
        print("✅ Dummy data inserted successfully for Float 1902043.")
        
    except Exception as e:
        print(f"❌ Failed to insert data: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    insert_dummy_data()
