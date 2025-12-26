import uuid
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_
from db.models import FileData, Credits 

END_OF_TIME = datetime.strptime("9999-12-31 00:00:00", "%Y-%m-%d %H:%M:%S")

def upsert_file(
    db: Session,
    external_file_id: str,
    user_id: str,
    tenant_id: str,
    customer_id: str,
    project_id: str,
    file_name: str,
    s3_file_path: str,
    platform_file_path: str,
    version: int
):
    now = datetime.now()
    try:
        # 1. Validate user (stub)
        # if not user_exists: return None
        user_exists = True 

        # 2. Check active file (SCD2 logic)
        existing_active = db.query(FileData).filter(
            FileData.tenant_id == tenant_id,
            FileData.file_name == file_name,
            FileData.e_dt == END_OF_TIME
        ).first()

        if existing_active:
            ai_file_id = existing_active.ai_file_id
            # Close the old version (Update e_dt)
            existing_active.e_dt = now
            existing_active.modified_by = user_id
        else:
            ai_file_id = str(uuid.uuid4())

        # 3. Insert new SCD2 record
        new_file_record = FileData(
            ai_file_id=ai_file_id,
            st_dt=now,
            e_dt=END_OF_TIME,
            external_file_id=external_file_id,
            cloud_file_path=s3_file_path,
            platform_file_path=platform_file_path,
            file_name=file_name,
            version=version,
            uploaded_by=user_id,
            tenant_id=tenant_id,
            customer_id=customer_id,
            project_id=project_id
        )
        
        db.add(new_file_record)
        db.commit()
        return ai_file_id

    except Exception as e:
        db.rollback()
        return None

def delete_file(
    db: Session,
    external_file_id: str,
    tenant_id: str,
    file_name: str,
    s3_file_path: str | None = None,
    platform_file_path: str | None = None,
    version: int | None = None
):
    now = datetime.now()
    try:
        # Find active record
        active_file = db.query(FileData).filter(
            FileData.tenant_id == tenant_id,
            FileData.file_name == file_name,
            FileData.e_dt == END_OF_TIME
        ).first()

        if not active_file:
            return None

        # Logic: Set e_dt to now to "deactivate" it
        active_file.e_dt = now
        db.commit()
        return active_file.ai_file_id
    
    except Exception:
        db.rollback()
        return None

def update_md_file_info(
    db: Session,
    ai_file_id: str,
    tenant_id: str,
    md_file_path: str,
    md_file_id: str
):
    try:
        # Find the active record for this specific ai_file_id
        active_file = db.query(FileData).filter(
            FileData.ai_file_id == ai_file_id,
            FileData.tenant_id == tenant_id,
            FileData.e_dt == END_OF_TIME
        ).first()

        if not active_file:
            return None

        # Update MD info
        active_file.md_file_path = md_file_path
        active_file.md_file_id = md_file_id
        
        db.commit()
        return ai_file_id
    except Exception:
        db.rollback()
        return None

def log_llm_credits(
    db: Session,
    user_id: str,
    customer_id: str,
    project_id: str,
    tenant_id: str,
    task_type: str,
    task_date_time: datetime,
    input_size: int | None,
    output_size: int | None,
    tokens_used: int | None,
    model_used: str | None
):
    task_id = str(uuid.uuid4())
    try:
        new_credit = Credits(
            task_id=task_id,
            user_id=user_id,
            tenant_id=tenant_id,
            customer_id=customer_id,
            project_id=project_id,
            task_type=task_type,
            task_date_time=task_date_time,
            input_size=input_size,
            output_size=output_size,
            tokens_used=tokens_used,
            model_used=model_used
        )
        db.add(new_credit)
        db.commit()
        return task_id
    except Exception:
        db.rollback()
        return None
    

def handle_file_from_files_ms(
    db: Session,
    external_file_id: str,
    user_id: str,
    tenant_id: str,
    customer_id: str,
    project_id: str,
    file_name: str,
    platform_file_path: str,
    version: int
):
    """
    Entry point for files coming from File MS.

    - Creates/updates file_data entry
    - Uses separate AI S3 bucket (fake path)
    - Relies on SCD2 logic inside upsert_file
    """

    # --------------------------------------------------
    # 1. Fake AI S3 path (separate from File MS bucket)
    # --------------------------------------------------
    fake_ai_s3_path = (
        f"s3://ai-bucket/"
        f"{tenant_id}/"
        f"{file_name}/"
        f"v{version}/"
        f"{file_name}"
    )

    # --------------------------------------------------
    # 2. Create / Update file record
    # --------------------------------------------------
    ai_file_id = upsert_file(
        db=db,
        external_file_id=external_file_id,
        user_id=user_id,
        tenant_id=tenant_id,
        customer_id=customer_id,
        project_id=project_id,
        file_name=file_name,
        s3_file_path=fake_ai_s3_path,
        platform_file_path=platform_file_path,
        version=version
    )

    if not ai_file_id:
        # TODO: central error logging
        return None

    return {
        "ai_file_id": ai_file_id,
        "ai_s3_path": fake_ai_s3_path,
        "version": version
    }


def rename_file_record(
    db: Session,
    ai_file_id: str,
    user_id: str,
    tenant_id: str,
    customer_id: str,
    project_id: str,
    old_file_name: str,
    new_file_name: str,
    s3_file_path: str,
    platform_file_path: str,
    version: int
):
    """
    Renames a file by closing old SCD2 row and inserting a new one
    with the SAME ai_file_id.
    """
    now = datetime.now()

    try:
        # --------------------------------------------------
        # 1. Find ACTIVE record with old name
        # --------------------------------------------------
        active = (
            db.query(FileData)
            .filter(
                FileData.ai_file_id == ai_file_id,
                FileData.tenant_id == tenant_id,
                FileData.file_name == old_file_name,
                FileData.e_dt == END_OF_TIME
            )
            .first()
        )

        if not active:
            # TODO: central error
            return None

        # --------------------------------------------------
        # 2. Close old record
        # --------------------------------------------------
        active.e_dt = now
        active.modified_by = user_id

        # --------------------------------------------------
        # 3. Insert new ACTIVE record (same ai_file_id)
        # --------------------------------------------------
        new_record = FileData(
            ai_file_id=ai_file_id,
            st_dt=now,
            e_dt=END_OF_TIME,
            external_file_id=active.external_file_id,
            cloud_file_path=s3_file_path,
            platform_file_path=platform_file_path,
            file_name=new_file_name,
            version=version,
            uploaded_by=active.uploaded_by,
            modified_by=user_id,
            tenant_id=tenant_id,
            customer_id=customer_id,
            project_id=project_id,
            file_hash=active.file_hash  # content unchanged
        )

        db.add(new_record)
        db.commit()
        return ai_file_id

    except Exception:
        db.rollback()
        return None

















# import uuid
# from datetime import datetime
# from sqlalchemy.orm import Session
# from db.models import FileData, Credits 

# # Global constant for the "Active" record end date
# END_OF_TIME = datetime(9999, 12, 31, 0, 0, 0)

# def upsert_file(
#     db: Session,
#     external_file_id: str,
#     user_id: str,
#     tenant_id: str,
#     customer_id: str,
#     project_id: str,
#     file_name: str,
#     s3_file_path: str,
#     platform_file_path: str,
#     version: int,
#     # Optional fields newly added
#     file_type: str = None,
#     description: str = None,
#     file_date: datetime = None,
#     # CRITICAL: Allows linking a new version to an existing history chain (e.g., Rename)
#     existing_ai_file_id: str = None 
# ):
#     now = datetime.now()
#     try:
#         # 1. Determine Identity Strategy
#         # If we are passed an ID (e.g. from a rename operation), we strictly use it.
#         ai_file_id = existing_ai_file_id

#         # 2. Check for an ACTIVE record with this exact name in this tenant
#         # We need to close it before creating a new one (SCD Type 2)
#         existing_active = db.query(FileData).filter(
#             FileData.tenant_id == tenant_id,
#             FileData.file_name == file_name,
#             FileData.e_dt == END_OF_TIME
#         ).first()

#         if existing_active:
#             # If we found an active file, we are about to replace it.
#             # If no specific ID was passed, we inherit the ID from this active file
#             # to maintain the version history of "this file name".
#             if not ai_file_id:
#                 ai_file_id = existing_active.ai_file_id
            
#             # Close the old version
#             existing_active.e_dt = now
#             existing_active.modified_by = user_id
        
#         # 3. If still no ID (New file, and no rename context), generate fresh UUID
#         if not ai_file_id:
#             ai_file_id = str(uuid.uuid4())

#         # 4. Insert new Active Record
#         new_file_record = FileData(
#             ai_file_id=ai_file_id,
#             st_dt=now,
#             e_dt=END_OF_TIME,
#             external_file_id=external_file_id,
#             cloud_file_path=s3_file_path,
#             platform_file_path=platform_file_path,
#             file_name=file_name,
#             file_type=file_type,
#             description=description,
#             version=version,
#             file_date=file_date,
#             uploaded_by=user_id,
#             tenant_id=tenant_id,
#             customer_id=customer_id,
#             project_id=project_id
#         )
        
#         db.add(new_file_record)
#         # Flush ensures the ID is generated/available before commit, catches integrity errors early
#         db.flush() 
#         db.commit()
#         return ai_file_id

#     except Exception as e:
#         db.rollback()
#         # In production, integrate with your central logging here
#         print(f"Error in upsert_file: {str(e)}")
#         return None

# def delete_file(db: Session, tenant_id: str, file_name: str, **kwargs):
#     """
#     Soft Delete: Finds the active record and sets its End Date to NOW.
#     Returns the ai_file_id of the closed file (useful for Renaming).
#     """
#     now = datetime.now()
#     try:
#         active_file = db.query(FileData).filter(
#             FileData.tenant_id == tenant_id,
#             FileData.file_name == file_name,
#             FileData.e_dt == END_OF_TIME
#         ).first()

#         if not active_file:
#             return None

#         # Logic: Set e_dt to now to "deactivate" it
#         active_file.e_dt = now
#         db.commit()
        
#         return active_file.ai_file_id
#     except Exception:
#         db.rollback()
#         return None