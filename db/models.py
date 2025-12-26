from sqlalchemy import Column, String, DateTime, Text, Integer, BIGINT ,Date, TIMESTAMP, text
from sqlalchemy.sql import func
from db.connection import Base
from sqlalchemy.dialects.mysql import CHAR, TINYINT, JSON
from datetime import datetime
import uuid

class FileData(Base):
    __tablename__ = "file_data"

    ai_file_id = Column(String(36), primary_key=True, nullable=False)
    st_dt = Column(DateTime, primary_key=True, nullable=False)
    e_dt = Column(DateTime, nullable=False, server_default=text("'9999-12-31 00:00:00'"))
    
    external_file_id = Column(String(255), nullable=True)
    cloud_file_path = Column(String(1024), nullable=False)
    md_file_path = Column(String(1024), nullable=True)
    md_file_id = Column(String(36), nullable=True)
    
    file_name = Column(String(255), nullable=False)
    platform_file_path = Column(String(1024), nullable=True)
    file_type = Column(String(50), nullable=True)
    description = Column(Text, nullable=True)
    
    version = Column(Integer, nullable=False)
    file_date = Column(Date, nullable=True)

    file_hash = Column(String(64), nullable=True)
    
    uploaded_by = Column(String(36), nullable=False)
    modified_by = Column(String(36), nullable=True)
    
    tenant_id = Column(String(36), nullable=False)
    customer_id = Column(String(36), nullable=False)
    project_id = Column(String(36), nullable=False)
    
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)

class Credits(Base):
    __tablename__ = "credits"

    task_id = Column(String(36), primary_key=True, nullable=False)
    user_id = Column(String(36), nullable=False)
    tenant_id = Column(String(36), nullable=False)
    customer_id = Column(String(36), nullable=False)
    project_id = Column(String(36), nullable=False)
    
    task_type = Column(String(32), nullable=False)
    task_date_time = Column(DateTime, nullable=False)
    
    input_size = Column(BIGINT, nullable=True)
    output_size = Column(BIGINT, nullable=True)
    tokens_used = Column(BIGINT, nullable=True)
    model_used = Column(String(100), nullable=True)
    
    created_at = Column(TIMESTAMP, server_default=func.now(), nullable=False)



# ============================================================
# User Microservice Models (matching User Microservice schema)
# Added for User Microservice integration
# ============================================================

END_OF_TIME = datetime(9999, 12, 31, 0, 0, 0)

class Organization(Base):
    """Organization table matching User Microservice schema"""
    __tablename__ = "organizations"
    
    id = Column(CHAR(36), primary_key=True, comment="UUID org identifier")
    parent_org_id = Column(CHAR(36), nullable=True, index=True, comment="Parent org ID (for hierarchy)")
    name = Column(String(255), nullable=False, comment="Organization name")
    org_type = Column(String(64), nullable=False, comment="PROVIDER / CUSTOMER / CONSUMER / VENDOR / INTERNAL")
    status = Column(String(32), nullable=False, default="ACTIVE", comment="ACTIVE / DEACTIVATED")
    # Use org_metadata as Python attribute, but map to 'metadata' column in DB
    org_metadata = Column("metadata", JSON, nullable=True, comment="Org-level configuration or descriptive metadata")
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), comment="Record creation timestamp")


class UserData(Base):
    """User data table matching User Microservice schema (SCD Type 2)"""
    __tablename__ = "user_data"
    
    # Composite PK: (u_id, st_dt) - REQUIRED for SCD Type 2
    u_id = Column(CHAR(36), primary_key=True, comment="User ID (shared across services)")
    st_dt = Column(DateTime, primary_key=True, nullable=False, default=func.now(), comment="SCD2 start datetime (valid from)")
    e_dt = Column(DateTime, nullable=False, default=END_OF_TIME, comment="SCD2 end datetime (valid till)")
    
    # Tenant ID field
    tenant_id = Column(CHAR(36), nullable=True, index=True, comment="Tenant ID for multi-tenant isolation")
    
    # Optional fields from User Microservice
    first_name = Column(String(100), nullable=True, comment="User first name")
    last_name = Column(String(100), nullable=True, comment="User last name")
    dob = Column(Date, nullable=True, comment="Date of birth")
    gender = Column(String(1), nullable=True, comment="M=Male, F=Female, X=Prefer not to say")
    image_path = Column(String(500), nullable=True, comment="Profile image URL or file path")
    account_type_id = Column(TINYINT, nullable=True, comment="References account_typ_master (logical join)")
    
    # Helper property for backward compatibility
    @property
    def id(self):
        """Backward compatibility: return u_id"""
        return self.u_id
    
    @property
    def display_name(self):
        """Get display name from first_name + last_name or return empty string"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.first_name or self.last_name or ""


class Email(Base):
    """Email table matching User Microservice schema (SCD Type 2)"""
    __tablename__ = "email"
    
    # Composite PK: (email_id, st_dt)
    email_id = Column(CHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()), comment="Email row ID")
    st_dt = Column(DateTime, primary_key=True, nullable=False, default=func.now(), comment="SCD2 start datetime")
    e_dt = Column(DateTime, nullable=False, default=END_OF_TIME, comment="SCD2 end datetime")
    
    # Email fields
    u_id = Column(CHAR(36), nullable=False, index=True, comment="User ID (logical reference to user_data)")
    email = Column(String(100), nullable=False, index=True, comment="Email address")
    is_verified = Column(String(1), nullable=True, comment="1=verified, 0=not verified")


class UserOrgMembership(Base):
    """User-Organization memberships matching User Microservice schema"""
    __tablename__ = "user_org_memberships"
    
    id = Column(CHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()), comment="UUID membership record ID")
    u_id = Column(CHAR(36), nullable=False, index=True, comment="User ID (logical reference to user_data)")
    org_id = Column(CHAR(36), nullable=False, index=True, comment="Organization ID (logical reference to organizations)")
    status = Column(String(32), nullable=False, comment="ACTIVE / INVITED / SUSPENDED / REMOVED")
    source = Column(String(32), nullable=False, comment="SELF_SIGNUP / INVITE / SYNC / WORKFLOW")
    created_at = Column(TIMESTAMP, nullable=False, default=func.now(), comment="Record creation timestamp")
    
    # Unique constraint: one membership per user-org pair
    # Note: This is application-managed, no DB constraint