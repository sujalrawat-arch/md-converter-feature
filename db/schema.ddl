-- DDL generated from SQLAlchemy models in db/models.py
-- Run this SQL on your MySQL database to create the tables

-- You may need to adjust types or constraints for your specific MySQL version

-- Table: file_data
CREATE TABLE file_data (
    ai_file_id VARCHAR(36) NOT NULL,
    st_dt DATETIME NOT NULL,
    e_dt DATETIME NOT NULL DEFAULT '9999-12-31 00:00:00',
    external_file_id VARCHAR(255),
    cloud_file_path VARCHAR(1024) NOT NULL,
    md_file_path VARCHAR(1024),
    md_file_id VARCHAR(36),
    file_name VARCHAR(255) NOT NULL,
    platform_file_path VARCHAR(1024),
    file_type VARCHAR(50),
    description TEXT,
    version INT NOT NULL,
    file_date DATE,
    file_hash VARCHAR(64),
    uploaded_by VARCHAR(36) NOT NULL,
    modified_by VARCHAR(36),
    tenant_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    project_id VARCHAR(36) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ai_file_id, st_dt)
);

-- Table: credits
CREATE TABLE credits (
    task_id VARCHAR(36) NOT NULL PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    tenant_id VARCHAR(36) NOT NULL,
    customer_id VARCHAR(36) NOT NULL,
    project_id VARCHAR(36) NOT NULL,
    task_type VARCHAR(32) NOT NULL,
    task_date_time DATETIME NOT NULL,
    input_size BIGINT,
    output_size BIGINT,
    tokens_used BIGINT,
    model_used VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Table: organizations
CREATE TABLE organizations (
    id CHAR(36) NOT NULL PRIMARY KEY,
    parent_org_id CHAR(36),
    name VARCHAR(255) NOT NULL,
    org_type VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    metadata JSON,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_parent_org_id (parent_org_id)
);

-- Table: user_data
CREATE TABLE user_data (
    u_id CHAR(36) NOT NULL,
    st_dt DATETIME NOT NULL,
    e_dt DATETIME NOT NULL DEFAULT '9999-12-31 00:00:00',
    tenant_id CHAR(36),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    dob DATE,
    gender VARCHAR(1),
    image_path VARCHAR(500),
    account_type_id TINYINT,
    PRIMARY KEY (u_id, st_dt),
    INDEX idx_tenant_id (tenant_id)
);

-- Table: email
CREATE TABLE email (
    email_id CHAR(36) NOT NULL,
    st_dt DATETIME NOT NULL,
    e_dt DATETIME NOT NULL DEFAULT '9999-12-31 00:00:00',
    u_id CHAR(36) NOT NULL,
    email VARCHAR(100) NOT NULL,
    is_verified VARCHAR(1),
    PRIMARY KEY (email_id, st_dt),
    INDEX idx_u_id (u_id),
    INDEX idx_email (email)
);

-- Table: user_org_memberships
CREATE TABLE user_org_memberships (
    id CHAR(36) NOT NULL PRIMARY KEY,
    u_id CHAR(36) NOT NULL,
    org_id CHAR(36) NOT NULL,
    status VARCHAR(32) NOT NULL,
    source VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_u_id (u_id),
    INDEX idx_org_id (org_id)
);
