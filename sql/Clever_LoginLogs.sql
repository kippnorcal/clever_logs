CREATE TABLE custom.Clever_LoginLogs (
    user_id VARCHAR(MAX),
    ip_address VARCHAR(MAX),
    timestamp DATETIME,
    auth_method VARCHAR(MAX),
    user_type VARCHAR(MAX),
    error VARCHAR(MAX),
    successful BIT,
    attributes VARCHAR(MAX),
    extra_context VARCHAR(MAX),
    email VARCHAR(MAX)
)

