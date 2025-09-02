-- OAuth 2.0 tables migration
-- Run this after initial setup migration

-- OAuth clients table
CREATE TABLE IF NOT EXISTS oauth_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(255) UNIQUE NOT NULL,
    client_secret_hash VARCHAR(255) NOT NULL,
    client_name VARCHAR(255),
    redirect_uris TEXT[],
    grant_types TEXT[],
    scopes TEXT[],
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP
);

CREATE INDEX idx_oauth_clients_client_id ON oauth_clients(client_id);
CREATE INDEX idx_oauth_clients_deleted_at ON oauth_clients(deleted_at);

-- Authorization codes table
CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code VARCHAR(255) PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    user_id UUID NOT NULL,
    scopes TEXT[],
    redirect_uri TEXT,
    code_challenge VARCHAR(255),
    code_challenge_method VARCHAR(10),
    nonce VARCHAR(255),
    state VARCHAR(255),
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_oauth_auth_codes_client_id ON oauth_authorization_codes(client_id);
CREATE INDEX idx_oauth_auth_codes_user_id ON oauth_authorization_codes(user_id);
CREATE INDEX idx_oauth_auth_codes_expires_at ON oauth_authorization_codes(expires_at);

-- Access tokens table (for revocation tracking)
CREATE TABLE IF NOT EXISTS oauth_access_tokens (
    jti VARCHAR(255) PRIMARY KEY,
    user_id UUID NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    scopes TEXT[],
    expires_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_oauth_access_tokens_user_id ON oauth_access_tokens(user_id);
CREATE INDEX idx_oauth_access_tokens_client_id ON oauth_access_tokens(client_id);
CREATE INDEX idx_oauth_access_tokens_expires_at ON oauth_access_tokens(expires_at);
CREATE INDEX idx_oauth_access_tokens_revoked_at ON oauth_access_tokens(revoked_at);

-- Refresh tokens table
CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
    token_hash VARCHAR(255) PRIMARY KEY,
    user_id UUID NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    scopes TEXT[],
    expires_at TIMESTAMP NOT NULL,
    used_at TIMESTAMP,
    revoked_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_oauth_refresh_tokens_user_id ON oauth_refresh_tokens(user_id);
CREATE INDEX idx_oauth_refresh_tokens_client_id ON oauth_refresh_tokens(client_id);
CREATE INDEX idx_oauth_refresh_tokens_expires_at ON oauth_refresh_tokens(expires_at);
CREATE INDEX idx_oauth_refresh_tokens_revoked_at ON oauth_refresh_tokens(revoked_at);

-- Signing keys table for JWT key rotation
CREATE TABLE IF NOT EXISTS oauth_signing_keys (
    kid VARCHAR(255) PRIMARY KEY,
    algorithm VARCHAR(10) NOT NULL,
    public_key TEXT NOT NULL,
    private_key_encrypted TEXT NOT NULL,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    rotated_at TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_oauth_signing_keys_active ON oauth_signing_keys(active);
CREATE INDEX idx_oauth_signing_keys_expires_at ON oauth_signing_keys(expires_at);

-- OAuth audit logs table
CREATE TABLE IF NOT EXISTS oauth_audit_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(50) NOT NULL,
    user_id UUID,
    client_id VARCHAR(255),
    scopes TEXT[],
    ip_address VARCHAR(45),
    user_agent TEXT,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_oauth_audit_logs_event_type ON oauth_audit_logs(event_type);
CREATE INDEX idx_oauth_audit_logs_user_id ON oauth_audit_logs(user_id);
CREATE INDEX idx_oauth_audit_logs_client_id ON oauth_audit_logs(client_id);
CREATE INDEX idx_oauth_audit_logs_created_at ON oauth_audit_logs(created_at);

-- Unity Catalog user mappings
CREATE TABLE IF NOT EXISTS unity_user_mappings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    supabase_user_id UUID NOT NULL UNIQUE,
    unity_username VARCHAR(255) NOT NULL,
    unity_permissions JSONB,
    last_synced TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_unity_user_mappings_supabase_user_id ON unity_user_mappings(supabase_user_id);
CREATE INDEX idx_unity_user_mappings_unity_username ON unity_user_mappings(unity_username);

-- Credential rotation tracking
CREATE TABLE IF NOT EXISTS credential_rotations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    credential_type VARCHAR(50) NOT NULL,
    credential_id VARCHAR(255),
    rotated_by UUID,
    rotation_date TIMESTAMP DEFAULT NOW(),
    next_rotation TIMESTAMP,
    status VARCHAR(20) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_credential_rotations_credential_type ON credential_rotations(credential_type);
CREATE INDEX idx_credential_rotations_next_rotation ON credential_rotations(next_rotation);
CREATE INDEX idx_credential_rotations_status ON credential_rotations(status);

-- Add updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add triggers for updated_at columns
CREATE TRIGGER update_oauth_clients_updated_at BEFORE UPDATE ON oauth_clients
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_unity_user_mappings_updated_at BEFORE UPDATE ON unity_user_mappings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add comments for documentation
COMMENT ON TABLE oauth_clients IS 'OAuth 2.0 client applications';
COMMENT ON TABLE oauth_authorization_codes IS 'Temporary authorization codes for OAuth flow';
COMMENT ON TABLE oauth_access_tokens IS 'Issued access tokens for revocation tracking';
COMMENT ON TABLE oauth_refresh_tokens IS 'Refresh tokens for obtaining new access tokens';
COMMENT ON TABLE oauth_signing_keys IS 'JWT signing keys with rotation support';
COMMENT ON TABLE oauth_audit_logs IS 'Audit trail for all OAuth operations';
COMMENT ON TABLE unity_user_mappings IS 'Maps Supabase users to Unity Catalog users';
COMMENT ON TABLE credential_rotations IS 'Tracks credential rotation history and schedules';