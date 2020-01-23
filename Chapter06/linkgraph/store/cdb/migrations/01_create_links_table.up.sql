CREATE TABLE IF NOT EXISTS links (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	url STRING UNIQUE,
	retrieved_at TIMESTAMP
);
