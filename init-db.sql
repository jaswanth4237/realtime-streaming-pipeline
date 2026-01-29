-- init-db.sql
CREATE TABLE page_view_counts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    page_url TEXT NOT NULL,
    view_count BIGINT,
    PRIMARY KEY (window_start, page_url)
);

CREATE TABLE active_users (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    active_user_count BIGINT,
    PRIMARY KEY (window_start)
);

CREATE TABLE user_sessions (
    user_id TEXT PRIMARY KEY,
    session_start_time TIMESTAMP,
    session_end_time TIMESTAMP,
    session_duration_seconds BIGINT
);
