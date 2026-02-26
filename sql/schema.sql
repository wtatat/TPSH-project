CREATE TABLE IF NOT EXISTS videos (
    id text PRIMARY KEY,
    creator_id text NOT NULL,
    video_created_at timestamptz NOT NULL,
    views_count bigint NOT NULL,
    likes_count bigint NOT NULL,
    comments_count bigint NOT NULL,
    reports_count bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id text PRIMARY KEY,
    video_id text NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    views_count bigint NOT NULL,
    likes_count bigint NOT NULL,
    comments_count bigint NOT NULL,
    reports_count bigint NOT NULL,
    delta_views_count bigint NOT NULL,
    delta_likes_count bigint NOT NULL,
    delta_comments_count bigint NOT NULL,
    delta_reports_count bigint NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_video_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_videos_views_count ON videos(views_count);

CREATE INDEX IF NOT EXISTS idx_video_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_created_at ON video_snapshots(created_at);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_views_count ON video_snapshots(delta_views_count);
CREATE INDEX IF NOT EXISTS idx_video_snapshots_delta_likes_count ON video_snapshots(delta_likes_count);
