-- Supabase Schema Update: sync_status table
-- 用於追蹤雲端自動更新狀態

CREATE TABLE IF NOT EXISTS sync_status (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_update TIMESTAMP WITH TIME ZONE,
    status TEXT DEFAULT 'pending',
    message TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 確保只有一筆記錄
INSERT INTO sync_status (id, status) 
VALUES (1, 'pending')
ON CONFLICT (id) DO NOTHING;

-- 啟用 RLS
ALTER TABLE sync_status ENABLE ROW LEVEL SECURITY;

-- 允許讀取
CREATE POLICY "Allow read sync_status" ON sync_status
    FOR SELECT USING (true);

-- 允許服務角色寫入
CREATE POLICY "Allow service write sync_status" ON sync_status
    FOR ALL USING (auth.role() = 'service_role');
