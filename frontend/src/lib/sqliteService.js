/**
 * SQLite 服務 - 手機本地資料庫讀取
 * 支援本地/雲端模式切換
 */
import { CapacitorSQLite, SQLiteConnection } from '@capacitor-community/sqlite';
import { Capacitor } from '@capacitor/core';
import { supabase } from './supabaseClient';

const DB_NAME = 'taiwan_stock.db';

// SQLite 連線實例
let sqlite = null;
let db = null;

/**
 * 檢查是否為原生平台 (Android/iOS)
 */
export function isNativePlatform() {
    return Capacitor.isNativePlatform();
}

/**
 * 取得讀取模式設定
 */
export function getReadSource() {
    return localStorage.getItem('readSource') || 'cloud';
}

/**
 * 設定讀取模式
 */
export function setReadSource(source) {
    localStorage.setItem('readSource', source);
}

/**
 * 初始化 SQLite 連線 (僅原生平台)
 */
export async function initSQLite() {
    if (!isNativePlatform()) {
        console.log('Not native platform, skipping SQLite init');
        return false;
    }

    try {
        sqlite = new SQLiteConnection(CapacitorSQLite);

        // 檢查資料庫是否存在
        const exists = await sqlite.isDatabase(DB_NAME);
        if (!exists.result) {
            console.log('Database not found, need to download first');
            return false;
        }

        // 開啟資料庫連線
        db = await sqlite.createConnection(DB_NAME, false, 'no-encryption', 1, false);
        await db.open();

        console.log('SQLite initialized successfully');
        return true;
    } catch (error) {
        console.error('Failed to init SQLite:', error);
        return false;
    }
}

/**
 * 執行 SQL 查詢 (本地)
 */
async function executeQuery(sql, params = []) {
    if (!db) {
        throw new Error('Database not initialized');
    }
    const result = await db.query(sql, params);
    return result.values || [];
}

/**
 * 取得所有股票清單
 */
export async function getAllStocks() {
    const source = getReadSource();

    if (source === 'local' && isNativePlatform() && db) {
        const sql = `
            SELECT code, name, market_type as market
            FROM stock_meta
            WHERE code GLOB '[0-9][0-9][0-9][0-9]'
            ORDER BY code
        `;
        return await executeQuery(sql);
    }

    // 雲端模式
    const { data, error } = await supabase
        .from('stock_data')
        .select('code, name, market')
        .order('code');

    if (error) {
        console.error('Error fetching stocks:', error);
        return [];
    }
    return data || [];
}

/**
 * 取得單一股票資料
 */
export async function getStockByCode(code) {
    const source = getReadSource();

    if (source === 'local' && isNativePlatform() && db) {
        const sql = `
            SELECT * FROM stock_snapshot WHERE code = ?
        `;
        const results = await executeQuery(sql, [code]);
        return results[0] || null;
    }

    // 雲端模式
    const { data, error } = await supabase
        .from('stock_snapshot')
        .select('*')
        .eq('code', code)
        .single();

    if (error) {
        console.error('Error fetching stock:', error);
        return null;
    }
    return data;
}

/**
 * 取得股票歷史資料
 */
export async function getStockHistory(code, limit = 60) {
    const source = getReadSource();

    if (source === 'local' && isNativePlatform() && db) {
        const sql = `
            SELECT * FROM stock_history 
            WHERE code = ? 
            ORDER BY date_int DESC 
            LIMIT ?
        `;
        const results = await executeQuery(sql, [code, limit]);
        return results.reverse();
    }

    // 雲端模式
    const { data, error } = await supabase
        .from('stock_history')
        .select('*')
        .eq('code', code)
        .order('date_int', { ascending: false })
        .limit(limit);

    if (error) {
        console.error('Error fetching history:', error);
        return [];
    }
    return (data || []).reverse();
}

/**
 * 取得法人買賣超排行
 */
export async function getInstitutionalRankings(type = 'foreign', limit = 50) {
    const source = getReadSource();
    const column = type === 'foreign' ? 'foreign_buy' : type === 'trust' ? 'trust_buy' : 'dealer_buy';

    if (source === 'local' && isNativePlatform() && db) {
        const sql = `
            SELECT code, name, close, change_pct, foreign_buy, trust_buy, dealer_buy
            FROM stock_snapshot
            ORDER BY ${column} DESC
            LIMIT ?
        `;
        return await executeQuery(sql, [limit]);
    }

    // 雲端模式
    const { data, error } = await supabase
        .from('stock_snapshot')
        .select('code, name, close, change_pct, foreign_buy, trust_buy, dealer_buy')
        .order(column, { ascending: false })
        .limit(limit);

    if (error) {
        console.error('Error fetching rankings:', error);
        return [];
    }
    return data || [];
}

/**
 * 從雲端下載資料庫檔案
 */
export async function downloadDatabase(progressCallback) {
    if (!isNativePlatform()) {
        console.log('Not native platform, skipping download');
        return false;
    }

    try {
        progressCallback?.(0, '正在連接雲端...');

        // 從 Supabase Storage 下載資料庫檔案
        const { data, error } = await supabase.storage
            .from('databases')
            .download('taiwan_stock.db');

        if (error) {
            throw error;
        }

        progressCallback?.(50, '正在寫入本地...');

        // 將檔案寫入本地
        const arrayBuffer = await data.arrayBuffer();
        const base64 = btoa(String.fromCharCode(...new Uint8Array(arrayBuffer)));

        await CapacitorSQLite.importFromJson({
            jsonstring: base64
        });

        progressCallback?.(100, '下載完成');
        return true;
    } catch (error) {
        console.error('Failed to download database:', error);
        progressCallback?.(0, '下載失敗: ' + error.message);
        return false;
    }
}
