/**
 * Supabase 客戶端 - 直接連接雲端資料庫
 * 用於手機 APK 直接讀取雲端資料
 */
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = 'https://bshxromrtsetlfjdeggv.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJzaHhyb21ydHNldGxmamRlZ2d2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5OTcyNTcsImV4cCI6MjA4MjU3MzI1N30.YourAnonKeyHere';

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

/**
 * 取得所有股票清單
 */
export async function getAllStocks() {
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
export async function getInstitutionalRankings(type = 'foreign', days = 1, limit = 50) {
    const column = type === 'foreign' ? 'foreign_buy' : type === 'trust' ? 'trust_buy' : 'dealer_buy';

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
 * 取得市場掃描結果
 */
export async function scanStocks(filters = {}) {
    let query = supabase
        .from('stock_snapshot')
        .select('*');

    // 應用篩選條件
    if (filters.minPrice) {
        query = query.gte('close', filters.minPrice);
    }
    if (filters.maxPrice) {
        query = query.lte('close', filters.maxPrice);
    }
    if (filters.minVolume) {
        query = query.gte('volume', filters.minVolume);
    }

    const { data, error } = await query.order('code').limit(100);

    if (error) {
        console.error('Error scanning stocks:', error);
        return [];
    }
    return data || [];
}
