from fastapi import APIRouter, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import sys
from backend.services.db import db_manager, get_system_status

router = APIRouter(prefix="/api/rankings", tags=["rankings"])

class RankingItem(BaseModel):
    code: str
    name: str
    close: Optional[float] = None
    change_pct: Optional[float] = None
    volume: Optional[int] = None
    foreign_buy: Optional[int] = None
    trust_buy: Optional[int] = None
    dealer_buy: Optional[int] = None
    total_buy: Optional[int] = None
    foreign_streak: Optional[int] = None
    trust_streak: Optional[int] = None
    dealer_streak: Optional[int] = None
    foreign_cumulative: Optional[int] = None
    trust_cumulative: Optional[int] = None
    dealer_cumulative: Optional[int] = None
    foreign_cumulative_pct: Optional[float] = None
    trust_cumulative_pct: Optional[float] = None
    dealer_cumulative_pct: Optional[float] = None
    foreign_holding_shares: Optional[int] = None
    foreign_holding_pct: Optional[float] = None
    trust_holding_shares: Optional[int] = None
    trust_holding_pct: Optional[float] = None

class RankingResponse(BaseModel):
    success: bool
    data: List[RankingItem]
    total_count: int
    total_pages: int
    current_page: int
    data_date: Optional[str] = None

@router.get("/institutional", response_model=RankingResponse)
async def get_institutional_rankings(
    type: str = Query(..., description="foreign, trust, dealer, total"),
    sort: str = Query("buy", description="buy (desc) or sell (asc)"),
    limit: int = Query(30, description="Number of items to return"),
    page: int = Query(1, description="Page number"),
    min_foreign_streak: int = Query(0, description="Min Foreign Streak"),
    min_trust_streak: int = Query(0, description="Min Trust Streak"),
    min_dealer_streak: int = Query(0, description="Min Dealer Streak"),
    sort_by: str = Query(None, description="Column to sort by"),
    direction: str = Query("desc", description="asc or desc"),
    days: int = Query(1, description="Number of days to accumulate")
):
    """
    Get institutional investor rankings.
    """
    # 雲端模式: 從 Supabase 讀取 stock_snapshot
    if db_manager.is_cloud_mode:
        date_str = None
        data = []
        total_count = 0
        
        try:
            # 1. Get Date
            status = get_system_status()
            latest_date = status.get('latest_date')
            if latest_date:
                d = str(latest_date)
                if len(d) == 8:
                    date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            
            # 2. Map sort column
            # type: foreign, trust, dealer, total
            # sort: buy (desc), sell (asc)
            
            col_map = {
                "foreign": "foreign_buy",
                "trust": "trust_buy",
                "dealer": "dealer_buy",
                "total": "volume" # Total buy not directly available in snapshot, fallback to volume or handle separately
            }
            
            target_col = col_map.get(type, "foreign_buy")
            if type == "total":
                # For total, we might need a different approach or just show foreign for now
                # Or if we want to sort by sum, Supabase doesn't support calculated order easily without RPC
                # Let's fallback to foreign_buy for total in cloud mode for now, or volume
                target_col = "foreign_buy" 

            # Determine order
            is_desc = True
            if sort == "sell":
                is_desc = False
            
            # 3. Query Supabase
            if db_manager.supabase:
                query = db_manager.supabase.table('stock_snapshot') \
                    .select('*') \
                    .order(target_col, desc=is_desc) \
                    .order(target_col, desc=is_desc) \
                    .limit(limit)
                
                # Apply filters
                if min_foreign_streak != 0:
                    if min_foreign_streak > 0:
                        query = query.gte('foreign_streak', min_foreign_streak)
                    else:
                        query = query.lte('foreign_streak', min_foreign_streak)
                
                if min_trust_streak != 0:
                    if min_trust_streak > 0:
                        query = query.gte('trust_streak', min_trust_streak)
                    else:
                        query = query.lte('trust_streak', min_trust_streak)
                        
                if min_dealer_streak != 0:
                    if min_dealer_streak > 0:
                        query = query.gte('dealer_streak', min_dealer_streak)
                    else:
                        query = query.lte('dealer_streak', min_dealer_streak)
                
                # Apply simple filters if needed (e.g. exclude 0)
                # query = query.neq(target_col, 0) 
                
                res = query.execute()
                if res.data:
                    data = res.data
                    
                    # Fetch holdings data from institutional_investors for these stocks
                    codes = [item['code'] for item in data]
                    holdings_map = {}
                    if codes and db_manager.supabase:
                        try:
                            # Get latest date first
                            latest_hist = db_manager.supabase.table('stock_history').select('date_int').order('date_int', desc=True).limit(1).execute()
                            if latest_hist.data:
                                latest_date_int = latest_hist.data[0]['date_int']
                                
                                # Query institutional_investors
                                inst_res = db_manager.supabase.table('institutional_investors') \
                                    .select('code, foreign_holding_shares, foreign_holding_pct, trust_holding_shares, trust_holding_pct') \
                                    .in_('code', codes) \
                                    .eq('date_int', latest_date_int) \
                                    .execute()
                                
                                if inst_res.data:
                                    for h in inst_res.data:
                                        holdings_map[h['code']] = h
                        except Exception as e:
                            print(f"Error fetching cloud holdings: {e}")

                    # Add calculated fields expected by frontend
                    for item in data:
                        # Frontend expects total_buy
                        f = item.get('foreign_buy') or 0
                        t = item.get('trust_buy') or 0
                        d = item.get('dealer_buy') or 0
                        item['total_buy'] = f + t + d
                        
                        # Frontend expects change_pct (snapshot has it? yes usually)
                        # If not, calculate? Snapshot should have it.
                        
                        # Frontend expects streak data (might be 0 in cloud)
                        if 'foreign_streak' not in item: item['foreign_streak'] = 0
                        if 'trust_streak' not in item: item['trust_streak'] = 0
                        if 'dealer_streak' not in item: item['dealer_streak'] = 0

                        # Frontend expects cumulative data and percentages
                        if 'foreign_cumulative' not in item: item['foreign_cumulative'] = 0
                        if 'trust_cumulative' not in item: item['trust_cumulative'] = 0
                        if 'dealer_cumulative' not in item: item['dealer_cumulative'] = 0
                        
                        # We don't have total_shares in snapshot, so we can't calculate pct accurately in cloud mode yet.
                        # Set to 0 to avoid frontend crash.
                        if 'foreign_cumulative_pct' not in item: item['foreign_cumulative_pct'] = 0.0
                        if 'trust_cumulative_pct' not in item: item['trust_cumulative_pct'] = 0.0
                        if 'dealer_cumulative_pct' not in item: item['dealer_cumulative_pct'] = 0.0
                        
                        # Merge holdings data
                        h_data = holdings_map.get(item['code'], {})
                        item['foreign_holding_shares'] = h_data.get('foreign_holding_shares') or 0
                        item['foreign_holding_pct'] = h_data.get('foreign_holding_pct') or 0.0
                        item['trust_holding_shares'] = h_data.get('trust_holding_shares') or 0
                        item['trust_holding_pct'] = h_data.get('trust_holding_pct') or 0.0
                        
                    total_count = len(data) # Approximation

        except Exception as e:
            print(f"Cloud mode ranking fetch error: {e}")

        return {
            "success": True, 
            "data": data,
            "total_count": total_count,
            "total_pages": 1,
            "current_page": 1,
            "data_date": date_str
        }
    
    sys.stdout.flush()
    
    # Map type to column name (for sorting/filtering)
    column_map = {
        "foreign": "s.foreign_buy",
        "trust": "s.trust_buy",
        "dealer": "s.dealer_buy",
        "total": "(s.foreign_buy + s.trust_buy + s.dealer_buy)"
    }
    
    target_col = column_map.get(type)
    if not target_col:
        return {"success": False, "data": [], "total_count": 0, "total_pages": 0, "current_page": 1}

    # Map type to streak column
    streak_map = {
        "foreign": "s.foreign_streak",
        "trust": "s.trust_streak",
        "dealer": "s.dealer_streak",
        "total": "0"
    }
    streak_col = streak_map.get(type, "0")

    # No base filter - always show all stocks sorted by value
    # (previously filtered by > 0 for buy, < 0 for sell, causing limited results)
    where_clauses = []

    # Filters
    if min_foreign_streak != 0:
        op = ">=" if min_foreign_streak > 0 else "<="
        where_clauses.append(f"s.foreign_streak {op} {min_foreign_streak}")

    if min_trust_streak != 0:
        op = ">=" if min_trust_streak > 0 else "<="
        where_clauses.append(f"s.trust_streak {op} {min_trust_streak}")

    if min_dealer_streak != 0:
        op = ">=" if min_dealer_streak > 0 else "<="
        where_clauses.append(f"s.dealer_streak {op} {min_dealer_streak}")

    if where_clauses:
        where_clause = " AND ".join(where_clauses)
    else:
        where_clause = "1=1"  # No filter, always true

    # Sorting Logic
    default_order = "DESC" if sort == "buy" else "ASC"
    
    if sort_by:
        valid_sorts = {
            "code": "s.code",
            "close": "s.close",
            "change_pct": "change_pct",
            "volume": "s.volume",
            "foreign": "s.foreign_buy",
            "trust": "s.trust_buy",
            "dealer": "s.dealer_buy",
            "streak": streak_col,
            "amount": f"ABS({target_col} * s.close)",
            # Explicit Streak Sorts
            "foreign_streak": "s.foreign_streak",
            "trust_streak": "s.trust_streak",
            "dealer_streak": "s.dealer_streak",
            # Cumulative Sorts
            "foreign_cumulative": "s.foreign_cumulative",
            "trust_cumulative": "s.trust_cumulative",
            "dealer_cumulative": "s.dealer_cumulative",
            # Cumulative Amount Sorts (ABS value for magnitude)
            "foreign_cumulative_amount": "ABS(s.foreign_cumulative * s.close)",
            "trust_cumulative_amount": "ABS(s.trust_cumulative * s.close)",
            "dealer_cumulative_amount": "ABS(s.dealer_cumulative * s.close)",
            # Holding Sorts
            "foreign_holding": "i.foreign_holding_shares",
            "trust_holding": "i.trust_holding_shares"
        }
        sort_col = valid_sorts.get(sort_by)
        order_clause = f"{sort_col} {direction.upper()}" if sort_col else f"{target_col} {default_order}"
    else:
        order_clause = f"{target_col} {default_order}"

    # Calculate Offset
    offset = (page - 1) * limit

    # Query Construction
    if days > 1:
        # 1. Get cutoff date
        date_sql = f"SELECT DISTINCT date_int FROM stock_history ORDER BY date_int DESC LIMIT {days}"
        dates = db_manager.execute_query(date_sql)
        if not dates:
             return {"success": False, "data": [], "total_count": 0, "total_pages": 0, "current_page": 1}
        cutoff_date = dates[-1]['date_int']

        # Count Query
        count_sql = f"""
        WITH Aggregated AS (
            SELECT 
                code,
                SUM(foreign_buy) as foreign_buy,
                SUM(trust_buy) as trust_buy,
                SUM(dealer_buy) as dealer_buy
            FROM stock_history
            WHERE date_int >= {cutoff_date}
            GROUP BY code
        )
        SELECT COUNT(*) as count
        FROM Aggregated agg
        JOIN stock_snapshot s ON agg.code = s.code
        JOIN stock_meta m ON s.code = m.code
        WHERE m.market_type IN ('TWSE', 'TPEx') 
          AND {where_clause.replace('s.foreign_buy', 'agg.foreign_buy').replace('s.trust_buy', 'agg.trust_buy').replace('s.dealer_buy', 'agg.dealer_buy').replace('foreign_buy', 'agg.foreign_buy').replace('trust_buy', 'agg.trust_buy').replace('dealer_buy', 'agg.dealer_buy')}
        """
        count_res = db_manager.execute_query(count_sql)
        total_count = count_res[0]['count'] if count_res else 0

        # Data Query
        sql = f"""
        WITH Aggregated AS (
            SELECT 
                code,
                SUM(foreign_buy) as foreign_buy,
                SUM(trust_buy) as trust_buy,
                SUM(dealer_buy) as dealer_buy
            FROM stock_history
            WHERE date_int >= {cutoff_date}
            GROUP BY code
        )
        SELECT 
            s.code, s.name, s.close, 
            ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct,
            s.volume,
            agg.foreign_buy, agg.trust_buy, agg.dealer_buy,
            (agg.foreign_buy + agg.trust_buy + agg.dealer_buy) as total_buy,
            s.foreign_streak, s.trust_streak, s.dealer_streak,
            COALESCE(s.foreign_cumulative, 0) as foreign_cumulative,
            COALESCE(s.trust_cumulative, 0) as trust_cumulative,
            COALESCE(s.dealer_cumulative, 0) as dealer_cumulative,
            ROUND(COALESCE(s.foreign_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as foreign_cumulative_pct,
            ROUND(COALESCE(s.trust_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as trust_cumulative_pct,
            ROUND(COALESCE(s.dealer_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as dealer_cumulative_pct,
            0 as foreign_holding_shares,
            0.0 as foreign_holding_pct,
            0 as trust_holding_shares,
            0.0 as trust_holding_pct
        FROM Aggregated agg
        JOIN stock_snapshot s ON agg.code = s.code
        JOIN stock_meta m ON s.code = m.code
        WHERE m.market_type IN ('TWSE', 'TPEx') 
          AND {where_clause.replace('s.foreign_buy', 'agg.foreign_buy').replace('s.trust_buy', 'agg.trust_buy').replace('s.dealer_buy', 'agg.dealer_buy').replace('foreign_buy', 'agg.foreign_buy').replace('trust_buy', 'agg.trust_buy').replace('dealer_buy', 'agg.dealer_buy')}
        ORDER BY {order_clause.replace('s.foreign_buy', 'agg.foreign_buy').replace('s.trust_buy', 'agg.trust_buy').replace('s.dealer_buy', 'agg.dealer_buy').replace('foreign_buy', 'agg.foreign_buy').replace('trust_buy', 'agg.trust_buy').replace('dealer_buy', 'agg.dealer_buy')}
        LIMIT {limit} OFFSET {offset}
        """
    else:
        # Standard 1-day query (from snapshot)
        # Get latest date that has holding data (may be different from latest net buy/sell date)
        date_sql = """
            SELECT MAX(date_int) as max_date FROM institutional_investors 
            WHERE foreign_holding_shares IS NOT NULL AND foreign_holding_shares != 0
        """
        date_res = db_manager.execute_query(date_sql)
        latest_date = date_res[0]['max_date'] if date_res else 0

        # Count Query
        count_sql = f"""
        SELECT COUNT(*) as count
        FROM stock_snapshot s
        JOIN stock_meta m ON s.code = m.code
        WHERE m.market_type IN ('TWSE', 'TPEx') 
          AND {where_clause}
        """
        count_res = db_manager.execute_query(count_sql)
        total_count = count_res[0]['count'] if count_res else 0

        # Data Query with institutional_investors join for holding data
        sql = f"""
        SELECT 
            s.code, s.name, s.close, 
            ROUND((s.close - s.close_prev) / s.close_prev * 100, 2) as change_pct,
            s.volume,
            s.foreign_buy, s.trust_buy, s.dealer_buy,
            (s.foreign_buy + s.trust_buy + s.dealer_buy) as total_buy,
            s.foreign_streak, s.trust_streak, s.dealer_streak,
            COALESCE(s.foreign_cumulative, 0) as foreign_cumulative,
            COALESCE(s.trust_cumulative, 0) as trust_cumulative,
            COALESCE(s.dealer_cumulative, 0) as dealer_cumulative,
            ROUND(COALESCE(s.foreign_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as foreign_cumulative_pct,
            ROUND(COALESCE(s.trust_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as trust_cumulative_pct,
            ROUND(COALESCE(s.dealer_cumulative, 0) * 100.0 / NULLIF(m.total_shares, 0), 2) as dealer_cumulative_pct,
            COALESCE(i.foreign_holding_shares, 0) as foreign_holding_shares,
            COALESCE(i.foreign_holding_pct, 0.0) as foreign_holding_pct,
            COALESCE(i.trust_holding_shares, 0) as trust_holding_shares,
            COALESCE(i.trust_holding_pct, 0.0) as trust_holding_pct
        FROM stock_snapshot s
        JOIN stock_meta m ON s.code = m.code
        LEFT JOIN institutional_investors i ON s.code = i.code AND i.date_int = {latest_date}
        WHERE m.market_type IN ('TWSE', 'TPEx') 
          AND {where_clause}
        ORDER BY {order_clause}
        LIMIT {limit} OFFSET {offset}
        """
    
    try:
        # print(f"Executing SQL: {sql[:200]}...")
        results = db_manager.execute_query(sql)
        total_pages = (total_count + limit - 1) // limit
        # print(f"Query returned {len(results)} results")
        
        # Format date for display (20251229 -> 2025-12-29)
        date_str = None
        if 'latest_date' in dir() and latest_date:
            d = str(latest_date)
            if len(d) == 8:
                date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        
        return {
            "success": True, 
            "data": results,
            "total_count": total_count,
            "total_pages": total_pages,
            "current_page": page,
            "data_date": date_str
        }
    except Exception as e:
        print(f"Ranking query error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "data": [], "total_count": 0, "total_pages": 0, "current_page": 1}
