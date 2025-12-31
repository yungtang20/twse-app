from fastapi import APIRouter, Query
from backend.services.db import db_manager
from typing import List, Optional
from pydantic import BaseModel
import sys

router = APIRouter(prefix="/api/rankings", tags=["rankings"])

class RankingItem(BaseModel):
    code: str
    name: str
    close: Optional[float]
    change_pct: Optional[float]
    volume: Optional[int]
    foreign_buy: Optional[int]
    trust_buy: Optional[int]
    dealer_buy: Optional[int]
    total_buy: Optional[int]
    foreign_streak: Optional[int]
    trust_streak: Optional[int]
    dealer_streak: Optional[int]
    foreign_cumulative: Optional[int] = 0
    trust_cumulative: Optional[int] = 0
    dealer_cumulative: Optional[int] = 0
    foreign_cumulative_pct: Optional[float] = 0.0
    trust_cumulative_pct: Optional[float] = 0.0
    dealer_cumulative_pct: Optional[float] = 0.0
    # Holding (Stock)
    foreign_holding_shares: Optional[int] = 0
    foreign_holding_pct: Optional[float] = 0.0
    trust_holding_shares: Optional[int] = 0
    trust_holding_pct: Optional[float] = 0.0

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
    # 雲端模式: 返回空資料 (此功能需要本地 SQLite)
    if db_manager.is_cloud_mode:
        return {
            "success": True, 
            "data": [],
            "total_count": 0,
            "total_pages": 0,
            "current_page": 1,
            "data_date": None
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
