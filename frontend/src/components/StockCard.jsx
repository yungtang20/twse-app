import './StockCard.css'

function StockCard({ stock, onClick }) {
    const { code, name, close, change_pct } = stock

    const formatNumber = (num) => {
        if (!num) return '-'
        return num.toLocaleString('zh-TW', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }

    const formatChange = (num) => {
        if (!num) return '-'
        const sign = num >= 0 ? '+' : ''
        return `${sign}${num.toFixed(2)}%`
    }

    const getColorClass = (value) => {
        if (!value) return ''
        return value > 0 ? 'up' : value < 0 ? 'down' : ''
    }

    return (
        <div className="stock-card" onClick={onClick}>
            <div className="stock-info">
                <span className="stock-code">{code}</span>
                <span className="stock-name">{name}</span>
            </div>
            <div className="stock-price">
                <span className={`price ${getColorClass(change_pct)}`}>
                    {formatNumber(close)}
                </span>
                <span className={`change ${getColorClass(change_pct)}`}>
                    {formatChange(change_pct)}
                </span>
            </div>
        </div>
    )
}

export default StockCard
