/**
 * API 服務模塊
 */
const BASE_URL = '/api'

class ApiService {
    async request(endpoint, options = {}) {
        try {
            const response = await fetch(`${BASE_URL}${endpoint}`, {
                headers: {
                    'Content-Type': 'application/json',
                    ...options.headers
                },
                ...options
            })

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`)
            }

            const json = await response.json()
            return json.data
        } catch (error) {
            console.error('API Error:', error)
            throw error
        }
    }

    // 系統狀態
    async getStatus() {
        return this.request('/status')
    }

    // 股票清單
    async getStocks(params = {}) {
        const query = new URLSearchParams(params).toString()
        return this.request(`/stocks?${query}`)
    }

    // 單一股票
    async getStock(code) {
        return this.request(`/stocks/${code}`)
    }

    // 股票歷史
    async getHistory(code, limit = 60) {
        return this.request(`/stocks/${code}/history?limit=${limit}`)
    }

    // 股票指標
    async getIndicators(code) {
        return this.request(`/stocks/${code}/indicators`)
    }

    // 市場掃描
    async scan(type, params = {}) {
        const query = new URLSearchParams(params).toString()
        return this.request(`/scan/${type}?${query}`)
    }

    // 法人排行
    async ranking(entity, direction, limit = 30) {
        return this.request(`/ranking/${entity}-${direction}?limit=${limit}`)
    }
}

export const api = new ApiService()
