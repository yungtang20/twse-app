// API Configuration
// In development: uses localhost:8000
// In production: uses relative path (same origin)

const isDev = import.meta.env.DEV;

export const API_BASE_URL = isDev ? 'http://localhost:8000' : '';

export const api = {
    get: async (endpoint) => {
        const url = `${API_BASE_URL}${endpoint}`;
        const res = await fetch(url);
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    },
    post: async (endpoint, data) => {
        const url = `${API_BASE_URL}${endpoint}`;
        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        if (!res.ok) throw new Error(`API error: ${res.status}`);
        return res.json();
    }
};

export default API_BASE_URL;
