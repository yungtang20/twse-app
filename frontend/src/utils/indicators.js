// 技術指標計算工具函數

export const calculateMA = (data, p, key = 'close') => {
    const result = [];
    for (let i = 0; i < data.length; i++) {
        if (i < p - 1) { result.push(null); continue; }
        let sum = 0;
        for (let j = 0; j < p; j++) sum += data[i - j][key];
        result.push(sum / p);
    }
    return result;
};

export const calculateKD = (data, kPeriod = 9) => {
    const kArr = [], dArr = [];
    let prevK = 50, prevD = 50;
    for (let i = 0; i < data.length; i++) {
        if (i < kPeriod - 1) { kArr.push(null); dArr.push(null); continue; }
        let highest = data[i].high, lowest = data[i].low;
        for (let j = 0; j < kPeriod; j++) { highest = Math.max(highest, data[i - j].high); lowest = Math.min(lowest, data[i - j].low); }
        const rsv = highest === lowest ? 50 : ((data[i].close - lowest) / (highest - lowest)) * 100;
        const k = (2 / 3) * prevK + (1 / 3) * rsv;
        const d = (2 / 3) * prevD + (1 / 3) * k;
        kArr.push(k); dArr.push(d);
        prevK = k; prevD = d;
    }
    return { k: kArr, d: dArr };
};

export const calculateMACD = (data) => {
    const difArr = [], macdArr = [], oscArr = [];
    const k12 = 2 / 13, k26 = 2 / 27, kSig = 2 / 10;
    let ema12 = 0, ema26 = 0, sig = 0;
    for (let i = 0; i < data.length; i++) {
        const c = data[i].close;
        ema12 = i === 0 ? c : c * k12 + ema12 * (1 - k12);
        ema26 = i === 0 ? c : c * k26 + ema26 * (1 - k26);
        const dif = ema12 - ema26;
        sig = i === 0 ? dif : dif * kSig + sig * (1 - kSig);
        difArr.push(dif); macdArr.push(sig); oscArr.push(dif - sig);
    }
    return { dif: difArr, macd: macdArr, osc: oscArr };
};

export const calculateRSI = (data, p) => {
    const result = [];
    let avgGain = 0, avgLoss = 0;
    for (let i = 0; i < data.length; i++) {
        if (i === 0) { result.push(null); continue; }
        const change = data[i].close - data[i - 1].close;
        const gain = change > 0 ? change : 0, loss = change < 0 ? -change : 0;
        if (i <= p) { avgGain += gain; avgLoss += loss; if (i === p) { avgGain /= p; avgLoss /= p; } result.push(i < p ? null : 100 - (100 / (1 + (avgLoss === 0 ? 100 : avgGain / avgLoss)))); }
        else { avgGain = (avgGain * (p - 1) + gain) / p; avgLoss = (avgLoss * (p - 1) + loss) / p; result.push(100 - (100 / (1 + (avgLoss === 0 ? 100 : avgGain / avgLoss)))); }
    }
    return result;
};

export const calculateMFI = (data, p = 14) => {
    const result = [];
    const mfPositive = [], mfNegative = [];
    for (let i = 0; i < data.length; i++) {
        if (i === 0) { result.push(null); mfPositive.push(0); mfNegative.push(0); continue; }
        const tp = (data[i].high + data[i].low + data[i].close) / 3;
        const tpPrev = (data[i - 1].high + data[i - 1].low + data[i - 1].close) / 3;
        const mf = tp * data[i].value;
        if (tp > tpPrev) { mfPositive.push(mf); mfNegative.push(0); }
        else if (tp < tpPrev) { mfPositive.push(0); mfNegative.push(mf); }
        else { mfPositive.push(0); mfNegative.push(0); }
        if (i < p) { result.push(null); continue; }
        let posSum = 0, negSum = 0;
        for (let j = i - p + 1; j <= i; j++) { posSum += mfPositive[j]; negSum += mfNegative[j]; }
        const mfi = negSum === 0 ? 100 : 100 - (100 / (1 + posSum / negSum));
        result.push(mfi);
    }
    return result;
};

export const calculateVWAP = (data) => {
    const result = [];
    let cumPV = 0, cumV = 0;
    for (let i = 0; i < data.length; i++) {
        const tp = (data[i].high + data[i].low + data[i].close) / 3;
        cumPV += tp * data[i].value;
        cumV += data[i].value;
        result.push(cumV === 0 ? null : cumPV / cumV);
    }
    return result;
};

export const calculateBollinger = (data, p = 20) => {
    const upper = [], mid = [], lower = [];
    for (let i = 0; i < data.length; i++) {
        if (i < p - 1) { upper.push(null); mid.push(null); lower.push(null); continue; }
        let sum = 0, sumSq = 0;
        for (let j = 0; j < p; j++) { sum += data[i - j].close; sumSq += data[i - j].close ** 2; }
        const mean = sum / p;
        const std = Math.sqrt(sumSq / p - mean ** 2);
        mid.push(mean);
        upper.push(mean + 2 * std);
        lower.push(mean - 2 * std);
    }
    return { upper, mid, lower };
};

export const calculateVP = (data, lookback = 20) => {
    const poc = [], vpUpper = [], vpLower = [];
    for (let i = 0; i < data.length; i++) {
        if (i < lookback - 1) { poc.push(null); vpUpper.push(null); vpLower.push(null); continue; }
        const high = Math.max(...data.slice(i - lookback + 1, i + 1).map(d => d.high));
        const low = Math.min(...data.slice(i - lookback + 1, i + 1).map(d => d.low));
        const step = (high - low) / 10 || 1;
        const volAtPrice = new Map();
        for (let j = i - lookback + 1; j <= i; j++) {
            const priceKey = Math.round((data[j].close - low) / step);
            volAtPrice.set(priceKey, (volAtPrice.get(priceKey) || 0) + data[j].value);
        }
        let maxVol = 0, pocKey = 5;
        volAtPrice.forEach((vol, key) => { if (vol > maxVol) { maxVol = vol; pocKey = key; } });
        const pocPrice = low + pocKey * step + step / 2;
        const sortedPrices = [...volAtPrice.entries()].sort((a, b) => b[1] - a[1]);
        const totalVol = sortedPrices.reduce((s, [, v]) => s + v, 0);
        let cumVol = 0, vaKeys = [];
        for (const [key, vol] of sortedPrices) { cumVol += vol; vaKeys.push(key); if (cumVol >= totalVol * 0.7) break; }
        const vaHigh = low + (Math.max(...vaKeys) + 1) * step;
        const vaLow = low + Math.min(...vaKeys) * step;
        poc.push(pocPrice); vpUpper.push(vaHigh); vpLower.push(vaLow);
    }
    return { poc, upper: vpUpper, lower: vpLower };
};

export const calculateVSBC = (data, win = 10) => {
    const upper = [], lower = [];
    for (let i = 0; i < data.length; i++) {
        if (i < win - 1) { upper.push(null); lower.push(null); continue; }
        let signedVolSum = 0, volSum = 0, rangeSum = 0, midSum = 0;
        for (let j = 0; j < win; j++) {
            const d = data[i - j];
            const sign = d.close >= d.open ? 1 : -1;
            signedVolSum += sign * d.value;
            volSum += d.value;
            rangeSum += d.high - d.low;
            midSum += (d.high + d.low) / 2;
        }
        const avgRange = rangeSum / win || 1;
        const baseMid = midSum / win;
        const shift = Math.max(-0.5, Math.min(0.5, signedVolSum / (volSum || 1)));
        const vsbcMid = baseMid + shift * avgRange;
        upper.push(vsbcMid + avgRange * 0.5);
        lower.push(vsbcMid - avgRange * 0.5);
    }
    return { upper, lower };
};

export const calculateADL = (data) => {
    const result = [];
    let adl = 0;
    for (let i = 0; i < data.length; i++) {
        const hl = data[i].high - data[i].low;
        const mfm = hl === 0 ? 0 : ((data[i].close - data[i].low) - (data[i].high - data[i].close)) / hl;
        adl += mfm * data[i].value;
        result.push(adl);
    }
    return result;
};

export const calculateNVI = (data) => {
    const result = [];
    let nvi = 1000;
    for (let i = 0; i < data.length; i++) {
        if (i > 0 && data[i].value < data[i - 1].value) {
            nvi += nvi * ((data[i].close - data[i - 1].close) / data[i - 1].close);
        }
        result.push(nvi);
    }
    return result;
};

export const calculatePVI = (data) => {
    const result = [];
    let pvi = 1000;
    for (let i = 0; i < data.length; i++) {
        if (i > 0 && data[i].value > data[i - 1].value) {
            pvi += pvi * ((data[i].close - data[i - 1].close) / data[i - 1].close);
        }
        result.push(pvi);
    }
    return result;
};

export const calculateSMI = (data, p = 14) => {
    const result = [];
    for (let i = 0; i < data.length; i++) {
        if (i < p - 1) { result.push(null); continue; }
        let highMax = data[i].high, lowMin = data[i].low;
        for (let j = 1; j < p; j++) { highMax = Math.max(highMax, data[i - j].high); lowMin = Math.min(lowMin, data[i - j].low); }
        const midpoint = (highMax + lowMin) / 2;
        const range = highMax - lowMin;
        const smi = range === 0 ? 0 : ((data[i].close - midpoint) / (range / 2)) * 100;
        result.push(smi);
    }
    return result;
};
