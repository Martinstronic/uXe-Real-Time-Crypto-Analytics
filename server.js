// ============================================================================
// server.js  —  Cockpit Backend (Binance Futures)
// ----------------------------------------------------------------------------

const express = require("express");
const axios = require("axios");
const crypto = require("crypto");
const cors = require("cors");
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const WS = require("ws");
const { WebSocketServer } = WS;
const PORT = Number(process.env.PORT) || 3000;



const cacheLiquidados = {}; 

let cacheLiquidacoes = {};

const app = express();
app.use(cors());
app.use(express.static(__dirname));
app.use(express.json());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));



app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  next();
});

// API Binance
const RELAY_SECRET = process.env.RELAY_SECRET || "";
const API_KEY = process.env.API_KEY || "";
const API_SECRET = process.env.API_SECRET || "";

// ===== URLs base da Binance =====
const BINANCE_FUTURES = "https://fapi.binance.com/fapi/v1";
const BINANCE_SPOT = "https://api.binance.com/api/v3";

// 🚀 Token e Chat ID do seu Bot do Telegram
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN || "";; 
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

const TELEGRAM_TOKEN2 = process.env.TELEGRAM_TOKEN2 || "";; 
const TELEGRAM_CHAT_ID2 = process.env.TELEGRAM_CHAT_ID2 || ""; 

const TELEGRAM_TOKEN3 = process.env.TELEGRAM_TOKEN3 || ""; 
const TELEGRAM_TOKEN4 = process.env.TELEGRAM_TOKEN4 || ""; 

const TELEGRAM_CHAT_ID3 = process.env.TELEGRAM_CHAT_ID3 || "";
const TELEGRAM_CHAT_ID4 = process.env.TELEGRAM_CHAT_ID4 || "";

// ================== [  Função de Cache Centralizada ] ==================

// Cache simples em memória (key -> { ts, data })
const CACHE = new Map();

/**
 * Obtém dados com cache.
 * @param {string} key - Identificador único do cache.
 * @param {number} ttlMs - Tempo de vida do cache em ms.
 * @param {Function} fetchFn - Função assíncrona que retorna os dados frescos.
 */
async function getWithCache(key, ttlMs, fetchFn) {
  const agora = Date.now();
  const cached = CACHE.get(key);

  if (cached && (agora - cached.ts) < ttlMs) {
    return cached.data;
  }


  let data;
  if (typeof fetchFn === "function") {
    data = await fetchFn();
  } else if (fetchFn && typeof fetchFn.then === "function") {

    data = await fetchFn;
  } else {
    console.warn(`[getWithCache] fetchFn não é função (tipo: ${typeof fetchFn}) para chave ${key}. Usando valor literal.`);
    data = fetchFn; // valor literal
  }

  CACHE.set(key, { ts: agora, data });
  return data;
}

function limparCache(expMs = 60_000) {
  const agora = Date.now();
  for (const [k, v] of CACHE.entries()) {
    if (agora - v.ts > expMs) {
      CACHE.delete(k);
    }
  }
}


setInterval(() => limparCache(300_000), 300_000);





const ALERTAS_ATIVOS_FILE = "./alertasAtivos.json";

let alertasAtivosPersist = [];
if (fs.existsSync(ALERTAS_ATIVOS_FILE)) {
  try {
    alertasAtivosPersist = JSON.parse(fs.readFileSync(ALERTAS_ATIVOS_FILE, "utf8"));
  } catch { alertasAtivosPersist = []; }
}

app.get("/alertas-ativos", (req,res)=>{
  res.json(alertasAtivosPersist);
});

app.post("/alertas-ativos", (req,res)=>{
  alertasAtivosPersist = req.body;
  fs.writeFileSync(ALERTAS_ATIVOS_FILE, JSON.stringify(alertasAtivosPersist, null, 2));
  res.json({ok:true});
});

let alertasSalvos = {};

app.get("/alertas-preco", (req, res) => {
  res.json(alertasSalvos);
});

app.post("/alertas-preco", express.json(), (req, res) => {
  alertasSalvos = req.body || {};
  res.json({ status: "ok" });
});


/* =========================[ AXIOS + USER-AGENT + PAG INICIAL ]========================== */
const api = axios.create({
  headers: {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
  },
  timeout: 25_000
});



// guarda últimas requisições por 60s para diagnóstico
let historicoReqDetalhado = [];
api.interceptors.request.use((config) => {
  let endpoint = config.url || "";
  try { endpoint = new URL(endpoint, "https://dummy.com").pathname; } catch {}
  historicoReqDetalhado.push({ endpoint, ts: Date.now() });
  return config;
});
setInterval(() => {
  const agora = Date.now();
  historicoReqDetalhado = historicoReqDetalhado.filter(r => agora - r.ts <= 60_000);

}, 60_000);

// =========================[ CONFIGURAÇÕES DE EVENTOS GLOBAIS ]=========================

const eventosGlobais = {};
const EVENTOS_GLOB_MAX = 50;
const EVENTO_MAX_MS = 5 * 60 * 1000; 

function limparEventosAntigos() {
  const agora = Date.now();
  for (const symbol in eventosGlobais) {
    eventosGlobais[symbol] = eventosGlobais[symbol].filter(ev => agora - ev.ts <= EVENTO_MAX_MS);
  }
}
setInterval(limparEventosAntigos, 60_000); 





function safeNum(v, fallback = 0) {
  if (v === null || v === undefined) return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}


function toPct(v) {
  return safeNum(v, 0);
}



// =========================[ CONFIGURAÇÕES DE RADAR ]=========================
const MAX_ATIVOS_RADAR = 50;  
const LIMIT_PRIORITARIOS = 5; 
const LIMIT_SECUNDARIOS = MAX_ATIVOS_RADAR - LIMIT_PRIORITARIOS;
const STICKY_DURATION_MINUTES = 30 
const stickySymbols = new Map();
let top100Volume = new Set();
let top50Volume = new Set();
const symbolsExpandido = new Map();

function ativarStreamsExpandido(symbol, minutos = 30) {
  const expira = Date.now() + minutos * 60 * 1000;
  symbolsExpandido.set(symbol, expira);
  console.log(`⚡ Streams expandidos ativados para ${symbol} até ${new Date(expira).toLocaleTimeString()}`);
  //recomputeStreams(); 
}

function isStreamExpandido(symbol) {
  const exp = symbolsExpandido.get(symbol);
  if (!exp) return false;
  if (Date.now() > exp) {
    symbolsExpandido.delete(symbol);
    return false;
  }
  return true;
}

// limpeza automática a cada minuto
setInterval(() => {
  const agora = Date.now();
  for (const [sym, exp] of symbolsExpandido.entries()) {
    if (agora > exp) symbolsExpandido.delete(sym);
  }
}, 60_000);


function promoteSymbol(symbol, minutes = 30) {
  const expireAt = Date.now() + minutes * 60 * 1000;
  stickySymbols.set(symbol, expireAt);
  console.log(`📌 Sticky adicionado: ${symbol} por ${minutes}min`);
}

function getStickySymbols() {
  const now = Date.now();
  const ativos = [];
  for (const [s, exp] of stickySymbols.entries()) {
    if (exp > now) {
      ativos.push(s);
    } else {
      stickySymbols.delete(s);
      console.log(`⏳ Sticky expirado: ${s}`);
    }
  }
  return ativos;
}


let cacheTopRadar = { ts: 0, dados: { prioritarios: [], secundarios: [] } };

async function fetchJSON(url, fallback = null) {
  try {
    const res = await axios.get(url, {
      timeout: 20_000,
      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });
    return res.data;
  } catch (err) {
    console.error(`❌ Erro em fetchJSON ${url}:`, err.message);
    return fallback;
  }
}

const marketSupportCache = new Map(); // symbol -> { futures: true/false, spot: true/false }
const invalidSymbols = new Set();

async function fetchKlinesWithFallback(symbol, interval, preferredMarket = "futures", limit = 60) {
  if (invalidSymbols.has(symbol)) return [];

  const known = marketSupportCache.get(symbol) || { futures: true, spot: true };

  const attempts =
    preferredMarket === "futures"
      ? [
          {
            name: "futures",
            enabled: known.futures,
            url: `${BINANCE_FUTURES}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
          },
          {
            name: "spot",
            enabled: known.spot,
            url: `${BINANCE_SPOT}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
          }
        ]
      : [
          {
            name: "spot",
            enabled: known.spot,
            url: `${BINANCE_SPOT}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
          },
          {
            name: "futures",
            enabled: known.futures,
            url: `${BINANCE_FUTURES}/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`
          }
        ];

  for (const attempt of attempts) {
    if (!attempt.enabled) continue;

    try {
      const response = await fetch(attempt.url, {
        headers: {
          "User-Agent": "Mozilla/5.0"
        }
      });

      if (!response.ok) {
        const status = response.status;

        if (status === 400 || status === 418) {
          const cur = marketSupportCache.get(symbol) || { futures: true, spot: true };
          cur[attempt.name] = false;
          marketSupportCache.set(symbol, cur);

          if (!cur.futures && !cur.spot) {
            invalidSymbols.add(symbol);
            console.warn(`🚫 Símbolo invalidado para klines: ${symbol}`);
          }
        }

        throw new Error(`HTTP ${status}`);
      }

      const data = await response.json();
      if (Array.isArray(data) && data.length > 0) {
        return data;
      }
    } catch (err) {
      console.error(`❌ fetchKlinesWithFallback (${symbol}, ${attempt.name}) ${err.message}`);
    }
  }

  return [];
}


async function fetchWithBackoff(url, opts = {}, retries = 3) {
  for (let i = 0; i <= retries; i++) {
    try {
      const resp = await axios({ url, ...opts, timeout: 20000 });
      return resp.data;
    } catch (err) {
      const status = err?.response?.status;
      if (i === retries) throw err;
      const wait = 1500 * Math.pow(2, i) + Math.round(Math.random()*200);
      console.warn(`⏳ fetchWithBackoff ${url} failed (${status || err.message}). retry em ${Math.round(wait/1000)}s`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}



const klinesCache = new Map();

/**
 * Busca candles com cache + fallback spot/futures
 * @param {string} symbol - Ex: BTCUSDT
 * @param {string} interval - Ex: "5m"
 * @param {string} prefer - "futures" ou "spot"
 * @param {number} limit - quantidade de candles
 * @param {number} ttlMs - tempo de vida do cache em ms 
 */
async function fetchKlinesCached(symbol, interval, prefer = "futures", limit = 2, ttlMs = 30_000) {
  const key = `klines-${symbol}-${interval}-${prefer}-${limit}`;
  const now = Date.now();

  
  const cached = klinesCache.get(key);
  if (cached && (now - cached.ts) < ttlMs) {
    return cached.data;
  }

  try {
    const data = await fetchKlinesWithFallback(symbol, interval, prefer);
    if (data) {
      klinesCache.set(key, { ts: now, data });
    }
    return data;
  } catch (err) {
    console.error(`❌ fetchKlinesCached erro ${symbol} ${interval}:`, err.message);
    return null;
  }
}


// ======= SCORE LOTE UNIFICADO =======

// Gera sparkline simples (últimos 10 closes 5m)
function gerarSpark(hist) {
  if (!hist || !Array.isArray(hist) || hist.length < 2) return [];
  return hist.slice(-30).map(v => Number(v) || 0);
}

// Detectar capitulação
function detectarCap(longVal, shortVal, fator = 20) {
  if (!longVal || !shortVal) return { capLong: false, capShort: false };
  if (longVal > shortVal * fator) return { capLong: true, capShort: false };
  if (shortVal > longVal * fator) return { capLong: false, capShort: true };
  return { capLong: false, capShort: false };
}


// DEBUG toggle
const DEBUG_PRESSURE = true;

// =======================================================
// 📊 calcularPressure ( retorna 0..100)
// =======================================================
/**
 * calcularPressure
 * @param {number[]} long5Vals  - notional values (USD) das long em 5m
 * @param {number[]} short5Vals - notional values (USD) das short em 5m
 * @param {number[]} long1hVals - notional values (USD) das long em 1h
 * @param {number[]} short1hVals- notional values (USD) das short em 1h
 * @param {number} varPrice5m - variação de preço 5m em % (ex: 1.23)
 * @param {number} varOIpct  - variação OI em % (ex: 2.5)
 * @param {string} dbgSymbol - (opcional) para log debug
 * @returns {number} pressureIndex (0..100)
 */

function calcularPressure(long5Vals = [], short5Vals = [], long1hVals = [], short1hVals = [], varPrice5m = 0, varOIpct = 0, dbgSymbol = "") {
  try {
    
    const sum = arr => (Array.isArray(arr) ? arr.reduce((a,b)=>a+safeNum(b,0),0) : 0);

    const long5 = sum(long5Vals);
    const short5 = sum(short5Vals);
    const long1h = sum(long1hVals);
    const short1h = sum(short1hVals);

    
    const weight5 = 0.7;
    const weight1h = 0.3;

    const weightedLong  = long5 * weight5 + long1h * weight1h;
    const weightedShort = short5 * weight5 + short1h * weight1h;

    const total = weightedLong + weightedShort;
   
    if (!total || total < 1) {
      return 0;
    }

   
    const baseRatio = (weightedLong - weightedShort) / total; 

    
    const priceFactor = Math.tanh(safeNum(varPrice5m,0) / 5); 
    const oiFactor    = Math.tanh(safeNum(varOIpct,0) / 5);

   
    const combined = baseRatio * 0.7 + priceFactor * 0.2 + oiFactor * 0.1;

    
    const index = Math.round(50 + combined * 50);

    
    const out = Math.max(0, Math.min(100, index));

    

    return out;
  } catch (err) {
    console.error("calcularPressure error:", err && err.stack ? err.stack : err);
    return "";
  }
}

/* =============================[  UTILITÁRIOS ]============================== */
function simboloValido(symbol) {
return /^[A-Z0-9]{1,15}$/.test(symbol);
}


/* =============================[  ENDPOINTS BASE ]=========================== */

app.get("/status", (req, res) => {
  res.json({ status: "ativo", timestamp: Date.now() });
});

// Telegram
app.get("/alerta", async (req, res) => {
  const mensagem = req.query.mensagem;
  if (!mensagem) return res.status(400).send("Mensagem ausente");
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT_ID) {
    return res.status(200).send("Telegram desativado (sem credenciais).");
  }
  try {
    await api.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text: mensagem
    });
    res.send("Enviado");
  } catch (e) {
    res.status(500).json({ erro: "Erro ao enviar", detalhes: e.message });
  }
});

/* ===========================[ 8) PREÇO SPOT CACHED ]========================== */
app.get("/preco/:symbol", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();

  try {
    const precoData = await getPrecoInterno(symbol);

    if (!precoData.preco) {
      return res.status(404).json({
        symbol,
        preco: 0,
        error: "Preço ainda não disponível em cache"
      });
    }

    res.json(precoData);
  } catch (err) {
    console.error(`❌ /preco ${symbol}:`, err.message);
    res.status(500).json({ error: "Erro ao obter preço", details: err.message });
  }
});

//  [ RSI WILDER OTIMIZADO ] 
const rsiCache = {};
let rsiQueues = { high: new Set(), mid: new Set(), low: new Set() };
let rsiQueueProcessing = false;
let lastRsiProcessTs = 0;
const cacheRSI = new Map();
const precoCache = new Map(); 

// =====================[ HELPERS INTERNOS ]======================

function getPrecoFromCaches(symbol) {
  const s = String(symbol || "").toUpperCase();
  const tickerWs = cacheTicker?.get?.(s);
  const precoWs = precoCache?.get?.(s);

  const preco =
    Number(tickerWs?.price ?? precoWs ?? 0) || 0;

  return {
    symbol: s,
    preco,
    source: tickerWs?.price != null ? "ws:ticker" : (precoWs != null ? "ws:precoCache" : "indisponivel"),
    ts: Date.now()
  };
}

async function getPrecoInterno(symbol) {
  const out = getPrecoFromCaches(symbol);
  return out;
}

async function getFundingInterno(symbol) {
  const s = String(symbol || "").toUpperCase();
  try {
    return await getWithCache(
      `funding-${s}`,
      7_200_000,
      async () => {
        try {
          const url = `${BINANCE_FUTURES}/premiumIndex?symbol=${s}`;
          const resp = await fetchWithBackoff(url, {}, 2);
          return {
            fundingRate: Number(resp?.lastFundingRate ?? resp?.fundingRate ?? 0),
            ts: Date.now()
          };
        } catch {
          return { fundingRate: 0, ts: Date.now() };
        }
      }
    );
  } catch {
    return { fundingRate: 0, ts: Date.now() };
  }
}

async function getLSRInterno(symbol) {
  const s = String(symbol || "").toUpperCase();
  try {
    let data = getLSRFromCache(s);
    if (!data) {
      data = await fetchLSR(s);
      setLSRCache(s, data);
    }
    return data;
  } catch {
    return { symbol: s, longShortRatio: 0, hist: [] };
  }
}

async function getLSARInterno(symbol) {
  const s = String(symbol || "").toUpperCase();
  try {
    let data = getLSARFromCache(s);
    if (!data) {
      data = await fetchLSAR(s);
      setLSARCache(s, data);
    }
    return data;
  } catch {
    return { symbol: s, ratio: 0, hist: [] };
  }
}

async function getLSPRInterno(symbol) {
  const s = String(symbol || "").toUpperCase();
  try {
    let data = getLSPRFromCache(s);
    if (!data) {
      data = await fetchLSPR(s);
      setLSPRCache(s, data);
    }
    return data;
  } catch {
    return { symbol: s, ratio: 0, hist: [] };
  }
}

async function getVolumeInterno(symbol, interval) {
  const s = String(symbol || "").toUpperCase();
  try {
    let data = getVolumeFromCache(s, interval);
    if (!data) {
      data = await fetchVolume(s, interval);
      setVolumeCache(s, interval, data);
    }
    return data;
  } catch {
    return { volume: 0 };
  }
}

async function getVariacaoInterno(symbol, interval) {
  const s = String(symbol || "").toUpperCase();
  try {
    const data = await fetchKlinesCached(s, interval, "futures", 10);
    if (!data?.length || data.length < 2) {
      return { value: "n/d", up: true, hist: [] };
    }

    const candles = data.map(c => ({
      open: parseFloat(c[1]),
      close: parseFloat(c[4])
    }));

    const prev = candles.at(-2);
    const value = prev.open > 0 ? ((prev.close - prev.open) / prev.open) * 100 : 0;
    const hist = candles.map(c => ((c.close - c.open) / c.open) * 100);

    return {
      value: value.toFixed(2),
      up: prev.close >= prev.open,
      hist: hist.map(v => v.toFixed(2))
    };
  } catch {
    return { value: "n/d", up: true, hist: [] };
  }
}

function getForcaRsiBtcInterno(symbol) {
  const s = String(symbol || "").toUpperCase();

  if (!cacheRSI.get(`BTCUSDT:1m`)) enqueueRSI("BTCUSDT", "high");
  if (!cacheRSI.get(`${s}:1m`)) enqueueRSI(s, "high");

  const result = {};
  const deltaRSI = {};
  const intervals = ["1m","5m","15m","30m","1h","4h","1d"];

  for (const int of intervals) {
    const ativoData = cacheRSI.get(`${s}:${int}`);
    const btcData   = cacheRSI.get(`BTCUSDT:${int}`);

    const rsiAtivo = ativoData?.rsi ?? null;
    const rsiBTC   = btcData?.rsi ?? null;

    if (typeof rsiAtivo === "number" && typeof rsiBTC === "number") {
      let delta = rsiAtivo - rsiBTC;
      if (delta > 100) delta = 100;
      if (delta < -100) delta = -100;

      result[int] = +delta.toFixed(2);
      deltaRSI[int] = +(rsiAtivo - rsiBTC).toFixed(2);
    } else {
      result[int] = null;
      deltaRSI[int] = null;
    }
  }

  return { symbol: s, base: "BTCUSDT", forcaRSI: result, deltaRSI };
}

async function getOiLoteInterno(symbols = []) {
  const results = {};

  for (const raw of symbols) {
    const symbol = String(raw || "").toUpperCase();
    try {
      const cached = getOiFromCache(symbol);
      if (cached) {
        results[symbol] = cached;
      } else {
        const data = await fetchOiCompleto(symbol);
        setOiCache(symbol, data);
        results[symbol] = data;
      }

      await new Promise(r => setTimeout(r, 250));
    } catch {
      results[symbol] = {
        symbol,
        sumOpenInterest: 0,
        oiUsd: 0,
        hist: [],
        hist1h: [],
        var1m: 0,
        var3m: 0,
        var5m: 0,
        var15m: 0,
        var1h: 0
      };
    }
  }

  return results;
}

async function getVolumeLoteInterno(symbols = [], interval) {
  const results = {};

  await processarEmLotes(symbols, 5, 600, async (s) => {
    const symbol = String(s || "").toUpperCase();
    let data = getVolumeFromCache(symbol, interval);
    if (!data) {
      data = await fetchVolume(symbol, interval);
      setVolumeCache(symbol, interval, data);
    }
    results[symbol] = data;
  });

  return results;
}



function sendVisualHistoryToClient(ws, symbol, tf = VISUAL_BOOTSTRAP_TF, limit = VISUAL_BOOTSTRAP_LIMIT) {
  try {
    const s = String(symbol || "").toUpperCase();
    const hist = cacheCandles[s]?.[tf];
    if (!Array.isArray(hist) || hist.length === 0) return false;

    ws.send(JSON.stringify({
      type: "candle_hist",
      symbol: s,
      tf,
      candles: hist.slice(-limit).map(c => ({
        open: c.open,
        close: c.close,
        vol: c.vol,
        exp: Number.isFinite(c.exp) ? c.exp : 50,
        volRel: Number.isFinite(c.volRel) ? c.volRel : 1,
        ts: c.ts
      }))
    }));


    const ult = cacheCandles[symbol]?.[VISUAL_BOOTSTRAP_TF]?.slice(-1)[0];
if (ult) {
  try {
    ws.send(JSON.stringify({
      type: "candle",
      symbol,
      tf: VISUAL_BOOTSTRAP_TF,
      open: ult.open,
      close: ult.close,
      vol: ult.vol,
      ts: ult.ts,
      exp: Number.isFinite(ult.exp) ? ult.exp : 50,
      volRel: Number.isFinite(ult.volRel) ? ult.volRel : 1
    }));
  } catch {}
}

    return true;
  } catch (err) {
    console.warn(`⚠️ sendVisualHistoryToClient falhou ${symbol}:`, err.message || err);
    return false;
  }
}





// TTLs por faixa de prioridade (ms)
const RSI_TTLS_PRIORITY = {
high: { "1m": 40_000, "5m": 120_000, "15m": 180_000, "30m": 240_000, "1h": 360_000, "4h": 1_800_000, "1d": 3_600_000 },
mid: { "1m": 45_000, "5m": 150_000, "15m": 230_000, "30m": 300_000, "1h": 380_000, "4h": 3_600_000, "1d": 7_200_000 },
low: { "1m": 60_000, "5m": 180_000, "15m": 300_000, "30m": 600_000, "1h": 900_000, "4h": 7_200_000, "1d": 14_400_000 }
};



const RSI_QUEUE_DELAY_MS = 300; 
const RSI_CYCLE = ["high","high","high","high","mid","high","mid","low"]; 


function setRsiCacheEntry(symbol, interval, value, ttlMs, hist = [], extra = {}) {
const key = `${symbol}-${interval}`;
rsiCache[key] = {
value,
hist,
ts: Date.now(),
expiresAt: Date.now() + (ttlMs || 60_000),
avgGain: extra.avgGain ?? null,
avgLoss: extra.avgLoss ?? null,
lastClose: extra.lastClose ?? null
};
}


function getRsiCacheEntry(symbol, interval) {
const key = `${symbol}-${interval}`;
const e = rsiCache[key];
if (!e) return null;
if (e.expiresAt && Date.now() > e.expiresAt) {
delete rsiCache[key];
return null;
}
return e;
}

function calcularRSI(closes, period = 14) {
  if (closes.length < period) return null;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gains += diff;
    else losses -= diff;
  }
  const avgGain = gains / period;
  const avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return +(100 - 100 / (1 + rs)).toFixed(2);
}

// Wilder RSI implementation returning extras
function calcularRSI_Wilder(closes, period = 14) {
if (!Array.isArray(closes) || closes.length < period + 1) return null;
let gains = 0, losses = 0;
for (let i = 1; i <= period; i++) {
const diff = closes[i] - closes[i - 1];
if (diff > 0) gains += diff; else losses += Math.abs(diff);
}
let avgGain = gains / period;
let avgLoss = losses / period;


for (let i = period + 1; i < closes.length; i++) {
const diff = closes[i] - closes[i - 1];
const gain = diff > 0 ? diff : 0;
const loss = diff < 0 ? Math.abs(diff) : 0;
avgGain = (avgGain * (period - 1) + gain) / period;
avgLoss = (avgLoss * (period - 1) + loss) / period;
}
if (avgLoss === 0) {
return { value: 100, avgGain, avgLoss, lastClose: closes.at(-1) };
}
const rs = avgGain / avgLoss;
return { value: +(100 - 100 / (1 + rs)).toFixed(2), avgGain, avgLoss, lastClose: closes.at(-1) };
}


async function fetchKlinesPrefer(symbol, tf, prioridade) {
  return await fetchKlinesWithFallback(symbol, tf, "futures", 60);
}

// =======================[ Telegram ]=======================

//Mensagem de Volume Nível 3 a 5
async function sendTelegramAlert(msg) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID,
      text: msg
    });
  } catch (err) {
    console.error("Erro ao enviar alerta Telegram 1:", err.message);
  }
}

//Mensagem de OI Nível 3 a 5
async function sendTelegramAlert2(msg) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN2}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID2,
      text: msg
    });
  } catch (err) {
    console.error("Erro ao enviar alerta Telegram 2:", err.message);
  }
}

//Mensagem de Volume Nível 1 e 2
async function sendTelegramAlert3(msg) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN3}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID3,
      text: msg
    });
  } catch (err) {
    console.error("Erro ao enviar alerta Telegram 3:", err.message);
  }
}


//Mensagem de OI Nível 1 e 2 
async function sendTelegramAlert4(msg) {
  try {
    await axios.post(`https://api.telegram.org/bot${TELEGRAM_TOKEN4}/sendMessage`, {
      chat_id: TELEGRAM_CHAT_ID4,
      text: msg
    });
  } catch (err) {
    console.error("Erro ao enviar alerta Telegram 4:", err.message);
  }
}





// Atualizar RSI com exp + volRel + fallback


async function atualizarRSI(symbol, prioridade = "mid") {
  try {
    const ttlMap = RSI_TTLS_PRIORITY[prioridade] || RSI_TTLS_PRIORITY.mid;
    const tfList = ["1m","5m","15m","30m","1h","4h","1d"];

    for (const tf of tfList) {
      
      if (getRsiCacheEntry(symbol, tf)) continue;

      //  tenta pegar dados com fallback (futures + spot)
      const kl = await fetchKlinesPrefer(symbol, tf, prioridade);
      if (!kl || kl.length === 0) continue;

      const closes = kl.map(k => parseFloat(k[4])).filter(Number.isFinite);
      if (closes.length < 15) continue;

      const rsiObj = calcularRSI_Wilder(closes);
      if (!rsiObj) continue;

      const rsi = calcularRSI(closes, 14);

      //  Volume relativo (vol atual / média vol últimos 20 candles)
      const volsArr = kl.map(k => parseFloat(k[5])).filter(Number.isFinite);
      const volMA = volsArr.slice(-20).reduce((a,b) => a+b, 0) / Math.max(1, Math.min(volsArr.length,20));
      const volRel = volMA > 0 ? volsArr.at(-1) / volMA : 1;

      // Exposição (exp) = (RSI asset / RSI BTC) * 50
      let exp = null;
      if (symbol !== "BTCUSDT") {
        const btcKey = `BTCUSDT:${tf}`;
        const btcData = cacheRSI.get(btcKey);
        if (btcData?.rsi) {
          exp = (rsi / btcData.rsi) * 50;
        }
      }

      // Salva no cache global (com exp + volRel)
      const cacheKey = `${symbol}:${tf}`;
      cacheRSI.set(cacheKey, {
        ts: Date.now(),
        rsi,
        exp,
        volRel,
        via: kl?.type ?? "spot",
        avgGain: rsiObj.avgGain,
        avgLoss: rsiObj.avgLoss,
        lastClose: rsiObj.lastClose,
        hist: closes.slice(-200) // histórico de closes
      });

            
      setRsiCacheEntry(symbol, tf, rsiObj.value, ttlMap[tf], closes.slice(-200), {
        avgGain: rsiObj.avgGain,
        avgLoss: rsiObj.avgLoss,
        lastClose: rsiObj.lastClose
      });
    }
  } catch (err) {
    console.error(`❌ Falha atualizarRSI ${symbol}:`, err && err.stack ? err.stack : err);
  }
}



function enqueueRSI(symbol, prioridade = 'high') {
if (!symbol) return;
prioridade = ['high','mid','low'].includes(prioridade) ? prioridade : 'mid';



const tf = '1m';
if (getRsiCacheEntry(symbol, tf)) return;



for (const q of Object.keys(rsiQueues)) {
if (q !== prioridade) rsiQueues[q].delete(symbol);
}
rsiQueues[prioridade].add(symbol);
// start process
processRsiQueues().catch(e => console.error('RSI queue error:', e));
}



async function processRsiQueues() {
if (rsiQueueProcessing) return;
rsiQueueProcessing = true;
try {
while (true) {

if (rsiQueues.high.size === 0 && rsiQueues.mid.size === 0 && rsiQueues.low.size === 0) break;


for (const prioridade of RSI_CYCLE) {
const setQ = rsiQueues[prioridade];
if (!setQ || setQ.size === 0) continue;

const [symbol] = setQ;
if (!symbol) continue;
setQ.delete(symbol);
lastRsiProcessTs = Date.now();
try {
await atualizarRSI(symbol, prioridade);
} catch (err) {
console.error('Erro processando RSI', symbol, err && err.stack ? err.stack : err);
}

await new Promise(r => setTimeout(r, RSI_QUEUE_DELAY_MS));
}

await new Promise(r => setTimeout(r, RSI_QUEUE_DELAY_MS));
}
} finally {
rsiQueueProcessing = false;
}
}



setInterval(() => {

const hasPending = rsiQueues.high.size + rsiQueues.mid.size + rsiQueues.low.size > 0;
if (hasPending && !rsiQueueProcessing && (Date.now() - lastRsiProcessTs > 10_000)) {
console.warn('⚠️ Watchdog: restarting RSI queue processor');
processRsiQueues().catch(e => console.error('RSI queue restart failed', e));
}

const topList = Array.from(top50Volume || []).slice(0, 30); 
  for (const s of topList) {
    enqueueRSI(s, 'high'); 
  }

}, 60_000);





app.get('/rsi-completo/:symbol/:interval', async (req,res) => {
try {
const { symbol, interval } = req.params;
const entry = getRsiCacheEntry(symbol, interval);
if (entry) return res.json({ symbol, interval, value: entry.value, hist: entry.hist || [], ts: entry.ts || null });



if (['BTCUSDT','ETHUSDT'].includes(symbol)) enqueueRSI(symbol, 'high');
else enqueueRSI(symbol, 'mid');


return res.json({ symbol, interval, value: null, hist: [], ts: null });
} catch (err) {
console.error('/rsi-completo erro', err && err.stack ? err.stack : err);
res.status(500).json({ error: String(err) });
}
});



app.get('/forca-rsi-btc/:symbol', async (req,res) => {
  try {
    const symbol = String(req.params.symbol || "").toUpperCase();
    const data = getForcaRsiBtcInterno(symbol);
    res.json(data);
  } catch (err) {
    console.error('/forca-rsi-btc erro', err && err.stack ? err.stack : err);
    res.status(500).json({ error: String(err) });
  }
});



function primeRsiForList(listSymbols, prioridade = 'high') {
for (const s of listSymbols) enqueueRSI(s, prioridade);
}



module.exports._rsi = { rsiCache, enqueueRSI, primeRsiForList };

/* ==========================[ FUNDING RATE ]========================= */
app.get("/funding/:symbol", async (req, res) => {
  const symbol = (req.params.symbol || "").toUpperCase();
  const out = await getFundingInterno(symbol);
  res.json(out);
});

// =====================[ OI CACHE GLOBAL ]======================
const oiCache = new Map(); 


function getOiFromCache(symbol, ttl = 45_000) {
  const item = oiCache.get(symbol);
  if (item && (Date.now() - item.ts) < ttl) {
  
    return item.data;
  }

  return null;
}


function setOiCache(symbol, data) {
  oiCache.set(symbol, { data, ts: Date.now() });
}


const oiNowHistory = {}; 


function atualizarOiNowHistory(symbol, oi) {
  if (!oiNowHistory[symbol]) oiNowHistory[symbol] = [];
  oiNowHistory[symbol].push({ ts: Date.now(), oi });

  
  if (oiNowHistory[symbol].length > 30) {
    oiNowHistory[symbol].shift();
  }
}

function calcularVarNow(symbol, minutos) {
  const arr = oiNowHistory[symbol] || [];
  if (arr.length < 2) return 0;

  const cutoff = Date.now() - minutos * 60_000;
  const atual = arr.at(-1);
  const antigo = arr.find(d => d.ts <= cutoff);

  if (atual && antigo && antigo.oi > 0) {
    return ((atual.oi - antigo.oi) / antigo.oi) * 100;
  }
  return 0;
}
// =====================[ CRONJOB OI NOW HISTORY - TIERS ]======================


function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}


async function processarBatch(symbols) {
  if (!symbols.length) return;

  await Promise.allSettled(symbols.map(async (symbol) => {
    try {
      let oiData = getOiFromCache(symbol);
      if (!oiData) {
        oiData = await fetchOiCompleto(symbol);
        setOiCache(symbol, oiData);
      }
      if (oiData?.sumOpenInterest) {
        atualizarOiNowHistory(symbol, oiData.sumOpenInterest);
        await radarOIProcessar(symbol, oiData);
      }
    } catch (err) {
      console.warn(`⚠️ Cron OI falhou em ${symbol}:`, err.message);
    }
  }));
}


function criarCron(symbols, intervalMs, nome) {
  if (!symbols.length) return;
  console.log(`⏱️ Criado cron '${nome}' para ${symbols.length} símbolos a cada ${intervalMs/1000}s`);

  setInterval(async () => {
    try {
      
      const batches = chunkArray(symbols, 20);
      for (const batch of batches) {
        await processarBatch(batch);
      }
    } catch (err) {
      console.error(`❌ Erro cron '${nome}':`, err.message);
    }
  }, intervalMs);
}


async function iniciarCronsOi() {
  try {
    const { prioritarios = [], secundarios = [] } = await obterTopAtivosRadar(MAX_ATIVOS_RADAR);
    const allSymbols = [...prioritarios, ...secundarios];

    if (!allSymbols.length) {
      console.log("⚠️ Nenhum símbolo disponível para cron OI");
      return;
    }

   const top50 = allSymbols.slice(0, 10);
    const top100 = allSymbols.slice(10, 20);
    const top200 = allSymbols.slice(20, 40);
    const rest = allSymbols.slice(40);


criarCron(top50, 25_000, "TOP50");    
criarCron(top100, 40_000, "51-100");   
criarCron(top200, 65_000, "101-200");  
criarCron(rest, 90_000, "201+");      


  } catch (err) {
    console.error("❌ Erro iniciarCronsOi:", err.message);
  }
}





// =====================[ FETCH OI BINANCE + NOW ]======================
async function fetchOiCompleto(symbol) {
  try {
   
    const fetchOI = async (period, limit) => {
      try {
        const { data } = await axios.get(
          "https://fapi.binance.com/futures/data/openInterestHist",
          { params: { symbol, period, limit } }
        );
        return data;
      } catch {
        return [];
      }
    };

    
    const [data5m, data1h] = await Promise.all([
      fetchOI("5m", 20),
      fetchOI("1h", 20)
    ]);

    const hist5m = (data5m || []).map(d => parseFloat(d.sumOpenInterest)).filter(Number.isFinite);
    const hist1h = (data1h || []).map(d => parseFloat(d.sumOpenInterest)).filter(Number.isFinite);

    const sumOpenInterest = hist5m.at(-1) || 0;

    
   let precoAtual = 0;
  try {
    const precoData = await getPrecoInterno(symbol);
    precoAtual = Number(precoData?.preco || 0);
  } catch {}

    const oiUsd = +(sumOpenInterest * precoAtual).toFixed(2);

    
    function smooth(values, n = 3) {
      if (!values || values.length < n) return values?.at(-1) || 0;
      return values.slice(-n).reduce((a, b) => a + b, 0) / n;
    }

    
    function calcularVar(symbol, minutos) {
      const arr = oiNowHistory[symbol] || [];
      if (arr.length < 2) return 0;

      const cutoff = Date.now() - minutos * 60_000;
      const atual = arr.at(-1);

      
      let antigo = arr[0];
      for (const h of arr) {
        if (h.ts <= cutoff) {
          antigo = h;
        } else {
          break;
        }
      }

      if (antigo && atual && antigo.oi > 0) {
        const pct = ((atual.oi - antigo.oi) / antigo.oi) * 100;
        return smooth([pct], 1); 
      }
      return 0;
    }

   
    const var1m = +calcularVar(symbol, 1).toFixed(2);
    const var3m = +calcularVar(symbol, 3).toFixed(2);

    
    let var5m = calcularVar(symbol, 5);
    if (data5m?.length >= 2) {
      const prev = parseFloat(data5m.at(-2).sumOpenInterest);
      const last = parseFloat(data5m.at(-1).sumOpenInterest);
      if (prev > 0 && last > 0) {
        var5m = ((last - prev) / prev) * 100;
      }
    }

    
    const extrairVaria = (arr, back = 1) => {
      const hist = (arr || []).map(d => parseFloat(d.sumOpenInterest)).filter(Number.isFinite);
      if (hist.length >= back + 1 && hist.at(-back - 1) > 0) {
        return ((hist.at(-1) - hist.at(-back - 1)) / hist.at(-back - 1)) * 100;
      }
      return 0;
    };

    const var15m = +extrairVaria(data5m, 3).toFixed(2); // ~15m
    const var1hCalc = +extrairVaria(data1h, 3).toFixed(2); // ~3h

    

    return {
      symbol,
      sumOpenInterest,
      oiUsd,
      hist: hist5m,
      hist1h,
      var1m,
      var3m,
      var5m: +var5m.toFixed(2),
      var15m,
      var1h: var1hCalc
    };
  } catch (err) {
    console.error("❌ Erro fetchOiCompleto", symbol, err.message);
    return {
      symbol,
      sumOpenInterest: 0,
      oiUsd: 0,
      hist: [],
      hist1h: [],
      var1m: 0,
      var3m: 0,
      var5m: 0,
      var15m: 0,
      var1h: 0
    };
  }
}



// =====================[ ROTA OI ]======================
app.get("/oi/:symbol", async (req, res) => {
  const symbol = req.params.symbol.toUpperCase();
  console.log(`➡️ ROTA /oi chamada para ${symbol}`);

  try {
    const cached = getOiFromCache(symbol);
    if (cached) {
      return res.json(cached);
    }

    console.log(`⚡ FETCH REAL OI para ${symbol}`);
    const oiData = await fetchOiCompleto(symbol);
    setOiCache(symbol, oiData);
    res.json(oiData);
  } catch (err) {
    console.error(`❌ Erro rota /oi ${symbol}`, err.message);
    res.json({ symbol, sumOpenInterest: 0, oiUsd: 0, hist: [], hist1h: [], var1m: 0, var3m: 0, var5m: 0, var15m: 0, var1h: 0 });
  }
});


// =====================[ ROTA OI LOTE ]======================
app.post("/oi-lote", async (req, res) => {
  const { symbols } = req.body || {};
  if (!Array.isArray(symbols) || !symbols.length) {
    return res.status(400).json({ error: "Lista de símbolos inválida" });
  }

  try {
    const results = await getOiLoteInterno(symbols);
    res.json(results);
  } catch (err) {
    console.error("❌ Erro /oi-lote:", err.message);
    res.status(500).json({ error: "Falha ao processar lote de OI" });
  }
});



// =====================[ TTL CONFIG ]=====================
const LSR_TTLS = {
  lsr: 5 * 60_000,    // 5 min
  lsar: 10 * 60_000,  // 10 min
  lspr: 15 * 60_000,  // 15 min
  volume: 60_000      // 1 min 
};

// =====================[ CACHE MAPS ]=====================
const lsrCache = new Map();   // Long/Short Ratio global
const lsarCache = new Map();  // Top accounts
const lsprCache = new Map();  // Top positions
const volumeCache = new Map();

// =====================[ FUNÇÕES AUXILIARES COM TTL ]=====================
function isExpired(entry) {
  return !entry || (entry.expiresAt && Date.now() > entry.expiresAt);
}

function getLSRFromCache(symbol) {
  const e = lsrCache.get(symbol);
  if (isExpired(e)) { lsrCache.delete(symbol); return null; }
  return e;
}
function setLSRCache(symbol, data) {
  lsrCache.set(symbol, { ...data, ts: Date.now(), expiresAt: Date.now() + LSR_TTLS.lsr });
}

function getLSARFromCache(symbol) {
  const e = lsarCache.get(symbol);
  if (isExpired(e)) { lsarCache.delete(symbol); return null; }
  return e;
}
function setLSARCache(symbol, data) {
  lsarCache.set(symbol, { ...data, ts: Date.now(), expiresAt: Date.now() + LSR_TTLS.lsar });
}

function getLSPRFromCache(symbol) {
  const e = lsprCache.get(symbol);
  if (isExpired(e)) { lsprCache.delete(symbol); return null; }
  return e;
}
function setLSPRCache(symbol, data) {
  lsprCache.set(symbol, { ...data, ts: Date.now(), expiresAt: Date.now() + LSR_TTLS.lspr });
}

function getVolumeFromCache(symbol, interval) {
  const e = volumeCache.get(`${symbol}-${interval}`);
  if (isExpired(e)) { volumeCache.delete(`${symbol}-${interval}`); return null; }
  return e;
}
function setVolumeCache(symbol, interval, data) {
  volumeCache.set(`${symbol}-${interval}`, { ...data, ts: Date.now(), expiresAt: Date.now() + LSR_TTLS.volume });
}


// =====================[ FETCHERS ]=====================
async function fetchLSR(symbol) {
  try {
    const { data } = await api.get(
      "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
      { params: { symbol, period: "5m", limit: 10 } }
    );
    const hist = data.map(d => parseFloat(d.longShortRatio)).filter(v => !isNaN(v));
    return { symbol, longShortRatio: hist.at(-1) || 0, hist };
  } catch {
    return { symbol, longShortRatio: 0, hist: [] };
  }
}

async function fetchLSAR(symbol) {
  try {
    const { data } = await api.get(
      "https://fapi.binance.com/futures/data/topLongShortAccountRatio",
      { params: { symbol, period: "5m", limit: 10 } }
    );
    const hist = data.map(d => parseFloat(d.longShortRatio)).filter(v => !isNaN(v));
    return { symbol, ratio: hist.at(-1) || 0, hist };
  } catch {
    return { symbol, ratio: 0, hist: [] };
  }
}

async function fetchLSPR(symbol) {
  try {
    const { data } = await api.get(
      "https://fapi.binance.com/futures/data/topLongShortPositionRatio",
      { params: { symbol, period: "5m", limit: 10 } }
    );
    const hist = data.map(d => parseFloat(d.longShortRatio)).filter(v => !isNaN(v));
    return { symbol, ratio: hist.at(-1) || 0, hist };
  } catch {
    return { symbol, ratio: 0, hist: [] };
  }
}

async function fetchVolume(symbol, interval) {
  try {
    const data = await fetchKlinesCached(symbol, interval, "futures", 1);
    if (!data?.length) return { volume: 0 };

    const volumeBase = parseFloat(data[0][5]); // volume (base)
    const close = parseFloat(data[0][4]);      // preço
    const volumeUSD = +(volumeBase * close).toFixed(2);

    return { volume: volumeUSD };
  } catch {
    return { volume: 0 };
  }
}


// =====================[ ROTAS INDIVIDUAIS ]=====================
app.get("/lsr/:symbol", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();
  const data = await getLSRInterno(symbol);
  res.json(data);
});

app.get("/lsar/:symbol", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();
  const data = await getLSARInterno(symbol);
  res.json(data);
});

app.get("/lspr/:symbol", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();
  const data = await getLSPRInterno(symbol);
  res.json(data);
});

app.get("/volume-usd/:symbol/:interval", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();
  const interval = req.params.interval;
  const data = await getVolumeInterno(symbol, interval);
  res.json(data);
});



// =====================[ ROTAS DE LOTE COM LOTE/THROTTLE ]=====================
app.post("/lsr-lote", async (req, res) => {
  const { symbols } = req.body || {};
  if (!Array.isArray(symbols) || !symbols.length) {
    return res.status(400).json({ error: "Lista inválida" });
  }

  const results = {};
  await processarEmLotes(symbols, 5, 600, async (s) => {
    let data = getLSRFromCache(s);
    if (!data) {
      data = await fetchLSR(s);
      setLSRCache(s, data);
    }
    results[s] = data;
  });

  res.json(results);
});

app.post("/lsar-lote", async (req, res) => {
  const { symbols } = req.body || {};
  if (!Array.isArray(symbols) || !symbols.length) {
    return res.status(400).json({ error: "Lista inválida" });
  }

  const results = {};
  await processarEmLotes(symbols, 5, 600, async (s) => {
    let data = getLSARFromCache(s);
    if (!data) {
      data = await fetchLSAR(s);
      setLSARCache(s, data);
    }
    results[s] = data;
  });

  res.json(results);
});

app.post("/lspr-lote", async (req, res) => {
  const { symbols } = req.body || {};
  if (!Array.isArray(symbols) || !symbols.length) {
    return res.status(400).json({ error: "Lista inválida" });
  }

  const results = {};
  await processarEmLotes(symbols, 5, 600, async (s) => {
    let data = getLSPRFromCache(s);
    if (!data) {
      data = await fetchLSPR(s);
      setLSPRCache(s, data);
    }
    results[s] = data;
  });

  res.json(results);
});

app.post("/volume-lote", async (req, res) => {
  const { symbols, interval } = req.body || {};
  if (!Array.isArray(symbols) || !symbols.length || !interval) {
    return res.status(400).json({ error: "Parâmetros inválidos" });
  }

  try {
    const results = await getVolumeLoteInterno(symbols, interval);
    res.json(results);
  } catch (err) {
    console.error("❌ Erro /volume-lote:", err.message);
    res.status(500).json({ error: "Falha ao processar lote de volume" });
  }
});


app.get("/variacao/:symbol/:interval", async (req, res) => {
  const symbol = String(req.params.symbol || "").toUpperCase();
  const interval = req.params.interval;
  const data = await getVariacaoInterno(symbol, interval);
  res.json(data);
});

 
// =======================[ Radar de OI em lotes ]=======================

const oiCacheSimples = new Map();

    function formatarNumeroUSD(valor) {
      const abs = Math.abs(valor);
      let texto = "";
    if (abs >= 1e9) return '$' + (abs / 1e9).toFixed(1) + 'B';
    else if (abs >= 1e6) return '$' + (abs / 1e6).toFixed(1) + 'M';
    else if (abs >= 1e3) return '$' + (abs / 1e3).toFixed(1) + 'K';
    else texto = '$' + abs.toFixed(0);
    if (abs < 0) texto = '-' + '$'+ texto;
    return texto;
  }

    function formatarNumero(valor) {
    
  const val = Number(valor); 
  if (isNaN(val)) return valor; 

  const abs = Math.abs(valor);
  let texto = "";
  if (abs >= 1e9) texto = (abs / 1e9).toFixed(2) + 'B';
  else if (abs >= 1e6) texto = (abs / 1e6).toFixed(2) + 'M';
  else if (abs >= 1e3) texto = (abs / 1e3).toFixed(2) + 'K';
  else texto = abs.toFixed(0);   }



/* =======================[ PATCH OTIMIZAÇÃO DE API ]======================= */

let validFuturesSymbols = new Set();

async function carregarContratosFuturos() {
  try {
    const { data } = await axios.get("https://fapi.binance.com/fapi/v1/exchangeInfo");

    validFuturesSymbols = new Set(
      data.symbols
        .filter(s => s.contractType === "PERPETUAL" && s.status === "TRADING")
        .map(s => s.symbol)
    );

    console.log(`✅ ${validFuturesSymbols.size} contratos futuros ativos carregados`);
  } catch (err) {
    console.error("❌ Falha ao carregar contratos futuros:", err.message);
  }
}



// =====================[ GET OI COM VARIAÇÃO ]======================
async function getOiComVariação(symbol) {
  if (!validFuturesSymbols.has(symbol)) {
    return { oi: 0, var5m: 0 }; 
  }

  try {
    
    const cached = getOiFromCache(symbol, 30_000);
    if (cached && typeof cached.sumOpenInterest === "number") {
      const hist = cached.hist || [];
      const oiAtual = hist.at(-1) || cached.sumOpenInterest;
      const oiAnt   = hist.at(-2) || cached.sumOpenInterest;
      const var5m   = oiAnt > 0 ? ((oiAtual - oiAnt) / oiAnt) * 100 : 0;

      return { oi: oiAtual, oiUsd: cached.oiUsd, var5m };
    }


    const url = `https://fapi.binance.com/fapi/v1/openInterest?symbol=${symbol}`;
    const data = await fetchWithBackoff(url);
    if (!data?.openInterest) return { oi: 0, var5m: 0 };

    const oiNovo = parseFloat(data.openInterest);
    const agora = Date.now();

    const anterior = oiCacheSimples.get(symbol);
    let var5m = 0;

    if (anterior && (agora - anterior.ts) < 6 * 60 * 1000) {
      var5m = ((oiNovo - anterior.oi) / anterior.oi) * 100;
    }

    oiCacheSimples.set(symbol, { oi: oiNovo, ts: agora });
    return { oi: oiNovo, var5m };
  } catch (err) {
    if (!oiCacheSimples.has(symbol)) {
      console.warn(`⚠️ OI indisponível para ${symbol}: ${err.message}`);
    }
    return { oi: 0, var5m: 0 };
  }
}


/* =========================[ 17) TOP ATIVOS (400) ]=========================== */
const ATIVOS_EXCLUIDOS = ["WBTCUSDT","WBETHUSDT", "HIFIUSDT"];
const ATIVOS_DESEJADOS = [ "GALAUSDT" ];
const ATIVOS_PRIORITARIOS = [ "BTCUSDT", "ETHUSDT"];




// =========================[ RADAR DE ATIVOS ]=========================
async function obterTopAtivosRadar(limitTotal = 50) {
  const { data } = await api.get(`${BINANCE_FUTURES}/ticker/24hr`);

  const candidatos = data
    .filter(d =>
      d.symbol.endsWith("USDT") &&
      !["BUSD", "TUSD", "USDC", "FDUSD", "USD1", "XUSD"].some(s => d.symbol.includes(s)) &&
      !ATIVOS_EXCLUIDOS.includes(d.symbol)
    )
    .filter(d => validFuturesSymbols.has(d.symbol))
    .filter(d => !invalidSymbols.has(d.symbol))
    .sort((a, b) => parseFloat(b.quoteVolume || 0) - parseFloat(a.quoteVolume || 0));

  const baseTop = candidatos.slice(0, limitTotal).map(d => d.symbol);

  const final = Array.from(new Set([
    ...ATIVOS_PRIORITARIOS,
    ...baseTop,
    ...ATIVOS_DESEJADOS
  ])).slice(0, limitTotal);

  const metade = Math.ceil(final.length / 2);
  const prioritarios = final.slice(0, metade);
  const secundarios = final.slice(metade);

  top100Volume = new Set(final.slice(0, Math.min(10, final.length)));
  top50Volume = new Set(final.slice(0, Math.min(5, final.length)));

  console.log(`📊 Radar atualizado com ${final.length} símbolos`);

  return { prioritarios, secundarios };
}


async function atualizarTopRadar() {
  try {
    const dados = await obterTopAtivosRadar(50);
    cacheTopRadar = { ts: Date.now(), dados };

    for (const s of dados.prioritarios) enqueueRSI(s, "mid");
    for (const s of dados.secundarios) enqueueRSI(s, "low");

    console.log("✅ TopRadar atualizado:",
      "pri:", dados.prioritarios.length,
      "sec:", dados.secundarios.length
    );
  } catch (err) {
    console.error("❌ Falha ao atualizar TopRadar:", err.message);
    // mantém o último cache válido
  }
}

async function warmVisualHistoryForTopSymbols() {
  try {
    const lista = Array.from(top50Volume || []).slice(0, 50);
    if (!lista.length) {
      console.warn("⚠️ warmVisualHistoryForTopSymbols: top50Volume ainda vazio.");
      return;
    }

    console.log(`🎨 Iniciando warmup visual de ${lista.length} símbolos (${VISUAL_BOOTSTRAP_TF})...`);

    for (let i = 0; i < lista.length; i++) {
      const symbol = lista[i];
      await warmVisualHistoryForSymbol(symbol, VISUAL_BOOTSTRAP_TF, VISUAL_BOOTSTRAP_LIMIT);

      if (i < lista.length - 1) {
        await new Promise(r => setTimeout(r, 350));
      }
    }

    console.log(`✅ Warmup visual concluído para ${lista.length} símbolos.`);
  } catch (err) {
    console.warn("⚠️ warmVisualHistoryForTopSymbols falhou:", err.message || err);
  }
}

async function inicializarServer() {
  await carregarContratosFuturos();
  console.log("✅ Contratos futuros carregados, iniciando TopRadar...");

  await atualizarTopRadar();

  // refresh do top 50 a cada 2 horas
  setInterval(async () => {
    console.log("🔄 Refresh programado do TopRadar (2h)");
    await atualizarTopRadar();
  }, 2 * 60 * 60 * 1000);

    setTimeout(() => {
    warmVisualHistoryForTopSymbols().catch(console.error);
    }, 12_000);

  // inicia WS Binance somente com os símbolos já em cache
  setTimeout(() => {
    try {
      const { prioritarios = [], secundarios = [] } = cacheTopRadar.dados || {};
      const symbols = [...new Set([...prioritarios, ...secundarios])].filter(s =>
        validFuturesSymbols.has(s)
      );

      if (!symbols.length) {
        console.warn("⚠️ Nenhum símbolo válido encontrado no radar para iniciar WS!");
        return;
      }

      console.log(`📡 WS Binance iniciado com ${symbols.length} símbolos em lotes`);
      iniciarWsBinanceLotes(symbols);
    } catch (err) {
      console.error("❌ Erro ao inicializar WS Binance:", err.message);
    }
  }, 20_000);
}
// =========================[ RADAR OI EM LOTES COM AUTO-THROTTLE ]=========================
let radarIndexPri = 0;
let radarIndexSec = 0;

let batchPri = 5;
let batchSec = 10;

const MIN_BATCH_PRI = 5;
const MIN_BATCH_SEC = 10;
const MAX_BATCH_PRI = 10;
const MAX_BATCH_SEC = 15;

let statsRadar = { erros: 0, reqs: 0, tempos: [] };

async function radarBatchOI(grupo) {
  const agora = Date.now();

 
  if (agora - cacheTopRadar.ts > 120_000) { 
    console.warn("⚠️ Cache do radar desatualizado, forçando refresh...");
    await atualizarTopRadar();
  }

  let { prioritarios, secundarios } = cacheTopRadar.dados;

  
  prioritarios = prioritarios.slice(0, LIMIT_PRIORITARIOS);
  secundarios = secundarios.slice(0, LIMIT_SECUNDARIOS);

  const symbols = grupo === "prioritarios" ? prioritarios : secundarios;
  if (!symbols.length) return;

  let idx = grupo === "prioritarios" ? radarIndexPri : radarIndexSec;
  let batchSize = grupo === "prioritarios" ? batchPri : batchSec;

  const batch = symbols.slice(idx, idx + batchSize);

  const t0 = Date.now();
 try {
  const lote = await getOiLoteInterno(batch);

  for (const s of batch) {
    try {
      const oiData = lote[s];
      if (!oiData) continue;
      await radarOIProcessar(s, oiData); 
 
      await radarVolumeProcessar(s); 
    } catch {
      statsRadar.erros++;
    } finally {
      statsRadar.reqs++;
    }
  }
} catch (err) {
  console.error("❌ Erro radarBatchOI fetch lote:", err.message);
  statsRadar.erros += batch.length;
}

  const tempo = Date.now() - t0;
  statsRadar.tempos.push(tempo);

  if (statsRadar.tempos.length > 20) statsRadar.tempos.shift();

  
  if (statsRadar.reqs >= 20) {
    const taxaErro = statsRadar.erros / statsRadar.reqs;
    const mediaLatencia = statsRadar.tempos.reduce((a, b) => a + b, 0) / statsRadar.tempos.length;

    if (taxaErro > 0.2 || mediaLatencia > 1500) {
      if (grupo === "prioritarios") batchPri = Math.max(MIN_BATCH_PRI, Math.floor(batchPri * 0.8));
      else batchSec = Math.max(MIN_BATCH_SEC, Math.floor(batchSec * 0.8));
      console.warn(`⚠️ Auto-throttle: reduzindo lote ${grupo} para`, grupo === "prioritarios" ? batchPri : batchSec);
    } else if (taxaErro < 0.05 && mediaLatencia < 800) {
      if (grupo === "prioritarios") batchPri = Math.min(MAX_BATCH_PRI, batchPri + 2);
      else batchSec = Math.min(MAX_BATCH_SEC, batchSec + 5);
    }

    statsRadar = { erros: 0, reqs: 0, tempos: [] };
  }''

  if (grupo === "prioritarios") {
    radarIndexPri = (radarIndexPri + batchSize) % symbols.length;
  } else {
    radarIndexSec = (radarIndexSec + batchSize) % symbols.length;
  }
}


setInterval(() => radarBatchOI("prioritarios"), 30_000);
setInterval(() => radarBatchOI("secundarios"), 120_000);

async function radarBatchVolume(grupo) {
  const agora = Date.now();

  if (agora - cacheTopRadar.ts > 120_000) {
    console.warn("⚠️ Cache do radar desatualizado (volume), forçando refresh...");
    await atualizarTopRadar();
  }

  let { prioritarios, secundarios } = cacheTopRadar.dados;
  prioritarios = prioritarios.slice(0, LIMIT_PRIORITARIOS);
  secundarios  = secundarios.slice(0, LIMIT_SECUNDARIOS);

  const symbols = grupo === "prioritarios" ? prioritarios : secundarios;
  if (!symbols.length) return;

  const batchSize = grupo === "prioritarios" ? batchPri : batchSec;
  const idx = grupo === "prioritarios" ? radarIndexPri : radarIndexSec;
  const batch = symbols.slice(idx, idx + batchSize);

  try {
 const lote = await getVolumeLoteInterno(batch, "5m");


    for (const s of batch) {
      try {
        const volData = {
          usd: lote[s]?.volume ?? 0,
          var1m: lote[s]?.var1m ?? 0,
          var3m: lote[s]?.var3m ?? 0,
          var5m: lote[s]?.var5m ?? 0
        };
        if (volData.usd > 0) {
          radarVolumeProcessar(s, volData);
        }
      } catch (err) {
        console.warn("⚠️ radarBatchVolume fail:", err.message);
      }
    }
  } catch (err) {
    console.error("❌ Erro radarBatchVolume fetch lote:", err.message);
  }

  if (grupo === "prioritarios") {
    radarIndexPri = (radarIndexPri + batchSize) % symbols.length;
  } else {
    radarIndexSec = (radarIndexSec + batchSize) % symbols.length;
  }
}

setInterval(() => radarBatchVolume("prioritarios"), 45_000);
setInterval(() => radarBatchVolume("secundarios"), 180_000);

// =======================================================
// 🔒 Controle global de cooldown de eventos (Volume/OI)
// =======================================================
const ultimoEvento = new Map(); 

function podeDispararEvento(symbol, hash, cooldownMs = 30 * 60 * 1000) {
  const agora = Date.now();
  const key = `${symbol}-${hash}`;
  const ultimo = ultimoEvento.get(key) || 0;

  if (agora - ultimo < cooldownMs) {
    return false; 
  }

  ultimoEvento.set(key, agora);
  return true;
}
// =======================================================
// ⚡ Radar de OI por intensidade
// =======================================================

async function radarOIProcessar(symbol, oiData) {
  try {
    const { oiUsd, var1m, var3m, var5m } = oiData;
    if (!oiUsd) return;

    
    if (!radarOIProcessar.state) radarOIProcessar.state = {};
    const state = radarOIProcessar.state;
    if (!state[symbol]) state[symbol] = { nivel: 0, ts: 0 };

    const agora = Date.now();
    const RESET_MS = 10 * 60 * 1000; 

    
    if (agora - state[symbol].ts > RESET_MS) {
      state[symbol] = { nivel: 0, ts: 0 };
    }

    
    const variacoes = {
      "1m": safeNum(var1m, 0),
      "3m": safeNum(var3m, 0),
      "5m": safeNum(var5m, 0)
    };

   
    let maiorTF = null;
    let maiorVar = 0;
    for (const [tf, valor] of Object.entries(variacoes)) {
      if (Math.abs(valor) > Math.abs(maiorVar)) {
        maiorVar = valor;
        maiorTF = tf;
      }
    }
    if (!maiorVar) return;

    
    const absVar = Math.abs(maiorVar);
    let nivel = 0;
    if (absVar >= 20) nivel = 5;
    else if (absVar >= 10) nivel = 4;
    else if (absVar >= 5)  nivel = 3;
    else if (absVar >= 3)  nivel = 2;
    else if (absVar >= 1)  nivel = 1;

    
    if (nivel <= 0 || state[symbol].nivel >= nivel) return;

    
    const hashEvento = `OI-${nivel}-${maiorTF}-${maiorVar > 0 ? "up" : "down"}-${Math.round(maiorVar)}`;
    if (!podeDispararEvento(symbol, hashEvento)) return;

    
    const direcao = maiorVar > 0 ? "⬆️ Aumento" : "⬇️ Queda";
    const msg = `⚡ OI ${maiorTF} | Nível ${nivel} ${direcao} em ${symbol}: ${maiorVar.toFixed(2)}% (${formatarNumeroUSD(oiUsd)})`;

    
    try {
      if (nivel >= 3) await sendTelegramAlert2(msg);
      else await sendTelegramAlert4(msg);
    } catch (e) {
      console.warn("telegram fail radarOI:", e.message);
    }

    
    if (!eventosGlobais[symbol]) eventosGlobais[symbol] = [];
    eventosGlobais[symbol].unshift({
      tag: `⚡OI ${maiorVar > 0 ? "↑" : "↓"} N${nivel}`,
      ts: agora,
      p: safeNum(maiorVar, 0),
      oiUsd,
      tf: maiorTF,
      note: msg,
      nivel
    });
    if (eventosGlobais[symbol].length > EVENTOS_GLOB_MAX) {
      eventosGlobais[symbol].length = EVENTOS_GLOB_MAX;
    }

    
    if (nivel >= 3) {
      ativarStreamsExpandido(symbol, 40); 
      console.log(`📌 Stream expandido ativado p/ ${symbol} (OI ${maiorVar.toFixed(2)}%)`);
       //setTimeout(() => recomputeStreams(), 5000);
    }

    
    state[symbol] = { nivel, ts: agora };

  } catch (err) {
    console.error(`❌ Erro radarOIProcessar ${symbol}:`, err.message || err);
  }
}


// =======================================================
// 📊 Radar de Volume por intensidade
// =======================================================
async function radarVolumeProcessar(symbol) {
  try {
    const tfList = ["1m", "3m", "5m", "15m"];
    const agora = Date.now();
    const RESET_MS = 10 * 60 * 1000; 

    if (!radarVolumeProcessar.state) radarVolumeProcessar.state = {};
    const state = radarVolumeProcessar.state;
    if (!state[symbol]) state[symbol] = { nivel: 0, ts: 0 };

    if (agora - state[symbol].ts > RESET_MS) {
      state[symbol] = { nivel: 0, ts: 0 };
    }

    if (!top100Volume.has(symbol)) return;

   
    const vols = {};
    for (const tf of tfList) {
      const v = getVolumeFromCache(symbol, tf);
      vols[tf] = v?.volume ?? 0;
    }

    
    const variacoes = {};
    if (vols["1m"] && vols["3m"]) variacoes["1m"] = ((vols["1m"] - vols["3m"]) / vols["3m"]) * 100;
    if (vols["3m"] && vols["5m"]) variacoes["3m"] = ((vols["3m"] - vols["5m"]) / vols["5m"]) * 100;
    if (vols["5m"] && vols["15m"]) variacoes["5m"] = ((vols["5m"] - vols["15m"]) / vols["15m"]) * 100;

    
    let maiorTF = null;
    let maiorVar = 0;
    for (const [tf, valor] of Object.entries(variacoes)) {
      if (Math.abs(valor) > Math.abs(maiorVar)) {
        maiorVar = valor;
        maiorTF = tf;
      }
    }
    if (!maiorVar) return;

   
    function classificarVolume(varPct) {
      if (varPct > 0) {
        if (varPct >= 5000) return 5;
        if (varPct >= 4000) return 4;
        if (varPct >= 1000) return 3;
        if (varPct >= 500)  return 2;
        if (varPct >= 100)  return 1;
      } else if (varPct < 0) {
        if (varPct <= -5000) return 5;
        if (varPct <= -4000) return 4;
        if (varPct <= -1000) return 3;
        if (varPct <= -500)  return 2;
        if (varPct <= -100)  return 1;
      }
      return 0;
    }

    const nivel = classificarVolume(maiorVar);
    if (nivel <= 0 || state[symbol].nivel >= nivel) return;

    
    const hashEvento = `VOL-${nivel}-${maiorTF}-${maiorVar > 0 ? "up" : "down"}-${Math.round(maiorVar)}`;
    if (!podeDispararEvento(symbol, hashEvento)) return;

    const direcao = maiorVar > 0 ? "⬆️ Aumento" : "⬇️ Queda";
    const msg = `📊 Volume ${maiorTF}, Nível ${nivel} ${direcao} em ${symbol}: ${maiorVar.toFixed(2)}%`;

    try {
      if (nivel >= 3) await sendTelegramAlert(msg);
      else await sendTelegramAlert3(msg);
    } catch (e) { console.warn("telegram fail radarVolume:", e.message); }

    if (!eventosGlobais[symbol]) eventosGlobais[symbol] = [];
    eventosGlobais[symbol].unshift({
      tag: `📊Vol ${maiorVar > 0 ? "↑" : "↓"} N${nivel}`,
      ts: agora,
      p: safeNum(maiorVar, 0),
      note: msg,
      nivel
    });
    if (eventosGlobais[symbol].length > EVENTOS_GLOB_MAX) {
      eventosGlobais[symbol].length = EVENTOS_GLOB_MAX;
    }

    state[symbol] = { nivel, ts: agora };

  } catch (err) {
    console.error(`❌ Erro radarVolumeProcessar ${symbol}:`, err.message || err);
  }
}



app.get("/top-ativos", async (req, res) => {
  try {
    const { prioritarios, secundarios } = cacheTopRadar.dados || { prioritarios: [], secundarios: [] };
    const listaUnica = [...prioritarios, ...secundarios];
    res.json(listaUnica);
  } catch (err) {
    console.error("❌ /top-ativos:", err.message);
    res.status(500).json([]);
  }
});


/* ==========================[ 18) KLINES (cache 15s) ]======================== */
app.get("/klines/:symbol/:interval", async (req, res) => {
  const { symbol, interval } = req.params;
  const market = req.query.market || "futures";
  const limit = Math.min(Number(req.query.limit || 10), 50);

  if (!simboloValido(symbol)) {
    return res.status(400).json({ error: "Símbolo inválido", symbol });
  }

  try {
    const klines = await getWithCache(
      `k-${market}-${symbol}-${interval}-${limit}`,
      15_000,
      async () => {
        return await fetchKlinesWithFallback(symbol, interval, market, limit);
      }
    );

    res.json(Array.isArray(klines) ? klines : []);
  } catch (err) {
    console.error(`❌ /klines ${symbol} ${interval} ${market}:`, err.message);
    res.status(500).json({ error: "Erro ao buscar klines", details: err.message });
  }
});


async function processarEmLotes(lista, tamanhoLote = 5, atrasoMs = 600, fn) {
  const out = [];
  for (let i = 0; i < lista.length; i += tamanhoLote) {
    const lote = lista.slice(i, i + tamanhoLote);
    const resp = await Promise.allSettled(lote.map(fn));
    resp.forEach(r => { if (r.status === "fulfilled") out.push(r.value); });
    if (atrasoMs) await new Promise(r => setTimeout(r, atrasoMs));
  }
  return out;
}


// =========================
// 🚀 SCORE-LOTE UNIFICADO (GET + POST)
// =========================

function safe(obj, path, def = 0) {
  try {
    return path.split(".").reduce((acc, k) => acc?.[k], obj) ?? def;
  } catch {
    return def;
  }
}



    function detectarLateralizacao(hist, threshold = 0.5) {
      if (!hist || hist.length < 5) return { lateral: false, score: 0 };

      const vals = hist.map(Number);
      const media = vals.reduce((a, b) => a + b, 0) / vals.length;
      const desvio = Math.sqrt(vals.reduce((a, b) => a + (b - media) ** 2, 0) / vals.length);

      const score = Math.max(0, 100 - desvio * 20); // 0–100 escala

      return {
        lateral: desvio < threshold && Math.abs(media) < threshold,
        score: score.toFixed(1)
      };
    }
    
async function montarScore(symbol) {
  try {


const key1m = `${symbol}:1m`;

    // ==== PREÇO ====
     let preco = 0;
      try {
        const precoData = await getPrecoInterno(symbol);
        preco = safe(precoData, "preco", 0);
      } catch {}

    // ==== OI DATA ====
    let oiData = getOiFromCache(symbol);
    if (!oiData) {
      try {
        oiData = await fetchOiCompleto(symbol); 
        setOiCache(symbol, oiData);
      } catch {
        oiData = { symbol, sumOpenInterest: 0, oiUsd: 0, hist: [], hist1h: [], var1m: 0, var3m: 0, var5m: 0, var15m: 0, var1h: 0 };
      }
    }


    // ==== LSR / LSAR / LSPR ====
   const lsrData  = await getLSRInterno(symbol);
   const lsarData = await getLSARInterno(symbol);
   const lsprData = await getLSPRInterno(symbol);


// ==== RSI, exp (delta RSI, antigo exposição ao BTC) e volrel (1m,5m,15m,30m,1h,4h,1d) ====

enqueueRSI(symbol);


const intervals = ["1m","5m","15m","30m","1h","4h","1d"];

const rsi = {};
const exp = {};
const volRel = {};

for (const intv of intervals) {
  const key = `${symbol}:${intv}`;
  const data = cacheRSI.get(key) ?? {};
  rsi[`rsi${intv}`] = data.rsi ?? null;
  exp[`exp${intv}`] = data.exp ?? null;
  volRel[`volRel${intv}`] = data.volRel ?? null;
}
  
  
   const forcaBTC = getForcaRsiBtcInterno(symbol);


    // ==== FUNDING ====
    const frData = await getFundingInterno(symbol);

    // ==== VOLUME USD ====
  const vol5m   = await getVolumeInterno(symbol, "5m");
  const vol15m  = await getVolumeInterno(symbol, "15m");
  const vol1d   = await getVolumeInterno(symbol, "1d");
  const vol4h   = await getVolumeInterno(symbol, "4h");
  const vol1h   = await getVolumeInterno(symbol, "1h");

    // ==== VARIAÇÃO ====
    const variacao1m  = await getVariacaoInterno(symbol, "1m");
    const variacao5m  = await getVariacaoInterno(symbol, "5m");
    const variacao15m = await getVariacaoInterno(symbol, "15m");
    const variacao30m = await getVariacaoInterno(symbol, "30m");
    const variacao4h  = await getVariacaoInterno(symbol, "4h");
    const variacao1h  = await getVariacaoInterno(symbol, "1h");
    const variacao1d  = await getVariacaoInterno(symbol, "1d");
    const variacao1w  = await getVariacaoInterno(symbol, "1w");

   
    const norm = (o) => ({
      value: o && typeof o === "object" && o.value !== undefined ? safeNum(o.value, null) : (typeof o === "number" ? o : null),
      hist: o && o.hist ? (Array.isArray(o.hist) ? o.hist.map(x => safeNum(x,0)) : []) : []
    });

    const vari1m = norm(variacao1m);
    const vari5m = norm(variacao5m);
    const vari15m = norm(variacao15m);
    const vari30m = norm(variacao30m);
    const vari1h = norm(variacao1h);
    const vari4h = norm(variacao4h);
    const vari1d = norm(variacao1d);
    const vari1w = norm(variacao1w);

    // ==== LIQUIDAÇÕES ====
    const agora = Date.now();
    const liqData = await fetchJSON(`http://localhost:${PORT}/liquidations/${symbol}`);

    // últimos 5 minutos
    const liq5m = {
      ...liqData,
      histLong: liqData.histLong.filter(ev => (agora - Number(ev.ts)) <= 5 * 60 * 1000),
      histShort: liqData.histShort.filter(ev => (agora - Number(ev.ts)) <= 5 * 60 * 1000),
    };

    // últimos 60 minutos
    const liq1h = {
      ...liqData,
      histLong: liqData.histLong.filter(ev => (agora - Number(ev.ts)) <= 60 * 60 * 1000),
      histShort: liqData.histShort.filter(ev => (agora - Number(ev.ts)) <= 60 * 60 * 1000),
    };

    // ==== SPARK (últimos 10 closes) ====
    const spark = gerarSpark(variacao5m.hist);

    // ==== DETECTAR LATERALIZAÇÃO ====
    function detectarLateralizacao(hist, threshold = 0.5) {
      if (!hist || hist.length < 5) return { lateral: false, score: 0 };

      const vals = hist.map(Number);
      const media = vals.reduce((a, b) => a + b, 0) / vals.length;
      const desvio = Math.sqrt(vals.reduce((a, b) => a + (b - media) ** 2, 0) / vals.length);

      const score = Math.max(0, 100 - desvio * 20); // 0–100 escala

      return {
        lateral: desvio < threshold && Math.abs(media) < threshold,
        score: score.toFixed(1)
      };
    }

  
    const lateral4h  = detectarLateralizacao(variacao4h.hist);
    const lateral1d  = detectarLateralizacao(variacao1d.hist);

    // ==== CAPITULAÇÃO ====
    const caps5m = detectarCap(liq5m.long, liq5m.short, 3);
    const caps1h = detectarCap(liq1h.long, liq1h.short, 3);

    // ==== VAR OI ====
    const histOI_srv = oiData?.hist || [];
    const oiAtual_srv = histOI_srv.at(-1) || 0;
    const oiAnt_srv   = histOI_srv.at(-2) || 0;
    const varOI_srv   = oiAnt_srv > 0 ? ((oiAtual_srv - oiAnt_srv) / oiAnt_srv) * 100 : 0;

    // ==== FLOW OI SCORE ====
    let oiFlowScore = 0;
    if (varOI_srv > 1.5 && variacao5m.value > 0.2) oiFlowScore = +2;
    else if (varOI_srv > 0.8 && variacao5m.value > 0.1) oiFlowScore = +1;
    else if (varOI_srv > 1.5 && variacao5m.value < -0.2) oiFlowScore = -2;
    else if (varOI_srv > 0.8 && variacao5m.value < -0.1) oiFlowScore = -1;
    else if (varOI_srv < -1) oiFlowScore = -1;

   // ==== CONVERT LIQ HIST -> NOTIONAL USD (preferir ev.notional) ====
    const toNotional = arr => (Array.isArray(arr) ? arr.map(ev => {
      if (ev?.notional && Number.isFinite(Number(ev.notional))) {
        return Number(ev.notional);
      }
      const p = Number(ev?.price ?? ev?.p ?? 0);
      const q = Number(ev?.qty ?? ev?.q ?? 0);
      const n = Number.isFinite(p) && Number.isFinite(q) ? p * q : NaN;
      return n;
    }).filter(Number.isFinite) : []);

    // ==== PREPARE HISTORICAL VALUES ====
    const histLong5Vals  = toNotional(liq5m?.histLong || []);
    const histShort5Vals = toNotional(liq5m?.histShort || []);
    const histLong1hVals  = toNotional(liq1h?.histLong || []);
    const histShort1hVals = toNotional(liq1h?.histShort || []);

      // ==== PRESSURE INDEX ====
    const pressureIndex = calcularPressure(
      histLong5Vals, histShort5Vals,
      histLong1hVals, histShort1hVals,
      variacao5m?.value ?? 0,
      varOI_srv,
      symbol
    );

  // ==== EVENTOS ====

const eventos = [];

  // 3) Lateralização agrupada
  const grupos = { curto: [], medio: [], longo: [] };

  if (lateral4h.lateral)  grupos.medio.push(Number(lateral4h.score));
  if (lateral1d.lateral)  grupos.longo.push(Number(lateral1d.score));

  function classificarLateralizacao(media) {
    if (media >= 100) return "📊 LF"; //Lateralização forte
    if (media >= 80)  return "📊 LA"; //Lateralização Alta
    if (media >= 50)  return "📊 LM";
    if (media >= 20)  return "🔄 LF";
    return null;
  }

  for (const [nome, arr] of Object.entries(grupos)) {
    if (arr.length === 0) continue;
    const media = arr.reduce((a, b) => a + b, 0) / arr.length;
    const label = classificarLateralizacao(media);
    if (label) {
      const horizonte = nome === "curto" ? "5m–1h"
                    : nome === "medio" ? "4h"
                    : "1d";
      eventos.push(`${label} (${horizonte}: ${media.toFixed(1)})`);
    }
  }

  // 4) Pressure Index
  if (pressureIndex && pressureIndex > 0) {
    let label = "";
    if (pressureIndex >= 81) label = "📈 PF";
    else if (pressureIndex >= 60) label = "📈 PA";
    else if (pressureIndex >= 41) label = "💱 PM";
    else if (pressureIndex >= 21) label = "📉 PB";
    else label = "📉 PMB";

    eventos.push(`${label} (${pressureIndex})`);
  }

      // ==== STICKY FLAG ====
  if (stickySymbols.has(symbol) && stickySymbols.get(symbol) > Date.now()) {
  const minsRestantes = Math.round((stickySymbols.get(symbol) - Date.now()) / 60000);
  eventos.push(`📌 Stick ativo (${minsRestantes}m)`);
}

// Quando Funding exagerado
if (frData?.fundingRate && Math.abs(frData.fundingRate) > 0.01) eventos.push("⚖️");

if (caps5m.capLong && caps1h.capLong) {
  eventos.push("💥🟦5m/1h");
} else if (caps5m.capShort && caps1h.capShort) {
  eventos.push("💥🟥5m/1h");
} else {
  if (caps5m.capLong)  eventos.push("💣🟦5m");
  if (caps5m.capShort) eventos.push("💣🟥5m");
  if (caps1h.capLong)  eventos.push("💥🟦1h");
  if (caps1h.capShort) eventos.push("💥🟥1h");
}


    return {
      symbol,
      preco:
        Number.isFinite(preco) && preco > 0
        ? preco
        : (cacheTicker.get(symbol)?.price ?? precoCache.get(symbol) ?? null),
      oiData,
      lsrData,
      lsarData,
      lsprData,
      rsi,
      exp,
      volRel,
      funding: frData,
      volume: { vol5m, vol15m, vol1h, vol4h, vol1d },
      variacao: {
      variacao1m: { value: vari1m.value, hist: vari1m.hist },
      variacao5m: { value: vari5m.value, hist: vari5m.hist },
      variacao15m: { value: vari15m.value, hist: vari15m.hist },
      variacao30m: { value: vari30m.value, hist: vari30m.hist },
      variacao1h: { value: vari1h.value, hist: vari1h.hist },
      variacao4h: { value: vari4h.value, hist: vari4h.hist },
      variacao1d: { value: vari1d.value, hist: vari1d.hist },
      variacao1w: { value: vari1w.value, hist: vari1w.hist }
      },
      liq: { liq5m, liq1h },
      caps: { caps5m, caps1h },
      spark,
      pressureIndex,
      eventos,
      oiFlowScore,
      forcaBTC
    };
  } catch (err) {
    console.error(`❌ montarScore erro em ${symbol}:`, err.message);
  }
}


// ===============================
// 📡 Score em lote (inclui RSI, exp e volRel)
// ===============================
app.get("/score-lote", async (req, res) => {
  try {
    const symbols = (req.query.symbols || "")
      .split(",")
      .map(s => s.trim().toUpperCase())
      .filter(Boolean);

    if (!symbols.length) {
      return res.status(400).json({ error: "Nenhum símbolo informado" });
    }

   
    let oiLote = {};
    try {
      const resp = await axios.post(`http://localhost:${PORT}/oi-lote`, { symbols });
      oiLote = resp.data || {};
    } catch (err) {
      console.warn("⚠️ Falha em obter OI-lote, fallback para chamadas individuais:", err.message);
    }

    const resultados = [];
    const MAX_CONC = 5;

    for (let i = 0; i < symbols.length; i += MAX_CONC) {
      const subset = symbols.slice(i, i + MAX_CONC);

      const lote = await Promise.all(
        subset.map(async (s) => {
          try {
            const oiData = oiLote[s] || null;

            const tfList = ["1m","5m","15m","30m","1h","4h","1d"];
            const rsiObj = {};
            const expObj = {};
            const volRelObj = {};

            for (const tf of tfList) {
              const entry = cacheRSI.get(`${s}:${tf}`);
              rsiObj[`rsi${tf}`] = entry?.rsi ?? null;
              expObj[`exp${tf}`] = entry?.exp ?? null;
              volRelObj[`volRel${tf}`] = entry?.volRel ?? null;
            }

           const score = await montarScore(s, oiData);
            return {
              symbol: s,
              preco: score?.preco ?? cacheTicker.get(s)?.price ?? precoCache.get(s) ?? null,
              rsi: rsiObj,
              exp: expObj,
              volRel: volRelObj,
              oiData,
              
              ...(await montarScore(s, oiData))
            };
          } catch (err) {
            console.warn(`⚠️ Falha em montarScore ${s}:`, err.message);
            return { symbol: s, error: true };
          }
        })
      );

      resultados.push(...lote);
    }

    res.json(resultados);
  } catch (err) {
    console.error("❌ /score-lote erro:", err.message);
    res.status(500).json({ error: err.message });
  }
});


app.post("/score-lote", express.json(), async (req, res) => {
  const symbols = req.body.symbols || [];
  if (!symbols.length) return res.json([]);
  try {
    const resultados = await Promise.all(symbols.map(montarScore));
    res.json(resultados);
  } catch (err) {
    console.error("❌ Erro geral em POST /score-lote:", err.message);
    res.status(500).json({ error: true });
  }
});

// ===============================
// 🔥 Rota unificada do Heatmap
// ===============================
app.get("/mapa", async (req, res) => {
  try {
    const result = [];

    
    for (const [symbol, dados] of Object.entries(cacheLiquidados)) {
      const long = dados.sumLong || 0;
      const short = dados.sumShort || 0;

      
      const score = long - short;

      result.push({
        symbol,
        long,
        short,
        score,
        cap: dados.cap || false, 
        oiFlowScore: dados.oiFlowScore || 0,
        liqData5m: dados.liqData5m || null   
      });
    }

    res.json(result);
  } catch (err) {
    console.error("❌ Erro na rota /mapa:", err.message);
    res.status(500).json({ error: "Falha ao montar mapa" });
  }
});

// =====================[ ROTA LIQUIDAÇÕES ]======================
function processarLiquidacao(ev) {
  try {
    const s = ev.s; 
    if (!s) return;

    if (!cacheLiquidacoes[s]) {
      cacheLiquidacoes[s] = { long: 0, short: 0, eventos: [] };
    }

    const side = ev.S; 
    const price = Number(ev.p);
    const qty = Number(ev.q);
    const ts = ev.T || Date.now();

    
    if (!price || !qty || price <= 0 || qty <= 0 || !Number.isFinite(price * qty)) {
    
      return; 
    }

    const notional = price * qty;

    
    if (notional > 500_000_000) {
      console.warn(`⚠️ Liquidação anômala ignorada em ${s}: $${notional.toFixed(0)}`);
      return;
    }

    
    if (side === "BUY") cacheLiquidacoes[s].long += notional;
    if (side === "SELL") cacheLiquidacoes[s].short += notional;

    
    cacheLiquidacoes[s].eventos.push({
      S: side,
      p: price,
      q: qty,
      n: notional, 
      T: ts
    });

    
    if (cacheLiquidacoes[s].eventos.length > 200) {
      cacheLiquidacoes[s].eventos.shift();
    }

  } catch (err) {
    console.error("❌ Erro processarLiquidacao", err.message);
  }
}


// =====================[ ROTA LIQUIDAÇÕES ]======================
app.get("/liquidations/:symbol", (req, res) => {
  const { symbol } = req.params;
  if (!simboloValido(symbol)) {
    return res.status(400).json({ error: "Símbolo inválido", symbol });
  }

  const dados = cacheLiquidacoes[symbol] || { long: 0, short: 0, eventos: [] };

 
  const ultimos = (dados.eventos || []).slice(-200).reverse();

  
  const normalizar = (ev) => {
    const price = Number(ev.p ?? ev.price ?? 0);
    const qty   = Number(ev.q ?? ev.qty ?? 0);
    const notional = Number.isFinite(price) && Number.isFinite(qty) ? price * qty : 0;
    return {
      side: ev.S || ev.side || "UNK",
      price,
      qty,
      notional,
      ts: ev.T ?? ev.ts ?? Date.now()
    };
  };

  const hist = ultimos.map(normalizar);
  const histLong = hist.filter(ev => ev.side === "BUY");
  const histShort = hist.filter(ev => ev.side === "SELL");

  const resposta = {
    symbol,
    long: dados.long || 0,
    short: dados.short || 0,
    hist,
    histLong,
    histShort
  };

 
  res.json(resposta);
});

let wsLiquidados = null;
let ativosWS = [];
let wsLiquidadosConectando = false; 


function iniciarWSLiquidados() {
  const { prioritarios, secundarios } = cacheTopRadar.dados;
  const ativos = [...prioritarios, ...secundarios];
  if (!ativos.length) {
    console.warn("⚠️ Nenhum ativo disponível para WS de liquidações");
    return;
  }

 
  const iguais = ativos.length === ativosWS.length &&
                 ativos.every((s, i) => s === ativosWS[i]);
  if (iguais || wsLiquidadosConectando) return;

  ativosWS = ativos;

  
  if (wsLiquidados &&
      (wsLiquidados.readyState === WebSocket.OPEN ||
       wsLiquidados.readyState === WebSocket.CLOSING)) {
    try { wsLiquidados.close(); } catch (e) {
      console.warn("⚠️ Erro ao fechar WS liquidados existente:", e.message);
    }
  }

  const streams = ativos.map(s => s.toLowerCase() + "@forceOrder").join("/");
  const url = `wss://fstream.binance.com/stream?streams=${streams}`;

  wsLiquidadosConectando = true;
  wsLiquidados = new WebSocket(url);

  wsLiquidados.on("open", () => {
    console.log("📡 WS forceOrder conectado:", ativos.length, "ativos");
    wsLiquidadosConectando = false;
  });

  wsLiquidados.on("message", msg => {
    try {
      const payload = JSON.parse(msg);
      if (payload?.data?.o) {
        processarLiquidacao(payload.data.o);
      }
    } catch (err) {
      console.error("Erro WS forceOrder", err.message);
    }
  });

  wsLiquidados.on("close", () => {
    console.log("⚠️ WS forceOrder fechado, tentando reconectar em 15s");
    wsLiquidadosConectando = false;
 //   setTimeout(() => iniciarWSLiquidados(), 15000);
  });

  wsLiquidados.on("error", err => {
    console.error("❌ Erro WS forceOrder:", err.message || err);
    wsLiquidadosConectando = false;
    try { wsLiquidados.close(); } catch {}
  });
}


async function warmVisualHistoryForSymbol(symbol, tf = VISUAL_BOOTSTRAP_TF, limit = VISUAL_BOOTSTRAP_LIMIT) {
  const s = String(symbol || "").toUpperCase();
  const key = `${s}:${tf}:${limit}`;

  const atual = cacheCandles[s]?.[tf];
  if (Array.isArray(atual) && atual.length >= Math.min(limit, 20)) {
    return atual.slice(-limit);
  }

  if (visualBootstrapInFlight.has(key)) {
    return visualBootstrapInFlight.get(key);
  }

  const promise = (async () => {
    try {
      const kl = await fetchKlinesWithFallback(s, tf, "futures", limit);
      if (!Array.isArray(kl) || !kl.length) return [];

      if (!cacheCandles[s]) cacheCandles[s] = {};
      if (!Array.isArray(cacheCandles[s][tf])) cacheCandles[s][tf] = [];

      cacheCandles[s][tf] = kl.map(k => ({
        open: parseFloat(k[1]),
        close: parseFloat(k[4]),
        vol: parseFloat(k[7] ?? k[5] ?? 0),
        ts: Number(k[6] ?? k[0] ?? Date.now()),
        exp: 50,
        volRel: 1
      })).filter(c =>
        Number.isFinite(c.open) &&
        Number.isFinite(c.close) &&
        Number.isFinite(c.vol) &&
        Number.isFinite(c.ts)
      );

      if (cacheCandles[s][tf].length > 300) {
        cacheCandles[s][tf] = cacheCandles[s][tf].slice(-300);
      }

      return cacheCandles[s][tf].slice(-limit);
    } catch (err) {
      console.warn(`⚠️ warmVisualHistoryForSymbol falhou ${s} ${tf}:`, err.message || err);
      return [];
    } finally {
      visualBootstrapInFlight.delete(key);
    }
  })();

  visualBootstrapInFlight.set(key, promise);
  return promise;
}
// ===============================[ 19) INICIALIZA WS ]===============================


const clientesAtivos = new Set();         
const symbolSubscribers = new Map();      
const binanceStreams = new Map();        


const cacheTicker = new Map();
const cacheCandles = {};   
const VISUAL_BOOTSTRAP_TF = "5m";
const VISUAL_BOOTSTRAP_LIMIT = 30;
const visualBootstrapInFlight = new Map();


// ==========================================
// 🔁 Conexão Binance por Lote (ticker + candles)
// ==========================================


function configurarListenersBinance(ws, lote, idx) {
  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      const data = msg?.data;
      if (!data) return;

      
      if (data.e === "24hrTicker") {
        const symbol = data.s;
        const price = parseFloat(data.c);
        if (!Number.isFinite(price)) return;

        const ultimo = cacheTicker.get(symbol);
        const agora = Date.now();

       
        if (!ultimo || Math.abs(price - ultimo.price) / ultimo.price > 0.0001 || agora - ultimo.ts > 5000) {
          cacheTicker.set(symbol, { price, ts: agora });
          if (typeof precoCache !== "undefined" && precoCache?.set) {
            precoCache.set(symbol, price);
          }
          broadcast({ type: "ticker", symbol, price });
        }
      }

      
      if (data.e === "kline") {
        const { s: symbol, i: tf, o, c, v, q, T } = data.k;

        const open = parseFloat(o);
        const close = parseFloat(c);
        const vol = parseFloat(q ?? v);
        if (!Number.isFinite(close)) return;

        if (!cacheCandles[symbol]) cacheCandles[symbol] = {};
        if (!cacheCandles[symbol][tf]) cacheCandles[symbol][tf] = [];

        cacheCandles[symbol][tf].push({ open, close, vol, ts: T });
        if (cacheCandles[symbol][tf].length > 300) cacheCandles[symbol][tf].shift();

       
        const cacheKey = `${symbol}:${tf}`;
        const rsiData = cacheRSI.get(cacheKey);

        let expVal = (rsiData?.exp !== undefined && Number.isFinite(rsiData.exp)) ? rsiData.exp : null;
        let volRelVal = (rsiData?.volRel !== undefined && Number.isFinite(rsiData.volRel)) ? rsiData.volRel : null;


        if (expVal === null || volRelVal === null) {
          try {
            
            enqueueRSI(symbol, 'high');
            enqueueRSI('BTCUSDT', 'high');
          } catch (e) {
            
          }
        }

        
        const expToSend = expVal === null ? 50 : expVal;
        const volRelToSend = volRelVal === null ? 1 : volRelVal;

        
        if (process.env.DEBUG_RSI === "1" && (expVal === null || volRelVal === null)) {
          console.log(`[RSI-MISSING] will broadcast candle WITHOUT RSI for ${symbol}:${tf} -> exp:${expVal} volRel:${volRelVal} (enqueued RSI)`);
        }

        const payload = {
        type: "candle",
        symbol,
        tf,
        open,
        close,
        vol,
        ts: T,
        exp: expToSend,
        volRel: volRelToSend
      };

      broadcastToSymbol(symbol, payload);


               
      }
    } catch (err) {
      console.warn(`❌ Erro WS Binance [lote ${idx}]`, err && err.message ? err.message : err);
    }
  });

  ws.on("close", () => {
    console.warn(`⚠️ WS Binance fechado [lote ${idx}] - Reabrindo em 5s`);
    setTimeout(() => iniciarWsBinanceLotes(lote), 5000);
  });

  ws.on("error", (err) => {
    console.warn(`❌ WS Binance erro [lote ${idx}]`, err.message || err);
  });
}





// ===============================[ 22) START SERVER ]===============================
const server = app.listen(PORT, () => {
  console.log(`✅ Server online:${PORT}`, new Date().toLocaleTimeString());
});

const wss = new WebSocketServer({ server, path: "/ws" });

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "Painel uXe Crypto.html"));
});

inicializarServer();



// ===============================[ 23 ) Inicia conexões WS Binance ]=================================
 
function iniciarWsBinanceLotes(symbols) {
  if (!symbols?.length) return;

  console.log(`🚀 Iniciando WS Binance com ${symbols.length} símbolos...`);

 
  enqueueRSI("BTCUSDT", "high");

  
  primeRsiForList(symbols.slice(0, 5), "high");
  if (process.env.DEBUG_RSI === "1")
    console.log("[RSI-PRIME] enqueued top 5 symbols for RSI priming");

  // =====================================================
  // 🧠 Delay adaptativo conforme número de pares
  // =====================================================
  let baseDelay = 1500; // padrão 1.5s
  if (symbols.length > 280) baseDelay = 3000; 
  else if (symbols.length > 200) baseDelay = 2000;
  else if (symbols.length > 100) baseDelay = 1500; 
  else baseDelay = 1000; // leve → 1s

  console.log(`⚙️ Delay adaptativo definido em ${(baseDelay / 1000).toFixed(1)}s por lote`);


  const lotes = [];
  for (let i = 0; i < symbols.length; i += 20) {
    lotes.push(symbols.slice(i, i + 20));
  }

  // =====================================================
  // 🔁 Inicializa cada lote com streams completos (para top50 ou expandidos)
  // =====================================================
  lotes.forEach((lote, idx) => {
    setTimeout(() => {
      const streams = lote
        .map((s) => {
          const base = ["1h", "1d"]; 
          const extras = ["1m", "5m", "15m", "30m", "4h"]; 
          const timeframes =
            top50Volume.has(s) || isStreamExpandido(s) ? [...base, ...extras] : base;

          const tfStreams = timeframes
            .map((tf) => `${s.toLowerCase()}@kline_${tf}`)
            .join("/");

          return `${s.toLowerCase()}@ticker/${tfStreams}`;
        })
        .join("/");

      const url = `wss://stream.binance.com:9443/stream?streams=${streams}`;
      console.log(
        `📡 WS Lote ${idx + 1}/${lotes.length}: ${lote.length} símbolos (${url.length} chars)`
      );

      try {
       const ws = new WS(url);

    
      configurarListenersBinance(ws, lote, idx);

     
      ws.on("open", () => {
        console.log(`📡 WS Lote ${idx + 1}/${lotes.length} conectado (${lote.length} símbolos)`);

       
        lote.forEach(symbol => {
          const tf = "5m";
          const cache = cacheCandles[symbol]?.[tf];
          if (!cache || cache.length === 0) return;

          const ultimos = cache.slice(-30);
          const payload = {
            type: "candle_hist",
            symbol,
            tf,
            candles: ultimos.map(c => ({
              open: c.open,
              close: c.close,
              vol: c.vol,
              exp: c.exp ?? 50,
              volRel: c.volRel ?? 1,
              ts: c.ts
            }))
          };

          
          broadcastToSymbol(symbol, payload);
        });
      });

      ws.on("close", () => {
        console.warn(`⚠️ WS Lote ${idx + 1} fechado. Reabrindo em 10s...`);
        setTimeout(() => iniciarWsBinanceLotes(lote), 10_000);
      });

      ws.on("error", (err) => {
        console.error(`❌ Erro WS Lote ${idx + 1}:`, err.message || err);
      });
      } 
      
      catch (err) {
        console.error(`❌ Falha ao iniciar WS Lote ${idx + 1}:`, err.message || err);
      }
    }, idx * baseDelay); 
  });
}

// =======================================================
// ♻️ Recalcula e reinicia streams Binance dinamicamente
// =======================================================
function recomputeStreams() {
  try {
    console.log("🔄 Recomputando streams Binance...");

  
    if (global.wsAtivos && Array.isArray(global.wsAtivos)) {
      global.wsAtivos.forEach(ws => {
        try {
          ws.close();
        } catch (err) {
          console.warn("⚠️ Erro ao fechar WS ativo:", err.message || err);
        }
      });
    }
    global.wsAtivos = [];

   
    const todosSimbolos = new Set([
      ...top100Volume,
      ...top50Volume,
      ...Array.from(symbolsExpandido.keys())
    ]);

   
    const listaFinal = Array.from(todosSimbolos).slice(0, MAX_ATIVOS_RADAR);
    console.log(
      `🧭 ${listaFinal.length} símbolos ativos após recompute (Top50=${top50Volume.size}, Expandidos=${symbolsExpandido.size})`
    );

   
    iniciarWsBinanceLotes(listaFinal);

  } catch (err) {
    console.error("❌ Erro ao recomputar streams:", err.message || err);
  }
}

// =======================================================
// 🕒 Auto-Recompute periódico das streams Binance a cada 2 horas
// =======================================================
const TOP_REFRESH_MS = 2 * 60 * 60 * 1000;

setInterval(async () => {
  try {
    console.log("🔄 Refresh programado do TopRadar (2h)");
    await atualizarTopRadar();
  } catch (err) {
    console.warn("⚠️ Falha no refresh programado do TopRadar:", err.message);
  }
}, TOP_REFRESH_MS);


// ==========================================
// 🔊 Broadcast para todos os clientes WebSocket conectados
// ==========================================
function broadcast(obj) {
  const payload = JSON.stringify(obj);
  for (const ws of clientesAtivos) {
    if (ws.readyState === WebSocket.OPEN) ws.send(payload);
  }
}


function broadcastToSymbol(symbol, payload) {
  const set = symbolSubscribers.get(symbol);
  if (!set) return;
  const raw = JSON.stringify(payload);
  for (const c of set) {
    if (c.readyState === WebSocket.OPEN) c.send(raw);
  }
}

function subscribeClient(ws, symbols = []) {
  symbols.forEach(s => {
    const sym = s.toUpperCase();
    if (!symbolSubscribers.has(sym)) symbolSubscribers.set(sym, new Set());
    symbolSubscribers.get(sym).add(ws);
  });
  // recomputeStreams();
}

function unsubscribeClient(ws) {
  for (const [sym, set] of symbolSubscribers.entries()) {
    set.delete(ws);
    if (set.size === 0) {
      // closeStream(sym);
    }
  }
}

wss.on("connection", (ws) => {
  clientesAtivos.add(ws);
  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);
      if (data.action === "subscribe" && Array.isArray(data.symbols)) {
        subscribeClient(ws, data.symbols);
      } else if (data.action === "unsubscribe" && Array.isArray(data.symbols)) {
        data.symbols.forEach(sym => {
          const set = symbolSubscribers.get(sym.toUpperCase());
          if (set) set.delete(ws);
        });
      }
    } catch (e) {
      console.warn("⚠️ Erro WS client:", e.message);
    }
  });

  if (msg?.action === "subscribe" && Array.isArray(msg.symbols)) {
  const symbols = [...new Set(msg.symbols.map(s => String(s || "").toUpperCase()))]
    .filter(Boolean)
    .slice(0, 120);

  for (const symbol of symbols) {
    const enviadoDoCache = sendVisualHistoryToClient(ws, symbol, VISUAL_BOOTSTRAP_TF, VISUAL_BOOTSTRAP_LIMIT);

    if (!enviadoDoCache) {
      warmVisualHistoryForSymbol(symbol, VISUAL_BOOTSTRAP_TF, VISUAL_BOOTSTRAP_LIMIT)
        .then(() => {
          sendVisualHistoryToClient(ws, symbol, VISUAL_BOOTSTRAP_TF, VISUAL_BOOTSTRAP_LIMIT);
        })
        .catch(err => {
          console.warn(`⚠️ Falha bootstrap visual pós-subscribe ${symbol}:`, err.message || err);
        });
    }
  }
}  

  ws.on("close", () => {
    clientesAtivos.delete(ws);
    unsubscribeClient(ws);
  });
});


let recomputeTimer = null;













