import { BacktestListItem, SummaryResponse, OhlcvRecord, OrderRecord, MinuteRecord, DateRangeFilter } from "./types";

// 生产环境（Docker + Nginx 反代）默认走同源 /api；本地开发可用 VITE_API_BASE 覆盖
const API_BASE = import.meta.env.VITE_API_BASE || "/api";

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`请求失败 ${res.status}`);
  }
  return res.json();
}

// 辅助函数：将时间区间参数添加到 URLSearchParams
function appendDateRangeParams(qs: URLSearchParams, dateRange?: DateRangeFilter) {
  if (dateRange?.startDate) qs.set("start_date", dateRange.startDate);
  if (dateRange?.endDate) qs.set("end_date", dateRange.endDate);
}

export function fetchResults(): Promise<BacktestListItem[]> {
  return getJSON<BacktestListItem[]>("/results");
}

export function fetchSummary(
  resultId: string, 
  equityPoints = 400, 
  symbol?: string,
  dateRange?: DateRangeFilter
): Promise<SummaryResponse> {
  const qs = new URLSearchParams({ equity_points: String(equityPoints) });
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  return getJSON<SummaryResponse>(`/results/${resultId}/summary?${qs.toString()}`);
}

export function fetchDaily(resultId: string, symbol?: string, dateRange?: DateRangeFilter): Promise<OhlcvRecord[]> {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<OhlcvRecord[]>(`/results/${resultId}/daily${suffix}`);
}

export function fetchMinute(resultId: string, symbol: string, date?: string): Promise<MinuteRecord[]> {
  const qs = new URLSearchParams({ symbol });
  if (date) qs.set("date", date);
  return getJSON<MinuteRecord[]>(`/results/${resultId}/minute?${qs.toString()}`);
}

export function fetchOrders(resultId: string, symbol?: string, date?: string, dateRange?: DateRangeFilter): Promise<OrderRecord[]> {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  if (date) qs.set("date", date);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<OrderRecord[]>(`/results/${resultId}/orders${suffix}`);
}

// 获取某个标的的全量订单（不按日期过滤，但支持时间区间）
export function fetchAllOrders(resultId: string, symbol?: string, dateRange?: DateRangeFilter): Promise<OrderRecord[]> {
  return fetchOrders(resultId, symbol, undefined, dateRange);
}

export function fetchTrades(resultId: string, symbol?: string, dateRange?: DateRangeFilter) {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<any[]>(`/results/${resultId}/trades${suffix}`);
}

export interface PositionRecord {
  datetime: string;
  position: number;
}

export interface PnlRecord {
  datetime: string;
  pnl: number;
  cumulative_pnl: number;
}

// 新增 Equity Record 接口
export interface EquityRecord {
  datetime: string;
  value: number; // 统一为 value，后端会做重命名
}

export function fetchPositions(resultId: string, symbol?: string, dateRange?: DateRangeFilter): Promise<PositionRecord[]> {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<PositionRecord[]>(`/results/${resultId}/positions${suffix}`);
}

export function fetchPnl(resultId: string, symbol?: string, dateRange?: DateRangeFilter): Promise<PnlRecord[]> {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<PnlRecord[]>(`/results/${resultId}/pnl${suffix}`);
}

// 修改 fetchEquity 支持 symbol 和时间区间
export function fetchEquity(resultId: string, symbol?: string, dateRange?: DateRangeFilter): Promise<EquityRecord[]> {
  const qs = new URLSearchParams();
  if (symbol) qs.set("symbol", symbol);
  appendDateRangeParams(qs, dateRange);
  const suffix = qs.toString() ? `?${qs.toString()}` : "";
  return getJSON<EquityRecord[]>(`/results/${resultId}/equity${suffix}`);
}
