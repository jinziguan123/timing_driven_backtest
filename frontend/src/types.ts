// 时间区间筛选参数
export interface DateRangeFilter {
  startDate?: string; // 格式：YYYY-MM-DD
  endDate?: string;   // 格式：YYYY-MM-DD
}

export interface BacktestMetadata {
  result_id: string;
  strategy_name: string;
  timestamp: string;
  stock_list?: string[];
  available_days?: string[];
  start_time?: string; // 回测数据的开始时间
  end_time?: string;   // 回测数据的结束时间
  data_shape?: {
    time_points: number;
    stocks: number;
  };
  end_value?: number | string;
  total_return?: number | string;
}

export interface BacktestListItem extends BacktestMetadata {}

export interface EquityPoint {
  datetime: string;
  [key: string]: number | string;
}

export interface SummaryResponse {
  metadata: BacktestMetadata;
  stats: Record<string, any>;
  equity: EquityPoint[];
}

export interface OhlcvRecord {
  datetime: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
  [key: string]: number | string | undefined;
}

export interface OrderRecord {
  Id?: number;
  Column: string;
  Timestamp: string;
  Price: number;
  Size: number;
  Fees?: number;
  Side?: string;
  [key: string]: any;
}

export interface MinuteRecord extends OhlcvRecord {
  position?: number;
  cumulative_pnl?: number;
}

