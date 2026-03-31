import { useState } from "react";
import { BacktestMetadata } from "../types";
import { formatBacktestTime } from "../utils";

interface Props {
  metadata: BacktestMetadata;
  stats?: Record<string, any>;
}

const STATS_CN: Record<string, string> = {
  'Start': '开始时间',
  'End': '结束时间',
  'Period': '回测时长',
  'Start Value': '初始资金',
  'End Value': '结束资金',
  'Total Return [%]': '总收益率 [%]',
  'Benchmark Return [%]': '基准收益率 [%]',
  'Max Gross Exposure [%]': '最大仓位占用 [%]',
  'Total Fees Paid': '总手续费',
  'Max Drawdown [%]': '最大回撤 [%]',
  'Max Drawdown Duration': '最大回撤持续时间',
  'Total Trades': '总交易次数',
  'Total Closed Trades': '已平仓交易数',
  'Total Open Trades': '持仓中交易数',
  'Open Trade PnL': '当前持仓盈亏',
  'Win Rate [%]': '胜率 [%]',
  'Best Trade [%]': '最佳单笔收益 [%]',
  'Worst Trade [%]': '最差单笔收益 [%]',
  'Avg Winning Trade [%]': '平均盈利 [%]',
  'Avg Losing Trade [%]': '平均亏损 [%]',
  'Avg Winning Trade Duration': '平均持仓时间(赢)',
  'Avg Losing Trade Duration': '平均持仓时间(亏)',
  'Profit Factor': '盈亏比',
  'Expectancy': '单笔期望收益',
  'Sharpe Ratio': '夏普比率',
  'Calmar Ratio': '卡玛比率',
  'Omega Ratio': '欧米伽比率',
  'Sortino Ratio': '索提诺比率'
};

function formatDuration(val: string): string {
  if (!val) return "";
  
  // 1. 尝试解析 ISO 8601 Duration (e.g., "P84DT13H25M0S")
  // 简单正则匹配：P(..D)?T(..H)?(..M)?(..S)?
  // 注意：实际 ISO 8601 很复杂，这里只处理 vectorbt 输出的这种常见格式
  const isoRegex = /P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?/;
  const match = String(val).match(isoRegex);

  if (match) {
    const days = match[1] ? parseInt(match[1], 10) : 0;
    const hours = match[2] ? parseInt(match[2], 10) : 0;
    const minutes = match[3] ? parseInt(match[3], 10) : 0;
    const seconds = match[4] ? parseFloat(match[4]) : 0;

    // 如果完全没匹配到任何数字（比如无效字符串），fallback 到原值
    if (days === 0 && hours === 0 && minutes === 0 && seconds === 0 && val !== "P0D") {
         // 可能是 "1074 days ..." 这种非 ISO 格式，走下面的逻辑
    } else {
        const parts = [];
        if (days > 0) parts.push(`${days}天`);
        if (hours > 0) parts.push(`${hours}小时`);
        if (minutes > 0) parts.push(`${minutes}分`);
        // 秒数保留整数或忽略，视需求而定，这里保留
        if (seconds > 0) parts.push(`${Math.floor(seconds)}秒`);
        
        return parts.join(" ") || "0天";
    }
  }

  // 2. 处理 Pandas timedelta 字符串 (e.g., "1074 days 05:29:00")
  return String(val)
    .replace(/days?/i, "天")
    .replace(/00:00:00/, "") // 如果是整天，去掉后面的时间
    .trim();
}

function formatValue(key: string, value: any) {
  if (value === undefined || value === null) return "N/A";
  
  // 时间处理
  if (key === "Start" || key === "End") {
    // 假设是 ISO 或类 ISO 字符串，尝试格式化
    // 如果是 "2023-01-03 09:31:00"，直接返回即可，只要确保分隔符一致
    return String(value).replace("T", " ");
  }
  
  // 时长处理
  if (key === "Period" || key.includes("Duration")) {
    return formatDuration(String(value));
  }

  const num = Number(value);
  
  if (!Number.isNaN(num)) {
    if (key === "Max Drawdown [%]") {
        // 最大回撤用绿色
        return <span style={{ color: "#22c55e", fontWeight: "bold" }}>{num.toFixed(2)}%</span>;
    }
    if (key.includes("[%]")) {
      const color = num > 0 ? "#ef4444" : num < 0 ? "#22c55e" : "inherit";
      return <span style={{ color, fontWeight: "bold" }}>{num.toFixed(2)}%</span>;
    }
    if (key === "Profit Factor" || key.includes("Ratio")) {
      return num.toFixed(3);
    }
    if (key === "Total Fees Paid" || key === "Start Value" || key === "End Value" || key === "Expectancy" || key === "Open Trade PnL") {
       return num.toFixed(2);
    }
    return String(value);
  }
  return String(value);
}

export default function SummaryInfo({ metadata, stats }: Props) {
  const [isExpanded, setIsExpanded] = useState(false);
  const statsData = stats?.stats || {}; 

  // 1. 优先展示的关键指标
  const keyMetrics = [
    "Total Return [%]",
    "Max Drawdown [%]",
    "Sharpe Ratio",
    "Win Rate [%]"
  ];

  // 2. 剩余指标 (过滤掉已展示的)
  const otherMetrics = Object.keys(statsData).filter(k => !keyMetrics.includes(k));

  return (
    <div className="card summary-section" style={{ position: "relative" }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
          <h2 style={{ marginTop: 0 }}>回测详细指标</h2>
          
          {/* 展开/收起按钮 */}
          <button 
            onClick={() => setIsExpanded(!isExpanded)}
            style={{
                background: "transparent",
                border: "1px solid #334155",
                borderRadius: "4px",
                color: "#94a3b8",
                cursor: "pointer",
                padding: "4px 8px",
                display: "flex",
                alignItems: "center",
                gap: "4px",
                fontSize: "12px",
                transition: "all 0.2s"
            }}
            title={isExpanded ? "收起详细信息" : "展开详细信息"}
          >
            {isExpanded ? "收起" : "展开"}
            <svg 
                width="12" 
                height="12" 
                viewBox="0 0 24 24" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="2" 
                strokeLinecap="round" 
                strokeLinejoin="round"
                style={{ transform: isExpanded ? "rotate(180deg)" : "rotate(0deg)", transition: "transform 0.2s" }}
            >
                <polyline points="6 9 12 15 18 9"></polyline>
            </svg>
          </button>
      </div>
      
      {/* 头部基础信息 */}
      <div className="grid" style={{ marginBottom: 20, gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))" }}>
         <div>
          <div style={{ opacity: 0.6, fontSize: "0.9em" }}>策略名称</div>
          <div style={{ fontSize: "1em", fontWeight: "bold" }}>{metadata.strategy_name}</div>
        </div>
        <div>
          <div style={{ opacity: 0.6, fontSize: "0.9em" }}>回测生成时间</div>
          <div>{formatBacktestTime(metadata.timestamp)}</div>
        </div>
      </div>

      <hr style={{ borderColor: "#334155", opacity: 0.5, margin: "16px 0" }} />

      {/* 核心指标高亮 (始终展示) */}
      <div className="grid" style={{ marginBottom: isExpanded ? 20 : 0, gridTemplateColumns: "repeat(4, 1fr)" }}>
        {keyMetrics.map(key => (
          <div key={key} style={{ background: "#1e293b", padding: "12px", borderRadius: "8px" }}>
             <div style={{ opacity: 0.7, fontSize: "0.85em", marginBottom: 4 }}>{STATS_CN[key] || key}</div>
             <div style={{ fontSize: "1.2em" }}>{formatValue(key, statsData[key])}</div>
          </div>
        ))}
      </div>

      {/* 详细指标列表 (可折叠) */}
      {isExpanded && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: "16px 24px", marginTop: "20px" }}>
            {otherMetrics.map(key => (
            <div key={key} style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", borderBottom: "1px solid #334155", paddingBottom: "4px" }}>
                <span style={{ opacity: 0.6, fontSize: "0.85em" }}>{STATS_CN[key] || key}</span>
                <span style={{ fontSize: "0.95em", textAlign: "right" }}>{formatValue(key, statsData[key])}</span>
            </div>
            ))}
        </div>
      )}
      
      {Object.keys(statsData).length === 0 && (
        <div style={{ padding: 20, textAlign: "center", color: "#64748b" }}>
          暂无统计数据
        </div>
      )}
    </div>
  );
}
