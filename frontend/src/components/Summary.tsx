import React from "react";
import { BacktestMetadata } from "../types";

interface Props {
  metadata?: BacktestMetadata;
  stats?: Record<string, any>;
  onSymbolChange: (sym: string) => void;
  currentSymbol?: string;
}

const Summary: React.FC<Props> = ({ metadata, stats, onSymbolChange, currentSymbol }) => {
  if (!metadata) {
    return (
      <div className="card">
        <div style={{ textAlign: "center", padding: "20px", color: "#94a3b8" }}>
          加载中...
        </div>
      </div>
    );
  }

  const symbols = metadata.stock_list || [];
  const totalReturnRaw = stats?.["Total Return [%]"];
  const totalReturn = totalReturnRaw !== undefined && totalReturnRaw !== null ? Number(totalReturnRaw) : undefined;
  const maxDrawdownRaw = stats?.["Max Drawdown [%]"];
  const maxDrawdown = maxDrawdownRaw !== undefined && maxDrawdownRaw !== null ? Number(maxDrawdownRaw) : undefined;
  const sharpeRatioRaw = stats?.["Sharpe Ratio"];
  const sharpeRatio = sharpeRatioRaw !== undefined && sharpeRatioRaw !== null ? Number(sharpeRatioRaw) : undefined;

  return (
    <div className="card" style={{ padding: "16px" }}>
      <h3 style={{ marginTop: 0, marginBottom: 16, fontSize: 18, fontWeight: 600 }}>回测基本信息</h3>
      
      {/* 策略和回测时间 */}
      <div className="grid" style={{ gap: 12, marginBottom: 16 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>使用策略</div>
          <div style={{ fontSize: 16, fontWeight: 600 }}>{metadata.strategy_name}</div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>回测时间</div>
          <div style={{ fontSize: 16, fontWeight: 600 }}>{metadata.timestamp}</div>
        </div>
      </div>

      {/* 股票选择 */}
      <div style={{ marginBottom: 16 }}>
        <label style={{ display: "block", fontSize: 13, marginBottom: 8, fontWeight: 500 }}>
          选择股票代码：
        </label>
        <select
          className="select"
          value={currentSymbol || ""}
          onChange={(e) => onSymbolChange(e.target.value)}
          style={{ width: "100%", fontSize: 14, padding: "8px 12px" }}
        >
          <option value="">请选择股票</option>
          {symbols.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>

      {/* 关键指标 */}
      <div className="grid" style={{ gap: 12 }}>
        <div style={{ display: "flex", flexDirection: "column", gap: 4, padding: "12px", backgroundColor: "#0f172a", borderRadius: "8px" }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>总收益</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: totalReturn !== undefined && totalReturn >= 0 ? "#22c55e" : "#ef4444" }}>
            {totalReturn !== undefined ? `${Number(totalReturn).toFixed(2)}%` : "-"}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4, padding: "12px", backgroundColor: "#0f172a", borderRadius: "8px" }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>最大回撤</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: maxDrawdown !== undefined && maxDrawdown < 0 ? "#ef4444" : "#e2e8f0" }}>
            {maxDrawdown !== undefined ? `${Number(maxDrawdown).toFixed(2)}%` : "-"}
          </div>
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 4, padding: "12px", backgroundColor: "#0f172a", borderRadius: "8px" }}>
          <div style={{ fontSize: 12, opacity: 0.7 }}>夏普比率</div>
          <div style={{ fontSize: 20, fontWeight: 700, color: "#e2e8f0" }}>
            {sharpeRatio !== undefined ? `${Number(sharpeRatio).toFixed(2)}` : "-"}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Summary;

