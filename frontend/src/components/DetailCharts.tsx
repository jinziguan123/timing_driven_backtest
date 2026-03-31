import React, { useRef } from "react";
import Plot from "react-plotly.js";
import { OhlcvRecord, OrderRecord, MinuteRecord } from "../types";
import { PositionRecord, PnlRecord } from "../api";

interface Props {
  daily: OhlcvRecord[];
  dailyOrders: OrderRecord[];
  positions: PositionRecord[];
  pnl: PnlRecord[];
  minute: MinuteRecord[];
  minuteOrders: OrderRecord[];
  onOpenDay: (date: string) => void;
  onBack: () => void;
  viewMode: "daily" | "intraday";
  selectedDate?: string;
  currentSymbol?: string;
}

function toDateStr(v: string) {
  return new Date(v).toISOString().slice(0, 10);
}

const DetailCharts: React.FC<Props> = ({
  daily,
  dailyOrders,
  positions,
  pnl,
  minute,
  minuteOrders,
  onOpenDay,
  onBack,
  viewMode,
  selectedDate,
  currentSymbol
}) => {
  // 保存图表的视图状态，避免双击时重置
  const dailyKLayoutRef = useRef<{ xaxis?: { range?: [any, any] }; yaxis?: { range?: [any, any] } } | null>(null);
  
  // 聚合日级买卖标记（合并同日多次成交）
  const buyMarks: { x: string[]; y: number[] } = { x: [], y: [] };
  const sellMarks: { x: string[]; y: number[] } = { x: [], y: [] };
  
  // 创建日期到日K记录的映射
  const dailyMap = daily.reduce((acc: Record<string, OhlcvRecord>, cur: OhlcvRecord) => {
    const dateKey = cur.datetime.slice(0, 10);
    acc[dateKey] = cur;
    return acc;
  }, {} as Record<string, OhlcvRecord>);
  
  // 按日期分组订单，合并同日多次交易
  const grouped: Record<string, { hasBuy: boolean; hasSell: boolean }> = {};
  dailyOrders.forEach((o) => {
    if (!o.Timestamp) return;
    const d = new Date(o.Timestamp).toISOString().slice(0, 10);
    if (!d) return;
    grouped[d] = grouped[d] || { hasBuy: false, hasSell: false };
    const side = (o.Side || "").toLowerCase();
    if (side === "buy") grouped[d].hasBuy = true;
    if (side === "sell") grouped[d].hasSell = true;
  });
  
  // 生成买卖标记点
  Object.entries(grouped).forEach(([d, flags]) => {
    const rec = dailyMap[d];
    if (!rec) return;
    // 计算价格偏移量（相对于K线高低点的百分比）
    const priceRange = rec.high - rec.low;
    const offset = Math.max(priceRange * 0.02, 0.01);
    
    if (flags.hasBuy) {
      buyMarks.x.push(rec.datetime);
      buyMarks.y.push(rec.low - offset);
    }
    if (flags.hasSell) {
      sellMarks.x.push(rec.datetime);
      sellMarks.y.push(rec.high + offset);
    }
  });

  // 日线视图：三张图表
  if (viewMode === "daily") {
    const dailyKData = [
      {
        type: "candlestick",
        x: daily.map((d) => d.datetime),
        open: daily.map((d) => d.open),
        high: daily.map((d) => d.high),
        low: daily.map((d) => d.low),
        close: daily.map((d) => d.close),
        increasing: { line: { color: "red" } },
        decreasing: { line: { color: "green" } },
        name: "日K"
      },
      {
        x: buyMarks.x,
        y: buyMarks.y,
        mode: "markers",
        name: "买入",
        marker: { color: "red", symbol: "triangle-up", size: 12 }
      },
      {
        x: sellMarks.x,
        y: sellMarks.y,
        mode: "markers",
        name: "卖出",
        marker: { color: "green", symbol: "triangle-down", size: 12 }
      }
    ];

    const commonLayout = {
      dragmode: "pan",
      hovermode: "x unified",
      spikemode: "across",
      plot_bgcolor: "#0f172a",
      paper_bgcolor: "#0f172a",
      font: { color: "#e2e8f0" },
      xaxis: {
        rangeslider: { visible: false },
        type: "date",
        tickformat: "%Y-%m-%d",
        showspikes: true,
        spikethickness: 1,
        spikedash: "dot",
        spikecolor: "#f87171"
      },
      yaxis: {
        showspikes: true,
        spikethickness: 1,
        spikedash: "dot",
        spikecolor: "#f87171"
      }
    };

    const commonConfig = {
      responsive: true,
      scrollZoom: true,
      displaylogo: false,
      modeBarButtonsToRemove: ["lasso2d", "select2d"]
    };

    // 按时间排序交易记录（日线视图）
    const sortedDailyOrders = [...dailyOrders].sort((a, b) => {
      const timeA = a.Timestamp ? new Date(a.Timestamp).getTime() : 0;
      const timeB = b.Timestamp ? new Date(b.Timestamp).getTime() : 0;
      return timeA - timeB;
    });

    return (
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        {/* 当前查看股票代码 */}
        {currentSymbol && (
          <div className="card" style={{ padding: "12px 16px", backgroundColor: "#0f172a" }}>
            <div style={{ fontSize: 16, fontWeight: 600 }}>
              当前查看股票代码：<span style={{ color: "#60a5fa", fontSize: 18 }}>{currentSymbol}</span>
            </div>
          </div>
        )}
        
        {/* 日K线图 */}
        <div className="card">
          <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16, fontWeight: 600 }}>
            日K线图（双击蜡烛线查看当日分钟线）
          </h3>
          <Plot
            key="daily-chart"
            data={dailyKData}
            layout={{
              ...commonLayout,
              ...(dailyKLayoutRef.current || {}),
              title: "",
              height: 400,
              showlegend: true,
              legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 }
            }}
            config={commonConfig}
            useResizeHandler={true}
            style={{ width: "100%", height: "100%" }}
            onRelayout={(eventData) => {
              // 保存当前的视图状态（缩放、平移等）
              if (eventData) {
                // 检查是否是重置操作（双击空白区域会设置 autorange 为 true）
                const hasAutorange = 
                  eventData["xaxis.autorange"] === true || 
                  eventData["yaxis.autorange"] === true;
                
                // 检查是否有 range 被清除（设置为 undefined 或 null）
                const hasRangeCleared = 
                  (eventData["xaxis.range[0]"] === undefined && eventData["xaxis.range[1]"] === undefined) ||
                  (eventData["yaxis.range[0]"] === undefined && eventData["yaxis.range[1]"] === undefined);
                
                if (hasAutorange || (hasRangeCleared && !dailyKLayoutRef.current)) {
                  // 清除保存的布局状态，让图表恢复原始大小
                  dailyKLayoutRef.current = null;
                } else {
                  // 保存当前的视图状态
                  const newLayout: any = {};
                  if (eventData["xaxis.range[0]"] !== undefined && eventData["xaxis.range[1]"] !== undefined) {
                    newLayout.xaxis = { range: [eventData["xaxis.range[0]"], eventData["xaxis.range[1]"]] };
                  }
                  if (eventData["yaxis.range[0]"] !== undefined && eventData["yaxis.range[1]"] !== undefined) {
                    newLayout.yaxis = { range: [eventData["yaxis.range[0]"], eventData["yaxis.range[1]"]] };
                  }
                  if (Object.keys(newLayout).length > 0) {
                    dailyKLayoutRef.current = { ...dailyKLayoutRef.current, ...newLayout };
                  }
                }
              }
            }}
            onDoubleClick={(ev) => {
              // 双击日K线数据点，打开分钟线视图
              if (ev && ev.points && ev.points.length > 0) {
                const pt = ev.points[0];
                if (pt && pt.x) {
                  const currentX = pt.x as string;
                  const dateStr = toDateStr(currentX);
                  onOpenDay(dateStr);
                }
              }
            }}
          />
        </div>
        
        {/* 交易记录表格（日线视图） */}
        <div className="card">
          <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16, fontWeight: 600 }}>交易记录表</h3>
          <div style={{ maxHeight: "400px", overflowY: "auto" }}>
            <table className="table" style={{ fontSize: 13 }}>
              <thead style={{ position: "sticky", top: 0, backgroundColor: "#1e293b", zIndex: 10 }}>
                <tr>
                  <th style={{ padding: "8px" }}>时间</th>
                  <th style={{ padding: "8px" }}>方向</th>
                  <th style={{ padding: "8px" }}>价格</th>
                  <th style={{ padding: "8px" }}>数量</th>
                  <th style={{ padding: "8px" }}>手续费</th>
                  <th style={{ padding: "8px" }}>成交金额</th>
                </tr>
              </thead>
              <tbody>
                {sortedDailyOrders.map((o, idx) => {
                  const amount = (o.Price || 0) * (o.Size || 0);
                  const sideColor = (o.Side || "").toLowerCase() === "buy" ? "#ef4444" : "#22c55e";
                  return (
                    <tr key={idx}>
                      <td style={{ padding: "6px 8px" }}>{o.Timestamp || "-"}</td>
                      <td style={{ padding: "6px 8px", color: sideColor, fontWeight: 500 }}>
                        {o.Side || "-"}
                      </td>
                      <td style={{ padding: "6px 8px" }}>{o.Price?.toFixed(2) || "-"}</td>
                      <td style={{ padding: "6px 8px" }}>{o.Size || "-"}</td>
                      <td style={{ padding: "6px 8px" }}>{o.Fees ? o.Fees.toFixed(2) : "-"}</td>
                      <td style={{ padding: "6px 8px" }}>{amount.toFixed(2)}</td>
                    </tr>
                  );
                })}
                {sortedDailyOrders.length === 0 && (
                  <tr>
                    <td colSpan={6} style={{ textAlign: "center", padding: "20px", color: "#94a3b8" }}>
                      暂无成交记录
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          {sortedDailyOrders.length > 0 && (
            <div style={{ marginTop: 12, padding: "8px", backgroundColor: "#1e293b", borderRadius: "4px", fontSize: 12 }}>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span>总成交笔数：{sortedDailyOrders.length}</span>
                <span>买入：{sortedDailyOrders.filter((o) => (o.Side || "").toLowerCase() === "buy").length} 笔</span>
                <span>卖出：{sortedDailyOrders.filter((o) => (o.Side || "").toLowerCase() === "sell").length} 笔</span>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }

  // 分钟线视图：显示分钟K线蜡烛图和当日交易记录
  const buyOrders = minuteOrders.filter((o) => (o.Side || "").toLowerCase() === "buy");
  const sellOrders = minuteOrders.filter((o) => (o.Side || "").toLowerCase() === "sell");

  // 构建分钟K线蜡烛图数据
  const minuteData = minute.length > 0 ? [
    {
      type: "candlestick" as const,
      x: minute.map((d) => d.datetime),
      open: minute.map((d) => d.open),
      high: minute.map((d) => d.high),
      low: minute.map((d) => d.low),
      close: minute.map((d) => d.close),
      increasing: { line: { color: "red" } },
      decreasing: { line: { color: "green" } },
      name: "分钟K线"
    },
    ...(buyOrders.length > 0 ? [{
      x: buyOrders.map((o) => o.Timestamp),
      y: buyOrders.map((o) => o.Price),
      mode: "markers" as const,
      name: "买入",
      marker: { color: "#ef4444", symbol: "triangle-up" as const, size: 12 }
    }] : []),
    ...(sellOrders.length > 0 ? [{
      x: sellOrders.map((o) => o.Timestamp),
      y: sellOrders.map((o) => o.Price),
      mode: "markers" as const,
      name: "卖出",
      marker: { color: "#22c55e", symbol: "triangle-down" as const, size: 12 }
    }] : [])
  ] : [
    {
      x: [] as string[],
      open: [] as number[],
      high: [] as number[],
      low: [] as number[],
      close: [] as number[],
      type: "candlestick" as const,
      name: "暂无数据"
    }
  ];

  // 按时间排序交易记录
  const sortedOrders = [...minuteOrders].sort((a, b) => {
    const timeA = a.Timestamp ? new Date(a.Timestamp).getTime() : 0;
    const timeB = b.Timestamp ? new Date(b.Timestamp).getTime() : 0;
    return timeA - timeB;
  });

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
      {/* 分钟线图表 */}
      <div className="card">
        <div style={{ marginBottom: 12, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <button onClick={onBack} style={{ fontSize: 14 }}>← 返回日线视图</button>
          <div style={{ opacity: 0.8, fontSize: 14 }}>当前日期：{selectedDate}</div>
        </div>
        <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16, fontWeight: 600 }}>
          {selectedDate ? `分钟K线图表 - ${selectedDate}` : "分钟K线图表"}
        </h3>
        {minute.length > 0 ? (
          <Plot
            key={`minute-${selectedDate}`}
            data={minuteData}
            layout={{
              title: "",
              height: 500,
              xaxis: {
                rangeslider: { visible: false },
                type: "date",
                tickformat: "%Y-%m-%d %H:%M",
                showspikes: true,
                spikethickness: 1,
                spikedash: "dot",
                spikecolor: "#f87171"
              },
              yaxis: {
                showspikes: true,
                spikethickness: 1,
                spikedash: "dot",
                spikecolor: "#f87171"
              },
              hovermode: "x unified",
              dragmode: "pan",
              spikemode: "across",
              plot_bgcolor: "#0f172a",
              paper_bgcolor: "#0f172a",
              font: { color: "#e2e8f0" },
              showlegend: true,
              legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 }
            }}
            config={{
              responsive: true,
              scrollZoom: true,
              displaylogo: false,
              modeBarButtonsToRemove: ["lasso2d", "select2d"]
            }}
            useResizeHandler={true}
            style={{ width: "100%", height: "100%" }}
          />
        ) : (
          <div style={{ padding: "40px", textAlign: "center", color: "#94a3b8" }}>
            正在加载分钟线数据...
          </div>
        )}
      </div>
      
      {/* 当日交易记录表格 */}
      <div className="card">
        <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16, fontWeight: 600 }}>当日交易记录</h3>
        <div style={{ maxHeight: "400px", overflowY: "auto" }}>
          <table className="table" style={{ fontSize: 13 }}>
            <thead style={{ position: "sticky", top: 0, backgroundColor: "#1e293b", zIndex: 10 }}>
              <tr>
                <th style={{ padding: "8px" }}>时间</th>
                <th style={{ padding: "8px" }}>方向</th>
                <th style={{ padding: "8px" }}>价格</th>
                <th style={{ padding: "8px" }}>数量</th>
                <th style={{ padding: "8px" }}>手续费</th>
                <th style={{ padding: "8px" }}>成交金额</th>
              </tr>
            </thead>
            <tbody>
              {sortedOrders.map((o, idx) => {
                const amount = (o.Price || 0) * (o.Size || 0);
                const sideColor = (o.Side || "").toLowerCase() === "buy" ? "#ef4444" : "#22c55e";
                return (
                  <tr key={idx}>
                    <td style={{ padding: "6px 8px" }}>{o.Timestamp || "-"}</td>
                    <td style={{ padding: "6px 8px", color: sideColor, fontWeight: 500 }}>
                      {o.Side || "-"}
                    </td>
                    <td style={{ padding: "6px 8px" }}>{o.Price?.toFixed(2) || "-"}</td>
                    <td style={{ padding: "6px 8px" }}>{o.Size || "-"}</td>
                    <td style={{ padding: "6px 8px" }}>{o.Fees ? o.Fees.toFixed(2) : "-"}</td>
                    <td style={{ padding: "6px 8px" }}>{amount.toFixed(2)}</td>
                  </tr>
                );
              })}
              {sortedOrders.length === 0 && (
                <tr>
                  <td colSpan={6} style={{ textAlign: "center", padding: "20px", color: "#94a3b8" }}>
                    暂无成交记录
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        {sortedOrders.length > 0 && (
          <div style={{ marginTop: 12, padding: "8px", backgroundColor: "#1e293b", borderRadius: "4px", fontSize: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>总成交笔数：{sortedOrders.length}</span>
              <span>买入：{buyOrders.length} 笔</span>
              <span>卖出：{sellOrders.length} 笔</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default DetailCharts;
