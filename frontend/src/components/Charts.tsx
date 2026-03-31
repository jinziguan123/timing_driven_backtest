import React from "react";
import Plot from "react-plotly.js";
import { OhlcvRecord, MinuteRecord, OrderRecord } from "../types";

interface Props {
  daily: OhlcvRecord[];
  dailyOrders: OrderRecord[];
  minute: MinuteRecord[];
  orders: OrderRecord[];
  onOpenDay: (date: string) => void;
  onBack: () => void;
  viewMode: "daily" | "intraday";
  selectedDate?: string;
}

function toDateStr(v: string) {
  return new Date(v).toISOString().slice(0, 10);
}

const Charts: React.FC<Props> = ({ daily, dailyOrders, minute, orders, onOpenDay, onBack, viewMode, selectedDate }) => {
  // 聚合日级买卖标记（合并同日多次成交）
  const buyMarks: { x: string[]; y: number[] } = { x: [], y: [] };
  const sellMarks: { x: string[]; y: number[] } = { x: [], y: [] };
  const dailyMap = daily.reduce((acc: Record<string, OhlcvRecord>, cur: OhlcvRecord) => {
    (acc as Record<string, OhlcvRecord>)[cur.datetime.slice(0, 10) as string] = cur;
    return acc;
  }, {});
  const grouped: Record<string, { hasBuy: boolean; hasSell: boolean }> = {};
  dailyOrders.forEach((o) => {
    const d = o.Timestamp ? new Date(o.Timestamp).toISOString().slice(0, 10) : "";
    if (!d) return;
    grouped[d] = grouped[d] || { hasBuy: false, hasSell: false };
    if ((o.Side || "").toLowerCase() === "buy") grouped[d].hasBuy = true;
    if ((o.Side || "").toLowerCase() === "sell") grouped[d].hasSell = true;
  });
  Object.entries(grouped).forEach(([d, flags]) => {
    const rec = dailyMap[d];
    if (!rec) return;
    if (flags.hasBuy) {
      buyMarks.x.push(rec.datetime);
      buyMarks.y.push(rec.low - 0.01); // 放在低点下方一点
    }
    if (flags.hasSell) {
      sellMarks.x.push(rec.datetime);
      sellMarks.y.push(rec.high + 0.01); // 放在高点上方一点
    }
  });

  const dailyFig = {
    data: [
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
    ],
    layout: {
      title: "日线 (双击查看当日分钟线)",
      dragmode: "pan",
      hovermode: "x unified",
      spikemode: "across",
      height: 500,
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
      },
      plot_bgcolor: "#0f172a",
      paper_bgcolor: "#0f172a",
      font: { color: "#e2e8f0" }
    },
    config: { responsive: true, scrollZoom: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] },
    useResizeHandler: true,
    style: { width: "100%", height: "100%" }
  };

  const buyOrders = orders.filter((o) => o.Side === "Buy" || o.Side === "BUY");
  const sellOrders = orders.filter((o) => o.Side === "Sell" || o.Side === "SELL");

  const minuteFig = {
    data: [
      {
        x: minute.map((d) => d.datetime),
        y: minute.map((d) => d.close),
        type: "scatter",
        mode: "lines",
        name: "分钟Close",
        line: { color: "#60a5fa" }
      },
      {
        x: buyOrders.map((o) => o.Timestamp),
        y: buyOrders.map((o) => o.Price),
        mode: "markers",
        name: "买入",
        marker: { color: "red", symbol: "triangle-up", size: 10 }
      },
      {
        x: sellOrders.map((o) => o.Timestamp),
        y: sellOrders.map((o) => o.Price),
        mode: "markers",
        name: "卖出",
        marker: { color: "green", symbol: "triangle-down", size: 10 }
      }
    ],
    layout: {
      title: selectedDate ? `分钟线 - ${selectedDate}` : "分钟线",
      height: 520,
      xaxis: {
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
      plot_bgcolor: "#0f172a",
      paper_bgcolor: "#0f172a",
      font: { color: "#e2e8f0" }
    },
    config: { responsive: true, scrollZoom: true, displaylogo: false, modeBarButtonsToRemove: ["lasso2d", "select2d"] },
    useResizeHandler: true,
    style: { width: "100%", height: "100%" }
  };

  return (
    <div className="card">
      {viewMode === "daily" && (
        <Plot
          {...dailyFig}
          onDoubleClick={(ev) => {
            if (ev && ev.points && ev.points.length > 0) {
              const pt = ev.points[0];
              if (pt && pt.x) {
                onOpenDay(toDateStr(pt.x as string));
              }
            }
          }}
        />
      )}
      {viewMode === "intraday" && (
        <>
          <div style={{ marginBottom: 8, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <button onClick={onBack}>返回日线</button>
            <div style={{ opacity: 0.8 }}>当前日期：{selectedDate}</div>
          </div>
          <Plot {...minuteFig} />
          <div style={{ marginTop: 12 }}>
            <table className="table">
              <thead>
                <tr>
                  <th>时间</th>
                  <th>方向</th>
                  <th>价格</th>
                  <th>数量</th>
                  <th>手续费</th>
                </tr>
              </thead>
              <tbody>
                {orders.map((o, idx) => (
                  <tr key={idx}>
                    <td>{o.Timestamp}</td>
                    <td>{o.Side || "-"}</td>
                    <td>{o.Price}</td>
                    <td>{o.Size}</td>
                    <td>{o.Fees ?? "-"}</td>
                  </tr>
                ))}
                {orders.length === 0 && (
                  <tr>
                    <td colSpan={5} style={{ textAlign: "center" }}>
                      暂无成交
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
};

export default Charts;

