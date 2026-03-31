import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDaily, fetchAllOrders, fetchPositions, fetchEquity } from "../api";
import { DateRangeFilter } from "../types";
import DailyChart from "./DailyChart";
import MinuteChart from "./MinuteChart";
import OrdersTable from "./OrdersTable";
import Plot from "react-plotly.js";

interface Props {
  resultId: string;
  symbol: string;
  dateRange?: DateRangeFilter;
}

export default function StockAnalysis({ resultId, symbol, dateRange }: Props) {
  const [selectedDate, setSelectedDate] = useState<string | null>(null);

  const { data: dailyData } = useQuery({
    queryKey: ["daily", resultId, symbol, dateRange],
    queryFn: () => fetchDaily(resultId, symbol, dateRange),
  });

  const { data: orders } = useQuery({
    queryKey: ["orders", resultId, symbol, dateRange],
    queryFn: () => fetchAllOrders(resultId, symbol, dateRange),
  });

  const { data: positions } = useQuery({
    queryKey: ["positions", resultId, symbol, dateRange],
    queryFn: () => fetchPositions(resultId, symbol, dateRange),
  });

  // 使用 fetchEquity 获取每日收盘权益 (value)
  const { data: equityData } = useQuery({
    queryKey: ["equity", resultId, symbol, dateRange],
    queryFn: () => fetchEquity(resultId, symbol, dateRange),
  });

  if (selectedDate) {
    const dayOrders = orders?.filter((o) => o.Timestamp.startsWith(selectedDate)) || [];
    
    // Calculate stats for minute view
    const buyOrders = dayOrders.filter(o => o.Size > 0);
    const sellOrders = dayOrders.filter(o => o.Size < 0);
    const totalVol = dayOrders.reduce((acc, cur) => acc + Math.abs(cur.Size), 0);
    const totalAmount = dayOrders.reduce((acc, cur) => acc + Math.abs(cur.Size * cur.Price), 0);

    return (
      <div className="flex-col" style={{ display: 'flex', flexDirection: 'column', gap: 16, height: '100%' }}>
        <div className="flex" style={{ alignItems: "center", gap: 12 }}>
          <button onClick={() => setSelectedDate(null)} style={{ padding: "6px 12px", fontSize: 14 }}>← 返回日线视图</button>
          <h3 style={{ margin: 0 }}>{selectedDate} 分钟线详情</h3>
        </div>
        
        <div style={{ flex: 1, minHeight: 400, background: '#1e293b', borderRadius: 12, padding: 12, display: 'flex', flexDirection: 'column' }}>
          <div style={{ flex: 1, minHeight: 0 }}>
            <MinuteChart
              resultId={resultId}
              symbol={symbol}
              date={selectedDate}
              orders={dayOrders}
            />
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, maxHeight: 400, flexShrink: 0 }}>
           <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: "0 4px" }}>
             <h4 style={{ margin: 0 }}>当日交易记录</h4>
             <div style={{ fontSize: 12, color: '#94a3b8', display: 'flex', gap: 16 }}>
               <span>共 {dayOrders.length} 笔</span>
               <span style={{ color: '#ef4444' }}>买 {buyOrders.length}</span>
               <span style={{ color: '#22c55e' }}>卖 {sellOrders.length}</span>
               <span>总成交量 {totalVol}</span>
               <span>总成交额 {totalAmount.toFixed(2)}</span>
             </div>
           </div>
           <OrdersTable orders={dayOrders} height={300} />
        </div>
      </div>
    );
  }

  return (
    <div className="flex-col" style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
      <div style={{ height: 500, background: '#1e293b', borderRadius: 12, padding: 12, display: 'flex', flexDirection: 'column' }}>
        <h4 style={{ marginTop: 0, marginBottom: 8 }}>日K线图 <span style={{ fontSize: 12, fontWeight: 'normal', color: '#94a3b8' }}>(双击查看分钟线)</span></h4>
        <div style={{ flex: 1, minHeight: 0 }}>
          {dailyData && orders && (
            <DailyChart
              data={dailyData}
              orders={orders}
              onSelectDate={setSelectedDate}
            />
          )}
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, height: 350 }}>
        <div style={{ background: '#1e293b', borderRadius: 12, padding: 12, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
           <h4 style={{ marginTop: 0, marginBottom: 8 }}>持仓数量（万股）</h4>
           <div style={{ flex: 1, minHeight: 0 }}>
             {positions && (
               <SimpleLineChart
                 data={positions}
                 xField="datetime"
                 yField="position"
                 yScale={1 / 10000}
                 yAxisTitle="万股"
                 color="#3b82f6"
                 title="持仓"
               />
             )}
           </div>
        </div>
        <div style={{ background: '#1e293b', borderRadius: 12, padding: 12, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
           <h4 style={{ marginTop: 0, marginBottom: 8 }}>收益预览（总权益/万元）</h4>
           <div style={{ flex: 1, minHeight: 0 }}>
             {equityData && (
               <SimpleLineChart
                 data={equityData}
                 xField="datetime"
                 yField="value"
                 yScale={1 / 10000}
                 yAxisTitle="万元"
                 color="#f59e0b"
                 title="权益"
               />
             )}
           </div>
        </div>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', marginTop: 0 }}>
        <h4 style={{ marginTop: 0, marginBottom: 8 }}>交易记录</h4>
        <div style={{ height: 400 }}>
          {orders && <OrdersTable orders={orders} height="100%" />}
        </div>
      </div>
    </div>
  );
}

// Simple chart component for Position/PnL
function SimpleLineChart({ data, xField, yField, yScale = 1, yAxisTitle, color, title }: any) {
  if (!data || data.length === 0) return <div style={{ color: '#64748b', textAlign: 'center', marginTop: 40 }}>暂无数据</div>;

  return (
    <Plot
      data={[
        {
          x: data.map((d: any) => d[xField]),
          y: data.map((d: any) => (Number(d[yField]) || 0) * yScale),
          type: "scatter",
          mode: "lines",
          line: { color, width: 2 },
          name: title,
          fill: "tozeroy",
          fillcolor: color + "20"
        },
      ]}
      layout={{
        autosize: true,
        margin: { t: 10, r: 10, l: 40, b: 30 },
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
        font: { color: "#e2e8f0" },
        xaxis: {
          gridcolor: "#334155",
          type: "date",
          tickformat: "%Y-%m-%d",
          zeroline: false
        },
        yaxis: {
          gridcolor: "#334155",
          automargin: true,
          zeroline: false,
          title: yAxisTitle ? { text: yAxisTitle, standoff: 8 } : undefined
        },
        showlegend: false
      }}
      config={{
        displayModeBar: false,
        responsive: true
      }}
      useResizeHandler
      style={{ width: "100%", height: "100%" }}
    />
  );
}
