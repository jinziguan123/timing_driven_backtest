import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import Plot from "react-plotly.js";
import { fetchMinute } from "../api";
import { OrderRecord } from "../types";

interface Props {
  resultId: string;
  symbol: string;
  date: string;
  orders: OrderRecord[];
}

export default function MinuteChart({ resultId, symbol, date, orders }: Props) {
  const [revision, setRevision] = useState(0);

  const { data: minuteData, isLoading } = useQuery({
    queryKey: ["minute", resultId, symbol, date],
    queryFn: () => fetchMinute(resultId, symbol, date),
  });

  const traces = useMemo(() => {
    if (!minuteData) return [];

    const dates = minuteData.map((d) => d.datetime);
    
    // Candlestick (or Line if minute data is simple close? Usually minute is OHLC)
    // Assuming OHLC
    const candleTrace: any = {
      x: dates,
      open: minuteData.map((d) => d.open),
      high: minuteData.map((d) => d.high),
      low: minuteData.map((d) => d.low),
      close: minuteData.map((d) => d.close),
      type: "candlestick",
      name: "Price",
      increasing: { line: { color: "#ef4444" } },
      decreasing: { line: { color: "#22c55e" } },
    };

    // Markers
    const markerX: string[] = [];
    const markerY: number[] = [];
    const markerSymbol: string[] = [];
    const markerColor: string[] = [];
    const markerText: string[] = [];

    orders.forEach((o) => {
      // Order timestamp: "2023-01-01 09:30:00"
      markerX.push(o.Timestamp);
      markerY.push(o.Price);
      
      if (o.Side === "Buy") {
        markerSymbol.push("triangle-up");
        markerColor.push("#ef4444");
        markerText.push(`Buy ${o.Size} @ ${o.Price}`);
      } else {
        markerSymbol.push("triangle-down");
        markerColor.push("#22c55e");
        markerText.push(`Sell ${Math.abs(o.Size)} @ ${o.Price}`);
      }
    });

    const markerTrace: any = {
      x: markerX,
      y: markerY,
      mode: "markers",
      type: "scatter",
      marker: {
        symbol: markerSymbol,
        color: markerColor,
        size: 10,
        line: { width: 1, color: "#fff" },
      },
      text: markerText,
      hoverinfo: "text+x+y",
      name: "Trades",
    };

    // 只返回蜡烛图和买卖标记，不包含持仓数量和收益预览
    return [candleTrace, markerTrace];
  }, [minuteData, orders]);

  if (isLoading) return <div className="loading-card">加载分钟线...</div>;

  const handleReset = () => {
    setRevision(r => r + 1);
  };

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <button
        onClick={handleReset}
        style={{
          position: 'absolute',
          top: 8,
          right: 8,
          zIndex: 10,
          background: 'rgba(51, 65, 85, 0.8)', // slate-700 with opacity
          color: '#e2e8f0',
          border: '1px solid #475569',
          borderRadius: 4,
          padding: '4px 8px',
          fontSize: 12,
          cursor: 'pointer',
        }}
      >
        重置图表
      </button>
      <Plot
        data={traces}
        layout={{
          autosize: true,
          dragmode: "pan",
          margin: { t: 30, r: 50, l: 50, b: 30 },
          paper_bgcolor: "transparent",
          plot_bgcolor: "transparent",
          font: { color: "#e2e8f0" },
          xaxis: {
            gridcolor: "#334155",
            rangeslider: { visible: false },
            type: "date",
            tickformat: "%Y-%m-%d %H:%M",
            rangebreaks: [
              { bounds: [11.5, 13], pattern: "hour" }
            ],
            autorange: true,
          },
          yaxis: {
            gridcolor: "#334155",
            autorange: true,
            title: "价格"
          },
          showlegend: true,
          legend: { 
            orientation: "h", 
            yanchor: "bottom", 
            y: 1.02, 
            xanchor: "right", 
            x: 1 
          },
          hovermode: "x unified",
          uirevision: revision,
        }}
        config={{
          scrollZoom: true,
          displayModeBar: false,
          doubleClick: false,
        }}
        useResizeHandler
        style={{ width: "100%", height: "100%" }}
      />
    </div>
  );
}
