import { useMemo, useRef, useState } from "react";
import Plot from "react-plotly.js";
import { OhlcvRecord, OrderRecord } from "../types";

interface Props {
  data: OhlcvRecord[];
  orders: OrderRecord[];
  onSelectDate: (date: string) => void;
}

export default function DailyChart({ data, orders, onSelectDate }: Props) {
  const lastClickRef = useRef<number>(0);
  const [revision, setRevision] = useState(0);

  const { traces, shapes, annotations } = useMemo(() => {
    // 1. Candlestick
    const dates = data.map((d) => d.datetime);
    const candlestickTrace: any = {
      x: dates,
      open: data.map((d) => d.open),
      high: data.map((d) => d.high),
      low: data.map((d) => d.low),
      close: data.map((d) => d.close),
      type: "candlestick",
      name: "Price",
      increasing: { line: { color: "#ef4444" } }, // Red for up (Chinese style)
      decreasing: { line: { color: "#22c55e" } }, // Green for down
    };

    // 2. Markers
    // Group orders by date
    const ordersByDate = new Map<string, OrderRecord[]>();
    orders.forEach((o) => {
      const date = o.Timestamp.split(" ")[0]; // YYYY-MM-DD
      if (!ordersByDate.has(date)) ordersByDate.set(date, []);
      ordersByDate.get(date)?.push(o);
    });

    const markerX: string[] = [];
    const markerY: number[] = [];
    const markerSymbol: string[] = [];
    const markerColor: string[] = [];
    const markerText: string[] = [];

    // Find price for each date to position marker
    const dateToPrice = new Map<string, { high: number; low: number }>();
    data.forEach((d) => {
      dateToPrice.set(d.datetime.split(" ")[0], { high: d.high, low: d.low });
    });

    ordersByDate.forEach((dayOrders, date) => {
        const netQty = dayOrders.reduce((sum, o) => sum + o.Size, 0);
        const hasBuy = dayOrders.some(o => o.Size > 0);
        const hasSell = dayOrders.some(o => o.Size < 0);
        const priceData = dateToPrice.get(date);

        if (!priceData) return;

        let symbol = "circle";
        let color = "yellow";
        let y = priceData.high; 
        let text = `Trades: ${dayOrders.length}`;

        if (netQty > 0) {
            symbol = "triangle-up";
            color = "#ef4444"; // Buy Red
            text = `Buy ${netQty}`;
            y = priceData.low * 0.99; // Below candle
        } else if (netQty < 0) {
            symbol = "triangle-down";
            color = "#22c55e"; // Sell Green
            text = `Sell ${Math.abs(netQty)}`;
            y = priceData.high * 1.01; // Above candle
        } else {
            // Net 0 but has trades
            symbol = "diamond";
            color = "#a855f7"; // Purple
            text = "Day Trade (Flat)";
            y = priceData.high * 1.01;
        }

        markerX.push(date);
        markerY.push(y);
        markerSymbol.push(symbol);
        markerColor.push(color);
        markerText.push(text);
    });

    const markerTrace: any = {
        x: markerX,
        y: markerY,
        mode: 'markers',
        type: 'scatter',
        marker: {
            symbol: markerSymbol,
            color: markerColor,
            size: 12,
            line: { width: 1, color: '#fff' }
        },
        text: markerText,
        hoverinfo: 'text+x',
        name: 'Orders'
    };

    return { traces: [candlestickTrace, markerTrace], shapes: [], annotations: [] };
  }, [data, orders]);

  const handleClick = (e: any) => {
    const now = Date.now();
    if (now - lastClickRef.current < 300) {
      // Double click detected
      if (e.points && e.points[0]) {
        const date = e.points[0].x;
        // Check if date includes time, strip it
        onSelectDate(date.split(" ")[0]);
      }
    }
    lastClickRef.current = now;
  };

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
          margin: { t: 30, r: 50, l: 50, b: 50 },
          paper_bgcolor: "transparent",
          plot_bgcolor: "transparent",
          font: { color: "#e2e8f0" },
          xaxis: {
            gridcolor: "#334155",
            rangeslider: { visible: false },
            type: "date",
            tickformat: "%Y-%m-%d",
            autorange: true,
          },
          yaxis: {
            gridcolor: "#334155",
            autorange: true
          },
          showlegend: false,
          uirevision: revision,
        }}
        config={{
          scrollZoom: true,
          displayModeBar: false,
          doubleClick: false,
        }}
        useResizeHandler
        style={{ width: "100%", height: "100%" }}
        onClick={handleClick}
      />
    </div>
  );
}
