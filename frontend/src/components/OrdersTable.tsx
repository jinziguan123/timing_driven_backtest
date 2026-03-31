import { OrderRecord } from "../types";

interface Props {
  orders: OrderRecord[];
  height?: number | string;
}

export default function OrdersTable({ orders, height = "auto" }: Props) {
  if (!orders || orders.length === 0) {
    return (
      <div 
        style={{ 
          padding: 20, 
          textAlign: "center", 
          color: "#94a3b8",
          background: "#1e293b",
          borderRadius: 8,
          border: "1px solid #334155"
        }}
      >
        暂无交易记录
      </div>
    );
  }

  return (
    <div style={{ 
      height: height, 
      overflow: "auto", 
      border: "1px solid #334155", 
      borderRadius: 8,
      background: "#1e293b" 
    }}>
      <table className="table" style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0 }}>
        <thead style={{ position: "sticky", top: 0, zIndex: 10 }}>
          <tr style={{ background: "#0f172a" }}>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "left", color: "#94a3b8", fontWeight: 600 }}>时间</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "left", color: "#94a3b8", fontWeight: 600 }}>标的</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "left", color: "#94a3b8", fontWeight: 600 }}>方向</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "right", color: "#94a3b8", fontWeight: 600 }}>价格</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "right", color: "#94a3b8", fontWeight: 600 }}>数量</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "right", color: "#94a3b8", fontWeight: 600 }}>手续费</th>
            <th style={{ padding: "10px 12px", borderBottom: "1px solid #334155", textAlign: "right", color: "#94a3b8", fontWeight: 600 }}>金额</th>
          </tr>
        </thead>
        <tbody>
          {orders.map((order, idx) => {
            const isBuy = order.Side === "Buy";
            const amount = Math.abs(order.Size * order.Price);
            return (
              <tr 
                key={idx} 
                style={{ 
                  background: idx % 2 === 0 ? "transparent" : "rgba(30, 41, 59, 0.5)",
                  transition: "background 0.2s"
                }}
                onMouseEnter={(e) => e.currentTarget.style.background = "#334155"}
                onMouseLeave={(e) => e.currentTarget.style.background = idx % 2 === 0 ? "transparent" : "rgba(30, 41, 59, 0.5)"}
              >
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", color: "#e2e8f0" }}>
                  {order.Timestamp.replace("T", " ")}
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", color: "#e2e8f0" }}>
                  {order.Column || order.Symbol}
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155" }}>
                  <span style={{ 
                    color: isBuy ? "#ef4444" : "#22c55e",
                    fontWeight: 600,
                    padding: "2px 6px",
                    background: isBuy ? "rgba(239, 68, 68, 0.1)" : "rgba(34, 197, 94, 0.1)",
                    borderRadius: 4,
                    fontSize: "0.85em"
                  }}>
                    {isBuy ? "买入" : "卖出"}
                  </span>
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", textAlign: "right", fontFamily: "monospace" }}>
                  {Number(order.Price).toFixed(2)}
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", textAlign: "right", fontFamily: "monospace" }}>
                  {Math.abs(order.Size)}
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", textAlign: "right", fontFamily: "monospace", color: "#94a3b8" }}>
                  {order.Fees ? Number(order.Fees).toFixed(2) : "0.00"}
                </td>
                <td style={{ padding: "8px 12px", borderBottom: "1px solid #334155", textAlign: "right", fontFamily: "monospace" }}>
                  {amount.toFixed(2)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
