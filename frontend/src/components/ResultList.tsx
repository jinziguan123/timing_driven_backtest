import React from "react";
import { BacktestListItem } from "../types";

interface Props {
  data: BacktestListItem[];
  activeId?: string;
  onSelect: (id: BacktestListItem) => void;
}

const ResultList: React.FC<Props> = ({ data, activeId, onSelect }) => {
  return (
    <div className="card" style={{ display: "flex", flexDirection: "column", height: "100%", padding: 0 }}>
      <div style={{ padding: "16px", borderBottom: "1px solid #334155", flexShrink: 0 }}>
        <h3 style={{ margin: 0, fontSize: 18, fontWeight: 600 }}>回测结果列表</h3>
      </div>
      <div className="scroll" style={{ padding: "8px" }}>
        <ul className="list">
          {data.map((item) => (
            <li
              key={item.result_id}
              className={activeId === item.result_id ? "active" : ""}
              onClick={() => onSelect(item)}
            >
              <div className="flex" style={{ marginBottom: "4px" }}>
                <span className="tag">{item.strategy_name}</span>
              </div>
              <div style={{ fontSize: 12, opacity: 0.7, marginBottom: "4px" }}>
                {item.timestamp}
              </div>
              <div style={{ fontSize: 12, opacity: 0.8 }}>
                收益: {item.total_return ?? "N/A"} | 末值: {item.end_value ?? "N/A"}
              </div>
            </li>
          ))}
          {data.length === 0 && (
            <li style={{ textAlign: "center", color: "#94a3b8" }}>
              暂无数据
            </li>
          )}
        </ul>
      </div>
    </div>
  );
};

export default ResultList;

