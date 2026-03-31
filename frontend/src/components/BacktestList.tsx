import { useQuery } from "@tanstack/react-query";
import { fetchResults } from "../api";
import { BacktestListItem } from "../types";

import { formatBacktestTime } from "../utils";

interface Props {
  selectedId: string | null;
  onSelect: (id: string) => void;
}

export default function BacktestList({ selectedId, onSelect }: Props) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["backtests"],
    queryFn: fetchResults,
  });

  if (isLoading) return <div style={{ padding: 12 }}>加载中...</div>;
  if (error) return <div style={{ padding: 12, color: "red" }}>加载失败</div>;

  return (
    <ul className="list scroll">
      {data?.map((item: BacktestListItem) => (
        <li
          key={item.result_id}
          className={selectedId === item.result_id ? "active" : ""}
          onClick={() => onSelect(item.result_id)}
        >
          <div style={{ fontWeight: "bold" }}>{item.strategy_name}</div>
          <div style={{ fontSize: "0.85em", opacity: 0.8 }}>
            {formatBacktestTime(item.timestamp)}
          </div>
        </li>
      ))}
    </ul>
  );
}
