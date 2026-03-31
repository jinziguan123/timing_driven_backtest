import { useState } from "react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import BacktestList from "./components/BacktestList";
import Dashboard from "./components/Dashboard";
import "./styles.css";

const queryClient = new QueryClient();

function App() {
  const [selectedResultId, setSelectedResultId] = useState<string | null>(null);

  return (
    <QueryClientProvider client={queryClient}>
      <div className="layout">
        <aside className="sidebar card">
          <h2 style={{ padding: "0 12px" }}>回测列表</h2>
          <BacktestList
            selectedId={selectedResultId}
            onSelect={setSelectedResultId}
          />
        </aside>
        <main className="main-content">
          {selectedResultId ? (
            <Dashboard resultId={selectedResultId} />
          ) : (
            <div className="card loading-card">请在左侧选择回测结果</div>
          )}
        </main>
      </div>
    </QueryClientProvider>
  );
}

export default App;
