import React, { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSummary, fetchEquity } from "../api";
import { DateRangeFilter } from "../types";
import SummaryInfo from "./SummaryInfo";
import StockAnalysis from "./StockAnalysis";
import Plot from "react-plotly.js";

interface Props {
  resultId: string;
}

// 时间区间选择组件
interface DateRangePickerProps {
  startDate: string;
  endDate: string;
  minDate?: string;  // 数据的最早日期
  maxDate?: string;  // 数据的最晚日期
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onReset: () => void;
}

function DateRangePicker({ 
  startDate, 
  endDate, 
  minDate, 
  maxDate,
  onStartDateChange, 
  onEndDateChange,
  onReset 
}: DateRangePickerProps) {
  const inputStyle: React.CSSProperties = {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 6,
    padding: "6px 10px",
    color: "#e2e8f0",
    fontSize: 13,
    cursor: "pointer",
  };

  const labelStyle: React.CSSProperties = {
    color: "#94a3b8",
    fontSize: 12,
    marginRight: 6,
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "8px 12px",
        background: "#0f172a",
        borderRadius: 8,
        border: "1px solid #1e293b",
      }}
    >
      <span style={{ color: "#64748b", fontSize: 13, fontWeight: 500 }}>
        时间筛选:
      </span>
      
      <div style={{ display: "flex", alignItems: "center" }}>
        <span style={labelStyle}>从</span>
        <input
          type="date"
          value={startDate}
          min={minDate}
          max={endDate || maxDate}
          onChange={(e) => onStartDateChange(e.target.value)}
          style={inputStyle}
        />
      </div>

      <div style={{ display: "flex", alignItems: "center" }}>
        <span style={labelStyle}>至</span>
        <input
          type="date"
          value={endDate}
          min={startDate || minDate}
          max={maxDate}
          onChange={(e) => onEndDateChange(e.target.value)}
          style={inputStyle}
        />
      </div>

      <button
        onClick={onReset}
        style={{
          padding: "6px 12px",
          background: "#334155",
          border: "1px solid #475569",
          borderRadius: 6,
          color: "#e2e8f0",
          cursor: "pointer",
          fontSize: 12,
          transition: "background 0.2s",
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.background = "#475569";
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = "#334155";
        }}
      >
        重置
      </button>

      {(startDate || endDate) && (
        <span style={{ color: "#f59e0b", fontSize: 11, fontStyle: "italic" }}>
          已启用时间筛选
        </span>
      )}
    </div>
  );
}

// 模糊搜索下拉组件
interface StockSearchSelectProps {
  stockList: string[];
  value: string;
  onChange: (value: string) => void;
}

function StockSearchSelect({ stockList, value, onChange }: StockSearchSelectProps) {
  const [searchText, setSearchText] = useState("");
  const [isOpen, setIsOpen] = useState(false);
  const [highlightIndex, setHighlightIndex] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // 模糊过滤股票列表
  const filteredStocks = useMemo(() => {
    if (!searchText.trim()) return stockList;
    const query = searchText.toLowerCase();
    return stockList.filter(stock => 
      stock.toLowerCase().includes(query)
    );
  }, [stockList, searchText]);

  // 点击外部关闭下拉
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // 键盘导航
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!isOpen) {
      if (e.key === "ArrowDown" || e.key === "Enter") {
        setIsOpen(true);
        e.preventDefault();
      }
      return;
    }

    switch (e.key) {
      case "ArrowDown":
        e.preventDefault();
        setHighlightIndex(prev => 
          prev < filteredStocks.length ? prev + 1 : prev
        );
        break;
      case "ArrowUp":
        e.preventDefault();
        setHighlightIndex(prev => (prev > 0 ? prev - 1 : 0));
        break;
      case "Enter":
        e.preventDefault();
        if (highlightIndex === 0) {
          handleSelect("");
        } else if (filteredStocks[highlightIndex - 1]) {
          handleSelect(filteredStocks[highlightIndex - 1]);
        }
        break;
      case "Escape":
        setIsOpen(false);
        break;
    }
  };

  // 滚动到高亮项
  useEffect(() => {
    if (isOpen && listRef.current) {
      const highlightedItem = listRef.current.children[highlightIndex] as HTMLElement;
      if (highlightedItem) {
        highlightedItem.scrollIntoView({ block: "nearest" });
      }
    }
  }, [highlightIndex, isOpen]);

  // 重置高亮索引
  useEffect(() => {
    setHighlightIndex(0);
  }, [searchText]);

  const handleSelect = (stock: string) => {
    onChange(stock);
    setSearchText("");
    setIsOpen(false);
    inputRef.current?.blur();
  };

  const displayValue = value || "整体统计 (Portfolio)";

  return (
    <div ref={containerRef} style={{ position: "relative", flex: 1, minWidth: 200 }}>
      {/* 当前选中显示 + 输入框 */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          background: "#1e293b",
          border: "1px solid #334155",
          borderRadius: 8,
          padding: "6px 10px",
          cursor: "text",
        }}
        onClick={() => {
          setIsOpen(true);
          inputRef.current?.focus();
        }}
      >
        {!isOpen ? (
          <span style={{ color: value ? "#e2e8f0" : "#94a3b8", flex: 1 }}>
            {displayValue}
          </span>
        ) : (
          <input
            ref={inputRef}
            type="text"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="输入股票代码搜索..."
            autoFocus
            style={{
              flex: 1,
              background: "transparent",
              border: "none",
              outline: "none",
              color: "#e2e8f0",
              fontSize: 14,
            }}
          />
        )}
        {/* 清除按钮 */}
        {value && !isOpen && (
          <span
            onClick={(e) => {
              e.stopPropagation();
              onChange("");
            }}
            style={{
              marginLeft: 8,
              color: "#64748b",
              cursor: "pointer",
              fontSize: 16,
              lineHeight: 1,
            }}
          >
            ✕
          </span>
        )}
        {/* 下拉箭头 */}
        <span style={{ marginLeft: 8, color: "#64748b", fontSize: 10 }}>
          {isOpen ? "▲" : "▼"}
        </span>
      </div>

      {/* 下拉列表 */}
      {isOpen && (
        <div
          ref={listRef}
          style={{
            position: "absolute",
            top: "100%",
            left: 0,
            right: 0,
            marginTop: 4,
            background: "#1e293b",
            border: "1px solid #334155",
            borderRadius: 8,
            maxHeight: 300,
            overflowY: "auto",
            zIndex: 100,
            boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
          }}
        >
          {/* 整体统计选项 */}
          <div
            onClick={() => handleSelect("")}
            style={{
              padding: "10px 12px",
              cursor: "pointer",
              background: highlightIndex === 0 ? "#334155" : "transparent",
              color: !value ? "#f59e0b" : "#94a3b8",
              borderBottom: "1px solid #334155",
              fontWeight: !value ? 600 : 400,
            }}
          >
            整体统计 (Portfolio)
          </div>

          {/* 股票列表 */}
          {filteredStocks.length > 0 ? (
            filteredStocks.map((stock, idx) => (
              <div
                key={stock}
                onClick={() => handleSelect(stock)}
                style={{
                  padding: "8px 12px",
                  cursor: "pointer",
                  background: highlightIndex === idx + 1 ? "#334155" : "transparent",
                  color: value === stock ? "#f59e0b" : "#e2e8f0",
                  fontWeight: value === stock ? 600 : 400,
                }}
              >
                {/* 高亮匹配文字 */}
                {searchText ? highlightMatch(stock, searchText) : stock}
              </div>
            ))
          ) : (
            <div style={{ padding: "12px", color: "#64748b", textAlign: "center" }}>
              未找到匹配的股票
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// 高亮匹配的文字
function highlightMatch(text: string, query: string): React.ReactNode {
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const index = lowerText.indexOf(lowerQuery);
  
  if (index === -1) return text;
  
  return (
    <>
      {text.slice(0, index)}
      <span style={{ color: "#f59e0b", fontWeight: 600 }}>
        {text.slice(index, index + query.length)}
      </span>
      {text.slice(index + query.length)}
    </>
  );
}

export default function Dashboard({ resultId }: Props) {
  const [selectedStock, setSelectedStock] = useState<string>("");
  
  // 时间区间状态
  const [startDate, setStartDate] = useState<string>("");
  const [endDate, setEndDate] = useState<string>("");

  // 构造 DateRangeFilter 对象
  const dateRange: DateRangeFilter | undefined = useMemo(() => {
    if (!startDate && !endDate) return undefined;
    return {
      startDate: startDate || undefined,
      endDate: endDate || undefined,
    };
  }, [startDate, endDate]);

  // 重置时间区间
  const handleResetDateRange = useCallback(() => {
    setStartDate("");
    setEndDate("");
  }, []);

  // 当 resultId 变化时重置时间区间
  useEffect(() => {
    handleResetDateRange();
    setSelectedStock("");
  }, [resultId, handleResetDateRange]);

  // 首先获取不带时间过滤的 summary 以获取数据范围和 stock_list
  const { data: baseSummary } = useQuery({
    queryKey: ["baseSummary", resultId],
    queryFn: () => fetchSummary(resultId, 400, undefined, undefined),
  });

  // summary 会随着 selectedStock 和 dateRange 变化：
  // - selectedStock 为空时返回全局统计（用于先拿到 stock_list）
  // - 选择股票后返回该股票的 stats（用于渲染"基本信息"）
  // - dateRange 变化时会按时间过滤
  const { data: summary, isLoading, error } = useQuery({
    queryKey: ["summary", resultId, selectedStock, dateRange],
    queryFn: () => fetchSummary(resultId, 400, selectedStock || undefined, dateRange),
  });

  // 获取权益曲线 (支持时间区间过滤)
  const { data: equityData } = useQuery({
    queryKey: ["equity", resultId, selectedStock, dateRange],
    queryFn: () => fetchEquity(resultId, selectedStock || undefined, dateRange),
  });

  if (isLoading) return <div className="loading-card">加载数据中...</div>;
  if (error) return <div className="error-card">加载失败</div>;
  if (!summary) return null;

  const stockList = summary.metadata.stock_list || [];
  
  // 从 baseSummary 获取数据的时间范围（用于日期选择器的限制）
  const dataStartTime = baseSummary?.metadata?.start_time;
  const dataEndTime = baseSummary?.metadata?.end_time;
  
  // 转换为日期格式 (YYYY-MM-DD)
  const minDate = dataStartTime ? dataStartTime.split(" ")[0] : undefined;
  const maxDate = dataEndTime ? dataEndTime.split(" ")[0] : undefined;

  // 获取当前标的在列表中的索引（-1 表示整体统计）
  const currentIndex = selectedStock ? stockList.indexOf(selectedStock) : -1;

  // 切换到上一个标的
  const handlePrevStock = () => {
    if (currentIndex === -1) {
      // 当前是整体统计，切换到最后一个股票
      if (stockList.length > 0) {
        setSelectedStock(stockList[stockList.length - 1]);
      }
    } else if (currentIndex === 0) {
      // 当前是第一个股票，切换到整体统计
      setSelectedStock("");
    } else {
      // 切换到上一个股票
      setSelectedStock(stockList[currentIndex - 1]);
    }
  };

  // 切换到下一个标的
  const handleNextStock = () => {
    if (currentIndex === -1) {
      // 当前是整体统计，切换到第一个股票
      if (stockList.length > 0) {
        setSelectedStock(stockList[0]);
      }
    } else if (currentIndex === stockList.length - 1) {
      // 当前是最后一个股票，切换到整体统计
      setSelectedStock("");
    } else {
      // 切换到下一个股票
      setSelectedStock(stockList[currentIndex + 1]);
    }
  };

  return (
    <div className="main-content">
      <SummaryInfo metadata={summary.metadata} stats={summary.stats} />

      <div className="card detail-section flex-col" style={{ display: "flex", flexDirection: "column" }}>
        {/* 时间区间选择器 */}
        <div style={{ marginBottom: 16 }}>
          <DateRangePicker
            startDate={startDate}
            endDate={endDate}
            minDate={minDate}
            maxDate={maxDate}
            onStartDateChange={setStartDate}
            onEndDateChange={setEndDate}
            onReset={handleResetDateRange}
          />
        </div>

        <div
          className="flex"
          style={{ 
            padding: "0 0 12px 0", 
            borderBottom: "1px solid #334155", 
            marginBottom: 12,
            alignItems: "center",
            gap: 12
          }}
        >
          <label style={{ fontWeight: "bold", whiteSpace: "nowrap" }}>选择标的：</label>
          
          {/* 上一标的按钮 */}
          <button
            onClick={handlePrevStock}
            disabled={stockList.length === 0}
            style={{
              padding: "6px 12px",
              background: "#334155",
              border: "1px solid #475569",
              borderRadius: 6,
              color: stockList.length === 0 ? "#64748b" : "#e2e8f0",
              cursor: stockList.length === 0 ? "not-allowed" : "pointer",
              fontSize: 13,
              whiteSpace: "nowrap",
              transition: "background 0.2s",
            }}
            onMouseEnter={(e) => {
              if (stockList.length > 0) {
                e.currentTarget.style.background = "#475569";
              }
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "#334155";
            }}
          >
            ◀ 上一标的
          </button>

          <StockSearchSelect
            stockList={stockList}
            value={selectedStock}
            onChange={setSelectedStock}
          />

          {/* 下一标的按钮 */}
          <button
            onClick={handleNextStock}
            disabled={stockList.length === 0}
            style={{
              padding: "6px 12px",
              background: "#334155",
              border: "1px solid #475569",
              borderRadius: 6,
              color: stockList.length === 0 ? "#64748b" : "#e2e8f0",
              cursor: stockList.length === 0 ? "not-allowed" : "pointer",
              fontSize: 13,
              whiteSpace: "nowrap",
              transition: "background 0.2s",
            }}
            onMouseEnter={(e) => {
              if (stockList.length > 0) {
                e.currentTarget.style.background = "#475569";
              }
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "#334155";
            }}
          >
            下一标的 ▶
          </button>

          <span style={{ color: "#64748b", fontSize: 12, whiteSpace: "nowrap" }}>
            {currentIndex === -1 ? "整体" : `${currentIndex + 1}/${stockList.length}`} · 共 {stockList.length} 只股票
          </span>
        </div>

        {/* 顶部通用权益图表区域 (可选：如果想在个股页面也展示该股的权益曲线，可以放这里) */}
        {/* 这里我们暂时保持你的设计：整体模式下展示组合权益，个股模式下展示个股分析组件 */}
        
        {selectedStock ? (
          <StockAnalysis resultId={resultId} symbol={selectedStock} dateRange={dateRange} />
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
            <div style={{ padding: 12, textAlign: "center", color: "#94a3b8", fontSize: 14 }}>
               下方展示组合每日收盘总权益曲线
            </div>
            
            {equityData && equityData.length > 0 ? (
               <div style={{ height: 400, background: '#1e293b', borderRadius: 12, padding: 12 }}>
                  <h4 style={{ marginTop: 0, marginBottom: 8, color: '#e2e8f0' }}>组合总权益 (Daily Close)</h4>
                  <Plot
                    data={[
                      {
                        x: equityData.map(d => d.datetime),
                        y: equityData.map(d => d.value),
                        type: "scatter",
                        mode: "lines",
                        line: { color: "#f59e0b", width: 2 }, // amber-500
                        fill: "tozeroy",
                        fillcolor: "rgba(245, 158, 11, 0.1)",
                        name: "总权益"
                      }
                    ]}
                    layout={{
                      autosize: true,
                      margin: { t: 10, r: 10, l: 50, b: 30 },
                      paper_bgcolor: "transparent",
                      plot_bgcolor: "transparent",
                      font: { color: "#e2e8f0" },
                      xaxis: {
                        gridcolor: "#334155",
                        type: "date",
                        tickformat: "%Y-%m-%d"
                      },
                      yaxis: {
                        gridcolor: "#334155",
                        automargin: true,
                        title: { text: "资金", standoff: 10 }
                      },
                      hovermode: "x unified",
                      showlegend: false
                    }}
                    config={{ displayModeBar: false, responsive: true }}
                    useResizeHandler
                    style={{ width: "100%", height: "100%" }}
                  />
               </div>
            ) : (
                <div style={{ textAlign: "center", color: "#64748b", padding: 40 }}>
                   暂无权益数据，请重新运行回测以生成。
                </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
