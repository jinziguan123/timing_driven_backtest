export function formatBacktestTime(timestamp: string): string {
  // Format: YYYYMMDD_HHMMSS
  if (!timestamp || timestamp.length !== 15) return timestamp;
  
  const year = timestamp.substring(0, 4);
  const month = timestamp.substring(4, 6);
  const day = timestamp.substring(6, 8);
  const hour = timestamp.substring(9, 11);
  const minute = timestamp.substring(11, 13);
  const second = timestamp.substring(13, 15);
  
  return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
}

export function parseDateString(dateStr: string): string {
  // Try to standardize date strings for Plotly if needed
  // Most formats like "YYYY-MM-DD HH:mm:ss" or ISO work fine with Plotly
  return dateStr;
}
