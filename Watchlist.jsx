import React, { useState, useEffect, useMemo, useCallback } from "react";
import { ref, get, set, update, remove } from "firebase/database";
import { database } from "./firebase";

// ============================================================================
// 1. CONFIGURATION & FORMATTING HELPERS
// ============================================================================
export const APP_CONFIG = {
  theme: {
    deepMaroon: "#58111A",        
    headerTextColor: "#ffffff",    
    rowYellow: "#fef9c3",          
    rowSky: "#e0f2fe",             
    rowSelectedBg: "#fef08a",      
    fontColor: "#000000",          
    fontWeight: "700",             
    accentCyan: "#06b6d4",         
    accentMagenta: "#ec4899",      
    accentAmber: "#f59e0b",        
    accentGreen: "#10b981",        
    accentRed: "#ef4444",          
    tableOuterBorder: "2px solid #1e3a8a", 
    tableCellBorder: "1px solid #1e3a8a",  
    fontFamily: "system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
  },
  // Strictly the 6 manual curation metadata fields
  columns: [
    "GROUP",
    "REVIEW", 
    "DURATION", 
    "REMARK", 
    "DATE",
    "TICKER"
  ],
  // Fundamental enrichment metrics merged into detailedDb from SCREENER
  enrichmentMetrics: [
    "PCCAP", "PE", "DPE%", "PB", "DPB%", "DY", "PS", "F-score", "G-score", "T-score",
    "YSG", "YPG", "sg-ttm", "pg-1", "sector", "industry"
  ]
};

const sanitizeKey = (key) =>
  String(key || "").trim().replace(/[.#$\[\]\/]/g, "").toUpperCase();

const extractStockCode = (item) => {
  if (!item) return "";
  if (typeof item === "string") return sanitizeKey(item);
  if (typeof item === "object") {
    const code = item.CODE || item.nseCode || item.NSE || item.BSE || item.Name || item.name;
    return sanitizeKey(code);
  }
  return sanitizeKey(String(item));
};

const extractDisplayName = (item, mainMap = {}) => {
  if (!item) return "";
  const code = extractStockCode(item);
  if (typeof item === "object" && (item.Name || item.name)) return String(item.Name || item.name).trim();
  if (mainMap[code] && (mainMap[code].Name || mainMap[code].name)) {
    return String(mainMap[code].Name || mainMap[code].name).trim();
  }
  return code;
};

const formatDateToDDMMYYYY = (dateObj) => {
  const formatter = new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });
  const parts = formatter.formatToParts(dateObj);
  const d = parts.find(p => p.type === 'day').value;
  const m = parts.find(p => p.type === 'month').value;
  const y = parts.find(p => p.type === 'year').value;
  return `${d}-${m}-${y}`;
};

// ============================================================================
// 2. MAIN COMPONENT
// ============================================================================
export default function StockWatchlist() {
  const theme = APP_CONFIG.theme;

  const [activeTab, setActiveTab] = useState("FILTER2"); 
  const [isModify, setIsModify] = useState(false);
  const [showModifyConfirm, setShowModifyConfirm] = useState(false);
  const [isWatchlistExpanded, setIsWatchlistExpanded] = useState(false);
  const [loading, setLoading] = useState(true);

  const [mainData, setMainData] = useState([]);
  const [filter2Codes, setFilter2Codes] = useState([]);
  const [stockDatabase, setStockDatabase] = useState({});
  const [originalDb, setOriginalDb] = useState({});
  const [watchlistCodes, setWatchlistCodes] = useState([]);

  const [watchlistEditSelected, setWatchlistEditSelected] = useState(new Set());
  const [selectedStockCodes, setSelectedStockCodes] = useState(new Set());
  const [appliedFilter, setAppliedFilter] = useState(null);

  // 1. ADD QUEUE STATES
  const [addQueue, setAddQueue] = useState([]);
  const [currentAddIndex, setCurrentAddIndex] = useState(0);
  const [addAnswers, setAddAnswers] = useState({ q1: "", q2: "", q3: "", q4: "" });

  // 2. DELETE QUEUE STATES
  const [deleteQueue, setDeleteQueue] = useState([]);
  const [currentDeleteIndex, setCurrentDeleteIndex] = useState(0);
  const [deleteAnswers, setDeleteAnswers] = useState({ q1: "", q2: "", q3: "", dateInput: "" });

  // 3. UPDATE QUEUE STATES
  const [updateQueue, setUpdateQueue] = useState([]);
  const [currentUpdateIndex, setCurrentUpdateIndex] = useState(0);
  const [updateAnswers, setUpdateAnswers] = useState({ q1: "", q2: "", q3: "" });

  // Modals & UI States
  const [notepadModal, setNotepadModal] = useState({ isOpen: false, stockCode: "", stockName: "", text: "", error: "" });
  const [tickerModal, setTickerModal] = useState({ isOpen: false, stockCode: "", stockName: "", text: "", defaultTicker: "", isManualMode: false });
  const [showHelpModal, setShowHelpModal] = useState(false);
  const [bannerMsg, setBannerMsg] = useState({ text: "", type: "info" });

  const todayFormattedDate = formatDateToDDMMYYYY(new Date());

  // Cloud Fetch Initializer
  const fetchAllCloudData = useCallback(async () => {
    setLoading(true);
    try {
      // 1. Fetch SCREENER (Keyed by CODE)
      const screenerSnap = await get(ref(database, 'SCREENER'));
      let screenerArr = [];
      if (screenerSnap.exists()) {
        const val = screenerSnap.val();
        screenerArr = Array.isArray(val) ? val : Object.values(val);
      }
      setMainData(screenerArr.filter(Boolean));

      // 2. Fetch filters/filter2
      const f2Snap = await get(ref(database, 'filters/filter2'));
      if (f2Snap.exists()) {
        const f2Val = f2Snap.val();
        const f2Arr = Array.isArray(f2Val) ? f2Val : Object.values(f2Val);
        setFilter2Codes(f2Arr.map(extractStockCode).filter(Boolean));
      } else {
        setFilter2Codes([]);
      }

      // 3. Fetch Watchlist & detailedDb (Keyed by CODE)
      const wlSnap = await get(ref(database, 'watchlist'));
      if (wlSnap.exists()) {
        const wlData = wlSnap.val() || {};
        const rawWl = Array.isArray(wlData.watchlist) ? wlData.watchlist : Object.keys(wlData.detailedDb || {});
        const wlArr = rawWl.map(extractStockCode).filter(Boolean);
        const details = wlData.detailedDb || {};
        
        setWatchlistCodes(wlArr);
        setWatchlistEditSelected(new Set(wlArr));
        setStockDatabase(details);
        setOriginalDb(JSON.parse(JSON.stringify(details)));
      } else {
        setWatchlistCodes([]);
        setWatchlistEditSelected(new Set());
        setStockDatabase({});
        setOriginalDb({});
      }

      setLoading(false);
    } catch (err) {
      console.error("Firebase Data Initialization Error:", err);
      setBannerMsg({ text: `Cloud Sync Error: ${err.message}`, type: "error" });
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAllCloudData();
  }, [fetchAllCloudData]);

  // Lookup map indexed by both CODE and Name for rapid reconciliation
  const mainDataMap = useMemo(() => {
    const map = {};
    if (Array.isArray(mainData)) {
      mainData.forEach(item => {
        if (item.CODE) map[sanitizeKey(item.CODE)] = item;
        if (item.Name) map[sanitizeKey(item.Name)] = item;
        if (item.NSE) map[sanitizeKey(item.NSE)] = item;
        if (item.BSE) map[sanitizeKey(item.BSE)] = item;
      });
    }
    return map;
  }, [mainData]);

  const handleFieldChange = (stockCode, fieldKey, value) => {
    const safeCode = sanitizeKey(stockCode);
    setStockDatabase(prev => ({
      ...prev,
      [safeCode]: {
        ...prev[safeCode],
        [fieldKey]: value
      }
    }));
  };

  const displayedStockCodes = useMemo(() => {
    let list = activeTab === "FILTER2" ? filter2Codes : watchlistCodes;
    if (activeTab === "WATCHLIST" && appliedFilter) {
      list = list.filter(code => appliedFilter.has(code));
    }
    return Array.from(new Set(list.map(extractStockCode).filter(Boolean)));
  }, [activeTab, filter2Codes, watchlistCodes, appliedFilter]);

  const selectableDisplayedCodes = useMemo(() => {
    if (activeTab === "FILTER2") {
      return displayedStockCodes.filter(code => !watchlistCodes.includes(code));
    }
    return displayedStockCodes;
  }, [displayedStockCodes, activeTab, watchlistCodes]);

  const handleToggleSelectStock = (stockCode) => {
    setSelectedStockCodes(prev => {
      const next = new Set(prev);
      if (next.has(stockCode)) next.delete(stockCode);
      else next.add(stockCode);
      return next;
    });
  };

  const handleToggleSelectAll = (e) => {
    if (e.target.checked) setSelectedStockCodes(new Set(selectableDisplayedCodes));
    else setSelectedStockCodes(new Set());
  };

  // Dispatches real-time command to master.py
  const dispatchStockEvent = async (action, stockCode, stockName, ticker = "") => {
    try {
      const cmdRef = ref(database, "system_commands/stock_event");
      await set(cmdRef, {
        action: action, // "ADD" or "DELETE"
        stock: stockCode,
        stockName: stockName,
        ticker: ticker,
        timestamp: Date.now()
      });
    } catch (err) {
      console.error("[EVENT] Failed to dispatch stock event:", err);
    }
  };

  // ============================================================================
  // 1. ADD HANDLER (Enriches metrics and uses CODE as Primary Key)
  // ============================================================================
  const startAddProcess = () => {
    const list = Array.from(selectedStockCodes);
    if (list.length === 0) return;
    setAddQueue(list);
    setCurrentAddIndex(0);
    setAddAnswers({ q1: "", q2: "", q3: "", q4: "" });
  };

  const handleOkAddStock = async () => {
    const codeToAdd = addQueue[currentAddIndex];
    const safeStockKey = sanitizeKey(codeToAdd);
    const mainRecord = mainDataMap[safeStockKey] || {};
    const stockDisplayName = mainRecord.Name || safeStockKey;
    const today = formatDateToDDMMYYYY(new Date());
    
    // Resolve Yahoo Finance Ticker
    const ticker = (stockDatabase[safeStockKey]?.TICKER) || (mainRecord.NSE ? `${mainRecord.NSE}.NS` : `${safeStockKey}.NS`);

    // Manual curation metadata fields
    const stockMetadata = {
      GROUP: stockDatabase[safeStockKey]?.GROUP || "GROUP-0",
      REVIEW: stockDatabase[safeStockKey]?.REVIEW || "NR",
      DURATION: stockDatabase[safeStockKey]?.DURATION || "NR",
      REMARK: stockDatabase[safeStockKey]?.REMARK || "",
      DATE: today,
      TICKER: ticker,
      Name: stockDisplayName,
      CODE: safeStockKey
    };

    // Auto-enrich fundamentals from SCREENER into detailedDb
    APP_CONFIG.enrichmentMetrics.forEach(metric => {
      if (mainRecord[metric] !== undefined && mainRecord[metric] !== null) {
        stockMetadata[metric] = mainRecord[metric];
      }
    });

    const nextWatchlist = Array.from(new Set([...watchlistCodes, safeStockKey]));

    // Update local React state
    setWatchlistCodes(nextWatchlist);
    setStockDatabase(prev => ({ ...prev, [safeStockKey]: stockMetadata }));
    setOriginalDb(prev => ({ ...prev, [safeStockKey]: JSON.parse(JSON.stringify(stockMetadata)) }));
    setWatchlistEditSelected(new Set(nextWatchlist));
    setSelectedStockCodes(prev => {
      const next = new Set(prev);
      next.delete(codeToAdd);
      return next;
    });

    try {
      // 1. Save list of CODEs to /watchlist/watchlist
      await set(ref(database, 'watchlist/watchlist'), nextWatchlist);

      // 2. Add metadata + enriched screener fields under /watchlist/detailedDb/<CODE>
      await set(ref(database, `watchlist/detailedDb/${safeStockKey}`), stockMetadata);

      // 3. Add (CODE: ticker) under /stocklist/<CODE>
      await set(ref(database, `stocklist/${safeStockKey}`), ticker);

      // 4. Instruct master.py to backfill 300 historical rows & live row under /stocks/<CODE>
      await dispatchStockEvent("ADD", safeStockKey, stockDisplayName, ticker);

      setBannerMsg({ text: `Successfully added ${stockDisplayName} (${safeStockKey})! Sync dispatched. ✅`, type: "success" });
      setTimeout(() => setBannerMsg({ text: "", type: "info" }), 3500);
    } catch (err) {
      console.error("Firebase Add Sync Error:", err);
      setBannerMsg({ text: `Add Error: ${err.message}`, type: "error" });
    }

    if (currentAddIndex + 1 < addQueue.length) {
      setCurrentAddIndex(prev => prev + 1);
      setAddAnswers({ q1: "", q2: "", q3: "", q4: "" });
    } else {
      setAddQueue([]);
    }
  };

  const handleCancelAddStock = () => {
    if (currentAddIndex + 1 < addQueue.length) {
      setCurrentAddIndex(prev => prev + 1);
      setAddAnswers({ q1: "", q2: "", q3: "", q4: "" });
    } else {
      setAddQueue([]);
    }
  };

  const handleAbortAdd = () => {
    setAddQueue([]);
    setCurrentAddIndex(0);
  };

  // ============================================================================
  // 2. DELETE HANDLER (Cascades across CODE keys)
  // ============================================================================
  const startDeleteProcess = () => {
    const list = Array.from(watchlistEditSelected);
    if (list.length === 0) return;
    setDeleteQueue(list);
    setCurrentDeleteIndex(0);
    setDeleteAnswers({ q1: "", q2: "", q3: "", dateInput: "" });
  };

  const handleOkDeleteStock = async () => {
    const codeToDelete = deleteQueue[currentDeleteIndex];
    const safeStockKey = sanitizeKey(codeToDelete);
    const stockDisplayName = extractDisplayName(safeStockKey, mainDataMap);

    const nextWatchlist = watchlistCodes.filter(c => c !== safeStockKey);

    // Update local state without touching other stocks
    setWatchlistCodes(nextWatchlist);
    setStockDatabase(prev => {
      const copy = { ...prev };
      delete copy[safeStockKey];
      return copy;
    });
    setOriginalDb(prev => {
      const copy = { ...prev };
      delete copy[safeStockKey];
      return copy;
    });
    setWatchlistEditSelected(prev => {
      const next = new Set(prev);
      next.delete(codeToDelete);
      return next;
    });

    try {
      // 1. Delete code from /watchlist/watchlist
      await set(ref(database, 'watchlist/watchlist'), nextWatchlist);

      // 2. Delete metadata under /watchlist/detailedDb/<CODE>
      await remove(ref(database, `watchlist/detailedDb/${safeStockKey}`));

      // 3. Delete from /stocklist/<CODE>
      await remove(ref(database, `stocklist/${safeStockKey}`));

      // 4. Delete directly from /stocks/<CODE> and /param/<CODE>
      await remove(ref(database, `stocks/${safeStockKey}`));
      await remove(ref(database, `param/${safeStockKey}`));

      // 5. Instruct master.py to complete cleanup
      await dispatchStockEvent("DELETE", safeStockKey, stockDisplayName);

      setBannerMsg({ text: `Purged ${stockDisplayName} (${safeStockKey}) cleanly from database. 🗑️`, type: "success" });
      setTimeout(() => setBannerMsg({ text: "", type: "info" }), 3500);
    } catch (err) {
      console.error("Firebase Complete Purge Sync Error:", err);
      setBannerMsg({ text: `Purge Error: ${err.message}`, type: "error" });
    }

    if (currentDeleteIndex + 1 < deleteQueue.length) {
      setCurrentDeleteIndex(prev => prev + 1);
      setDeleteAnswers({ q1: "", q2: "", q3: "", dateInput: "" });
    } else {
      setDeleteQueue([]);
    }
  };

  const handleCancelDeleteStock = () => {
    if (currentDeleteIndex + 1 < deleteQueue.length) {
      setCurrentDeleteIndex(prev => prev + 1);
      setDeleteAnswers({ q1: "", q2: "", q3: "", dateInput: "" });
    } else {
      setDeleteQueue([]);
    }
  };

  const handleAbortDelete = () => {
    setDeleteQueue([]);
    setCurrentDeleteIndex(0);
  };

  // ============================================================================
  // 3. UPDATE HANDLER (Operates on Selected Stocks & Synchronizes Enriched Data)
  // ============================================================================
  const startUpdateProcess = () => {
    // Works strictly on selected stocks within watchlist checkboxes
    const list = Array.from(watchlistEditSelected);
    if (list.length === 0) return;
    setUpdateQueue(list);
    setCurrentUpdateIndex(0);
    setUpdateAnswers({ q1: "", q2: "", q3: "" });
  };

  const handleOkUpdateStock = async () => {
    const codeToUpdate = updateQueue[currentUpdateIndex];
    const safeStockKey = sanitizeKey(codeToUpdate);
    const mainRecord = mainDataMap[safeStockKey] || {};
    const stockDisplayName = extractDisplayName(safeStockKey, mainDataMap);
    const today = formatDateToDDMMYYYY(new Date());

    const curr = stockDatabase[safeStockKey] || {};
    const orig = originalDb[safeStockKey] || {};

    const coreChanged =
      curr.GROUP !== orig.GROUP ||
      curr.DURATION !== orig.DURATION ||
      curr.REVIEW !== orig.REVIEW ||
      curr.REMARK !== orig.REMARK ||
      curr.TICKER !== orig.TICKER;

    const updatedDate = coreChanged ? today : (curr.DATE || today);

    // Build update package preserving manual inputs and refreshed screener enrichment
    const updatedMetadata = {
      ...curr,
      GROUP: curr.GROUP || "GROUP-0",
      REVIEW: curr.REVIEW || "NR",
      DURATION: curr.DURATION || "NR",
      REMARK: curr.REMARK || "",
      DATE: updatedDate,
      TICKER: curr.TICKER || `${safeStockKey}.NS`,
      Name: curr.Name || stockDisplayName,
      CODE: safeStockKey
    };

    // Merge latest Screener metrics
    APP_CONFIG.enrichmentMetrics.forEach(metric => {
      if (mainRecord[metric] !== undefined && mainRecord[metric] !== null) {
        updatedMetadata[metric] = mainRecord[metric];
      }
    });

    setStockDatabase(prev => ({
      ...prev,
      [safeStockKey]: updatedMetadata
    }));
    setOriginalDb(prev => ({
      ...prev,
      [safeStockKey]: JSON.parse(JSON.stringify(updatedMetadata))
    }));

    try {
      // Patch under /watchlist/detailedDb/<CODE>
      await update(ref(database, `watchlist/detailedDb/${safeStockKey}`), updatedMetadata);

      // Keep stocklist mapping synced
      if (updatedMetadata.TICKER) {
        await set(ref(database, `stocklist/${safeStockKey}`), updatedMetadata.TICKER);
      }

      setBannerMsg({ text: `Updated metadata for ${stockDisplayName} (${safeStockKey})! 💾`, type: "success" });
      setTimeout(() => setBannerMsg({ text: "", type: "info" }), 3500);
    } catch (err) {
      console.error("Firebase Update Sync Error:", err);
      setBannerMsg({ text: `Update Error: ${err.message}`, type: "error" });
    }

    if (currentUpdateIndex + 1 < updateQueue.length) {
      setCurrentUpdateIndex(prev => prev + 1);
      setUpdateAnswers({ q1: "", q2: "", q3: "" });
    } else {
      setUpdateQueue([]);
    }
  };

  const handleCancelUpdateStock = () => {
    if (currentUpdateIndex + 1 < updateQueue.length) {
      setCurrentUpdateIndex(prev => prev + 1);
      setUpdateAnswers({ q1: "", q2: "", q3: "" });
    } else {
      setUpdateQueue([]);
    }
  };

  const handleAbortUpdate = () => {
    setUpdateQueue([]);
    setCurrentUpdateIndex(0);
  };

  const handleApply = () => {
    setActiveTab("WATCHLIST");
    setAppliedFilter(new Set(watchlistEditSelected));
  };

  const handleSaveNotepad = () => {
    handleFieldChange(notepadModal.stockCode, "REMARK", notepadModal.text);
    setNotepadModal({ isOpen: false, stockCode: "", stockName: "", text: "", error: "" });
  };

  const openExternalLink = (type, stockCode) => {
    const safeCode = sanitizeKey(stockCode);
    const mainRecord = mainDataMap[safeCode] || {};
    const nse = mainRecord.NSE || (safeCode.endsWith(".NS") ? safeCode.replace(".NS", "") : safeCode);
    const bse = mainRecord.BSE;
    const fallbackCode = mainRecord.CODE || safeCode; 

    let url = "";
    if (type === "SCR") url = `https://www.screener.in/company/${nse || bse || fallbackCode}/consolidated/`;
    else if (type === "TV") url = `https://in.tradingview.com/chart/?symbol=${nse || fallbackCode}`;
    else if (type === "GF") url = nse ? `https://www.google.com/finance/beta/quote/${nse}:NSE` : `https://www.google.com/finance/`;
    else if (type === "YF") url = nse ? `https://finance.yahoo.com/quote/${nse}.NS/` : `https://finance.yahoo.com/lookup/?s=${encodeURIComponent(fallbackCode)}`;

    if (!url) return;
    const width = 1000, height = 700;
    const left = (window.innerWidth - width) / 2 + window.screenX;
    const top = (window.innerHeight - height) / 2 + window.screenY;
    window.open(url, "_blank", `width=${width},height=${height},top=${top},left=${left},resizable=yes,scrollbars=yes`);
  };

  const isAllDisplayedSelected = selectableDisplayedCodes.length > 0 && selectableDisplayedCodes.every(code => selectedStockCodes.has(code));

  if (loading) {
    return (
      <div style={{ color: theme.accentCyan, backgroundColor: "#0b132b", minHeight: "100vh", padding: "40px", textAlign: "center", fontFamily: theme.fontFamily }}>
        <h2>⏳ Loading Watchlist from Firebase Realtime Database...</h2>
      </div>
    );
  }

  return (
    <div style={{ fontFamily: theme.fontFamily, backgroundColor: "#0b132b", minHeight: "100vh", padding: "16px", boxSizing: "border-box" }}>
      
      {/* CONTROL PANEL */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", backgroundColor: "#0f172a", padding: "10px 16px", borderRadius: "8px", border: `2px solid ${theme.deepMaroon}`, marginBottom: "12px", boxShadow: "0 4px 20px rgba(0,0,0,0.5)", flexWrap: "wrap", gap: "10px" }}>
        
        <div style={{ display: "flex", alignItems: "center", gap: "10px", flexWrap: "wrap" }}>
          <button
            onClick={() => { setActiveTab("FILTER2"); setSelectedStockCodes(new Set()); setAppliedFilter(null); }}
            style={{
              backgroundColor: activeTab === "FILTER2" ? theme.accentAmber : "#1e293b",
              color: activeTab === "FILTER2" ? "#000000" : theme.accentAmber,
              border: `2px solid ${theme.accentAmber}`,
              padding: "8px 14px",
              borderRadius: "6px",
              fontWeight: "900",
              fontSize: "12px",
              cursor: "pointer",
              textTransform: "uppercase",
              boxShadow: activeTab === "FILTER2" ? "0 0 10px rgba(245, 158, 11, 0.5)" : "none"
            }}
          >
            FILTER2 ({filter2Codes.length})
          </button>

          <div style={{ display: "flex", alignItems: "center", background: "#1e293b", borderRadius: "6px", border: `2px solid ${theme.accentCyan}`, overflow: "hidden" }}>
            <button
              onClick={() => { setActiveTab("WATCHLIST"); setSelectedStockCodes(new Set()); setAppliedFilter(null); }}
              style={{
                backgroundColor: activeTab === "WATCHLIST" ? theme.accentCyan : "transparent",
                color: activeTab === "WATCHLIST" ? "#000000" : theme.accentCyan,
                border: "none",
                padding: "8px 14px",
                fontWeight: "900",
                fontSize: "12px",
                cursor: "pointer",
                textTransform: "uppercase"
              }}
            >
              WATCHLIST ({watchlistCodes.length})
            </button>
            <button
              onClick={() => setIsWatchlistExpanded(prev => !prev)}
              title="Expand Tools"
              style={{
                backgroundColor: "#334155",
                color: theme.accentCyan,
                border: "none",
                padding: "8px 12px",
                fontWeight: "900",
                fontSize: "14px",
                cursor: "pointer"
              }}
            >
              {isWatchlistExpanded ? "▼" : "+"}
            </button>
          </div>
        </div>

        <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
          <button
            onClick={() => setShowModifyConfirm(true)}
            style={{
              backgroundColor: isModify ? theme.accentRed : "#7c3aed",
              color: "#ffffff",
              border: "none",
              padding: "8px 14px",
              borderRadius: "6px",
              fontWeight: "900",
              fontSize: "12px",
              cursor: "pointer",
              textTransform: "uppercase",
              boxShadow: isModify ? "0 0 10px rgba(239, 68, 68, 0.6)" : "none"
            }}
          >
            MODIFY: {isModify ? "YES" : "NO"}
          </button>

          <button
            onClick={() => setShowHelpModal(true)}
            style={{
              backgroundColor: theme.accentAmber,
              color: "#000000",
              border: "none",
              padding: "8px 14px",
              borderRadius: "6px",
              fontWeight: "900",
              fontSize: "12px",
              cursor: "pointer",
              textTransform: "uppercase"
            }}
          >
            HELP
          </button>
        </div>
      </div>

      {bannerMsg.text && (
        <div style={{ marginBottom: "12px", padding: "10px 14px", backgroundColor: bannerMsg.type === "error" ? "rgba(239, 68, 68, 0.2)" : "rgba(16, 185, 129, 0.2)", border: `2px solid ${bannerMsg.type === "error" ? theme.accentRed : theme.accentGreen}`, color: bannerMsg.type === "error" ? "#fca5a5" : "#6ee7b7", borderRadius: "8px", fontSize: "12px", fontWeight: "bold", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span>{bannerMsg.text}</span>
          <button onClick={() => setBannerMsg({ text: "", type: "info" })} style={{ background: "transparent", border: "none", color: "#fff", cursor: "pointer", fontWeight: "bold" }}>✕</button>
        </div>
      )}

      {/* EXPANSION PANEL */}
      {isWatchlistExpanded && (
        <div style={{ marginBottom: "16px", padding: "16px", backgroundColor: "#0f172a", border: `2px solid ${theme.accentCyan}`, borderRadius: "8px" }}>
          
          <div style={{ color: theme.accentCyan, fontWeight: "900", textTransform: "uppercase", fontSize: "12px", marginBottom: "12px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <span>WATCHLIST CHECKBOXES ({watchlistCodes.length} TOTAL):</span>
            <div style={{ display: "flex", gap: "8px" }}>
              <button onClick={() => setWatchlistEditSelected(new Set(watchlistCodes))} style={{ backgroundColor: "#1e293b", color: theme.accentCyan, border: `1px solid ${theme.accentCyan}`, padding: "4px 10px", borderRadius: "4px", cursor: "pointer", fontSize: "11px", fontWeight: "bold" }}>SELECT ALL</button>
              <button onClick={() => setWatchlistEditSelected(new Set())} style={{ backgroundColor: "#1e293b", color: theme.accentRed, border: `1px solid ${theme.accentRed}`, padding: "4px 10px", borderRadius: "4px", cursor: "pointer", fontSize: "11px", fontWeight: "bold" }}>DESELECT ALL</button>
            </div>
          </div>

          <div style={{ display: "flex", flexWrap: "wrap", gap: "10px", marginBottom: "16px" }}>
            {watchlistCodes.length === 0 ? (
              <p style={{ color: "#94a3b8", fontSize: "12px", fontWeight: "bold" }}>No stocks in Watchlist. Select from Filter2 and Add.</p>
            ) : (
              watchlistCodes.map(code => {
                const isChecked = watchlistEditSelected.has(code);
                const name = extractDisplayName(code, mainDataMap);
                return (
                  <label key={code} style={{ display: "flex", alignItems: "center", gap: "6px", background: isChecked ? "#1e293b" : "#334155", padding: "6px 12px", borderRadius: "6px", border: `1px solid ${isChecked ? theme.accentCyan : "#475569"}`, color: "#ffffff", fontWeight: "bold", fontSize: "12px", cursor: "pointer" }}>
                    <input
                      type="checkbox"
                      checked={isChecked}
                      onChange={(e) => {
                        const next = new Set(watchlistEditSelected);
                        if (e.target.checked) next.add(code);
                        else next.delete(code);
                        setWatchlistEditSelected(next);
                      }}
                      style={{ accentColor: theme.accentCyan }}
                    />
                    {name} ({code})
                  </label>
                );
              })
            )}
          </div>

          <div style={{ display: "flex", gap: "12px", flexWrap: "wrap", alignItems: "center", borderTop: "1px solid #334155", paddingTop: "12px" }}>
            <button 
              onClick={startAddProcess} 
              disabled={selectedStockCodes.size === 0} 
              style={{ backgroundColor: theme.accentGreen, color: "#000000", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "900", fontSize: "12px", cursor: selectedStockCodes.size === 0 ? "not-allowed" : "pointer", opacity: selectedStockCodes.size === 0 ? 0.5 : 1 }}
            >
              ➕ ADD TO WATCHLIST ({selectedStockCodes.size})
            </button>
            
            <button 
              onClick={startDeleteProcess} 
              disabled={watchlistEditSelected.size === 0} 
              style={{ backgroundColor: theme.accentRed, color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "900", fontSize: "12px", cursor: watchlistEditSelected.size === 0 ? "not-allowed" : "pointer", opacity: watchlistEditSelected.size === 0 ? 0.5 : 1 }}
            >
              🗑️ DELETE CHECKED ({watchlistEditSelected.size})
            </button>
            
            <div style={{ width: "2px", height: "24px", backgroundColor: "#334155", margin: "0 5px" }} />
            
            <button onClick={handleApply} disabled={watchlistEditSelected.size === 0} style={{ backgroundColor: theme.accentAmber, color: "#000000", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "900", fontSize: "12px", cursor: watchlistEditSelected.size === 0 ? "not-allowed" : "pointer", opacity: watchlistEditSelected.size === 0 ? 0.5 : 1 }}>
              ✔️ APPLY ({watchlistEditSelected.size})
            </button>

            <button 
              onClick={startUpdateProcess} 
              disabled={watchlistEditSelected.size === 0}
              style={{ 
                backgroundColor: theme.accentCyan, 
                color: "#000000", 
                border: "none", 
                padding: "8px 16px", 
                borderRadius: "6px", 
                fontWeight: "900", 
                fontSize: "12px", 
                marginLeft: "auto", 
                cursor: watchlistEditSelected.size === 0 ? "not-allowed" : "pointer", 
                opacity: watchlistEditSelected.size === 0 ? 0.5 : 1,
                boxShadow: "0 0 10px rgba(6, 182, 212, 0.5)",
                transition: "all 0.2s ease-in-out"
              }}
            >
              💾 UPDATE DATABASE ({watchlistEditSelected.size})
            </button>
          </div>
        </div>
      )}

      {/* SPREADSHEET TABLE */}
      <div style={{ overflowX: "auto", overflowY: "auto", maxHeight: "calc(100vh - 240px)", border: isModify ? "2px solid #ef4444" : theme.tableOuterBorder, borderRadius: "8px", backgroundColor: "#ffffff", boxShadow: "0 10px 30px rgba(0,0,0,0.5)" }}>
        <table style={{ width: "100%", borderCollapse: "separate", borderSpacing: 0, fontSize: "13px", fontWeight: theme.fontWeight, color: theme.fontColor }}>
          <thead>
            <tr style={{ position: "sticky", top: 0, zIndex: 30, backgroundColor: theme.deepMaroon, color: theme.headerTextColor }}>
              <th style={{ padding: "12px 8px", border: theme.tableCellBorder, textAlign: "center", width: "50px", backgroundColor: theme.deepMaroon, position: "sticky", left: 0, zIndex: 40, textTransform: "uppercase" }}>
                <input type="checkbox" checked={isAllDisplayedSelected} onChange={handleToggleSelectAll} style={{ width: "16px", height: "16px", cursor: "pointer", accentColor: theme.accentAmber }} />
              </th>
              <th style={{ padding: "12px 16px", border: theme.tableCellBorder, textAlign: "left", backgroundColor: theme.deepMaroon, position: "sticky", left: "50px", zIndex: 40, textTransform: "uppercase", whiteSpace: "nowrap" }}>
                STOCK NAME
              </th>
              {APP_CONFIG.columns.map((colKey) => (
                <th key={colKey} style={{ padding: "12px 14px", border: theme.tableCellBorder, textAlign: "center", backgroundColor: theme.deepMaroon, color: theme.headerTextColor, textTransform: "uppercase", whiteSpace: "nowrap" }}>
                  {colKey}
                </th>
              ))}
              <th style={{ padding: "12px 14px", border: theme.tableCellBorder, textAlign: "center", backgroundColor: theme.deepMaroon, color: theme.headerTextColor, textTransform: "uppercase", whiteSpace: "nowrap" }}>
                LINK BUTTONS
              </th>
            </tr>
          </thead>
          <tbody>
            {displayedStockCodes.length === 0 ? (
              <tr>
                <td colSpan={9} style={{ textAlign: "center", padding: "40px", color: "#64748b", fontWeight: "bold" }}>
                  No stocks found in {activeTab}.
                </td>
              </tr>
            ) : (
              displayedStockCodes.map((code, index) => {
                const stockData = stockDatabase[code] || {};
                const stockDisplayName = extractDisplayName(code, mainDataMap);
                const isEven = index % 2 === 0;
                const rowBg = isEven ? theme.rowYellow : theme.rowSky;
                const isSelected = selectedStockCodes.has(code);
                const isEditable = isModify || activeTab === "WATCHLIST";
                const isAlreadyInWatchlist = activeTab === "FILTER2" && watchlistCodes.includes(code);

                return (
                  <tr key={code} style={{ backgroundColor: isSelected ? theme.rowSelectedBg : rowBg }}>
                    <td style={{ padding: "8px", border: theme.tableCellBorder, textAlign: "center", backgroundColor: isSelected ? theme.rowSelectedBg : rowBg, position: "sticky", left: 0, zIndex: 10 }}>
                      <input 
                        type="checkbox" 
                        checked={isAlreadyInWatchlist ? false : isSelected} 
                        disabled={isAlreadyInWatchlist}
                        onChange={() => handleToggleSelectStock(code)} 
                        style={{ width: "18px", height: "18px", cursor: isAlreadyInWatchlist ? "not-allowed" : "pointer", accentColor: theme.accentAmber, opacity: isAlreadyInWatchlist ? 0.75 : 1 }} 
                        title={isAlreadyInWatchlist ? "Already in Watchlist" : "Select to Add"}
                      />
                    </td>

                    <td style={{ padding: "8px 16px", border: theme.tableCellBorder, textAlign: "left", backgroundColor: isSelected ? theme.rowSelectedBg : rowBg, position: "sticky", left: "50px", zIndex: 10, fontWeight: "900", whiteSpace: "nowrap", color: "#000000" }}>
                      {stockDisplayName}
                      {isAlreadyInWatchlist && (
                        <span 
                          style={{ marginLeft: "8px", display: "inline-flex", alignItems: "center", justifyContent: "center", width: "18px", height: "18px", borderRadius: "50%", backgroundColor: theme.accentRed, color: "#ffffff", fontSize: "10px", fontWeight: "900", verticalAlign: "middle", marginBottom: "2px" }} 
                          title="Already in Watchlist"
                        >
                          W
                        </span>
                      )}
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <input
                        type="text"
                        maxLength={20}
                        disabled={!isEditable}
                        value={stockData["GROUP"] ?? "GROUP-0"}
                        onChange={(e) => handleFieldChange(code, "GROUP", e.target.value)}
                        style={{ width: "100%", padding: "6px", backgroundColor: isEditable ? "#0f172a" : "#cbd5e1", color: isEditable ? theme.accentAmber : "#000000", fontWeight: "900", borderRadius: "4px", border: "1px solid #334155", cursor: isEditable ? "text" : "not-allowed", textAlign: "center", fontSize: "12px", boxSizing: "border-box" }}
                      />
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <select
                        disabled={!isEditable}
                        value={stockData["REVIEW"] || "NR"}
                        onChange={(e) => handleFieldChange(code, "REVIEW", e.target.value)}
                        style={{ width: "100%", padding: "6px", backgroundColor: isEditable ? "#0f172a" : "#cbd5e1", color: isEditable ? theme.accentMagenta : "#000000", fontWeight: "900", borderRadius: "4px", border: "1px solid #334155", cursor: isEditable ? "pointer" : "not-allowed", textAlign: "center" }}
                      >
                        {["NR", "1 STAR", "2 STAR", "3 STAR", "4 STAR", "5 STAR"].map(opt => (
                          <option key={opt} value={opt}>{opt}</option>
                        ))}
                      </select>
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <select
                        disabled={!isEditable}
                        value={stockData["DURATION"] || "NR"}
                        onChange={(e) => handleFieldChange(code, "DURATION", e.target.value)}
                        style={{ width: "100%", padding: "6px", backgroundColor: isEditable ? "#0f172a" : "#cbd5e1", color: isEditable ? theme.accentCyan : "#000000", fontWeight: "900", borderRadius: "4px", border: "1px solid #334155", cursor: isEditable ? "pointer" : "not-allowed", textAlign: "center" }}
                      >
                        {["V. Long (3-10 Years)", "Long (1-3 Years)", "Medium (6-12 Month)", "Short (3-6 Month)", "V. Short (0-3 Month)", "NR"].map(opt => (
                          <option key={opt} value={opt}>{opt}</option>
                        ))}
                      </select>
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <button
                        type="button"
                        onClick={() => {
                          if (!isEditable) { alert("Enable MODIFY mode to edit remarks!"); return; }
                          setNotepadModal({ isOpen: true, stockCode: code, stockName: stockDisplayName, text: stockData["REMARK"] || "", error: "" });
                        }}
                        style={{ backgroundColor: stockData["REMARK"] ? theme.accentGreen : theme.accentAmber, color: "#000000", border: "none", padding: "6px 12px", borderRadius: "4px", fontWeight: "900", cursor: "pointer", display: "inline-flex", alignItems: "center", gap: "4px", fontSize: "12px" }}
                      >
                        ✏️ {stockData["REMARK"] ? "EDIT" : "ADD"}
                      </button>
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center", color: "#16a34a", fontWeight: "900", whiteSpace: "nowrap" }}>
                      {stockData["DATE"] || ""}
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <button
                        type="button"
                        onClick={() => {
                          if (!isEditable) { alert("Enable MODIFY mode to edit ticker!"); return; }
                          const mainRecord = mainDataMap[code] || {};
                          const defaultTicker = mainRecord.NSE ? `${mainRecord.NSE}.NS` : `${code}.NS`;
                          setTickerModal({ isOpen: true, stockCode: code, stockName: stockDisplayName, text: stockData["TICKER"] || defaultTicker, defaultTicker: defaultTicker, isManualMode: false });
                        }}
                        style={{ 
                          backgroundColor: isEditable ? "#0f172a" : "#cbd5e1", 
                          color: isEditable ? theme.accentAmber : "#000000", 
                          border: `1px solid ${isEditable ? "#334155" : "#cbd5e1"}`, 
                          padding: "6px", 
                          borderRadius: "4px", 
                          fontWeight: "900", 
                          cursor: isEditable ? "pointer" : "not-allowed", 
                          fontSize: "12px", 
                          width: "120px",
                          boxSizing: "border-box",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap"
                        }}
                      >
                         {stockData["TICKER"] || "SET TICKER"}
                      </button>
                    </td>

                    <td style={{ padding: "6px 10px", border: theme.tableCellBorder, textAlign: "center" }}>
                      <div style={{ display: "flex", justifyContent: "center", gap: "4px" }}>
                        <button type="button" onClick={() => openExternalLink("SCR", code)} style={{ backgroundColor: "#2563eb", color: "#fff", border: "none", padding: "4px 8px", borderRadius: "4px", fontWeight: "bold", fontSize: "11px", cursor: "pointer" }}>SCR</button>
                        <button type="button" onClick={() => openExternalLink("TV", code)} style={{ backgroundColor: "#ea580c", color: "#fff", border: "none", padding: "4px 8px", borderRadius: "4px", fontWeight: "bold", fontSize: "11px", cursor: "pointer" }}>TV</button>
                        <button type="button" onClick={() => openExternalLink("GF", code)} style={{ backgroundColor: "#16a34a", color: "#fff", border: "none", padding: "4px 8px", borderRadius: "4px", fontWeight: "bold", fontSize: "11px", cursor: "pointer" }}>GF</button>
                        <button type="button" onClick={() => openExternalLink("YF", code)} style={{ backgroundColor: "#9333ea", color: "#fff", border: "none", padding: "4px 8px", borderRadius: "4px", fontWeight: "bold", fontSize: "11px", cursor: "pointer" }}>YF</button>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {/* 1. ADD CONFIRMATION MODAL */}
      {addQueue.length > 0 && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.85)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `2px solid ${theme.accentGreen}`, borderRadius: "12px", padding: "24px", width: "480px", color: "#f8fafc", boxShadow: "0 10px 40px rgba(0,0,0,0.8)" }}>
            
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #334155", paddingBottom: "10px", marginBottom: "16px" }}>
              <span style={{ fontSize: "12px", fontWeight: "bold", color: "#94a3b8" }}>
                ADDING ITEM {currentAddIndex + 1} OF {addQueue.length}
              </span>
              <span style={{ fontSize: "11px", backgroundColor: "#1e293b", padding: "3px 8px", borderRadius: "4px", border: `1px solid ${theme.accentGreen}`, color: theme.accentGreen, fontWeight: "900" }}>
                SECURITY CHECK
              </span>
            </div>

            <div style={{ textAlign: "center", marginBottom: "20px" }}>
              <h1 style={{ color: theme.accentGreen, fontSize: "28px", fontWeight: "900", margin: "0 0 6px 0", letterSpacing: "1px" }}>
                {extractDisplayName(addQueue[currentAddIndex], mainDataMap)}
              </h1>
              <p style={{ margin: 0, fontSize: "12px", color: "#cbd5e1" }}>Primary Key CODE: <b>{addQueue[currentAddIndex]}</b></p>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "12px", fontSize: "13px", marginBottom: "24px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>A. Are you sure you want to add this stock?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setAddAnswers({ ...addAnswers, q1: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: addAnswers.q1 === opt ? theme.accentGreen : "#334155", color: addAnswers.q1 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>B. Do you add this stock without any purpose?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setAddAnswers({ ...addAnswers, q2: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: addAnswers.q2 === opt ? theme.accentGreen : "#334155", color: addAnswers.q2 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>C. Have you completed your research on this stock?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setAddAnswers({ ...addAnswers, q3: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: addAnswers.q3 === opt ? theme.accentGreen : "#334155", color: addAnswers.q3 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>D. Have you filled-up all the manual entries?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setAddAnswers({ ...addAnswers, q4: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: addAnswers.q4 === opt ? theme.accentGreen : "#334155", color: addAnswers.q4 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", borderTop: "1px solid #334155", paddingTop: "14px" }}>
              <button onClick={handleAbortAdd} style={{ backgroundColor: "#334155", color: "#f87171", border: "1px solid #ef4444", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                ABORT
              </button>
              <button onClick={handleCancelAddStock} style={{ backgroundColor: "#475569", color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                CANCEL
              </button>

              {addAnswers.q1 === "Y" && addAnswers.q2 === "N" && addAnswers.q3 === "Y" && addAnswers.q4 === "Y" && (
                <button onClick={handleOkAddStock} style={{ backgroundColor: theme.accentGreen, color: "#000000", border: "none", padding: "8px 24px", borderRadius: "6px", fontWeight: "900", cursor: "pointer" }}>
                  OK
                </button>
              )}
            </div>

          </div>
        </div>
      )}

      {/* 2. DELETE CONFIRMATION MODAL */}
      {deleteQueue.length > 0 && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.85)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `2px solid ${theme.accentRed}`, borderRadius: "12px", padding: "24px", width: "480px", color: "#f8fafc", boxShadow: "0 10px 40px rgba(0,0,0,0.8)" }}>
            
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #334155", paddingBottom: "10px", marginBottom: "16px" }}>
              <span style={{ fontSize: "12px", fontWeight: "bold", color: "#94a3b8" }}>
                DELETING ITEM {currentDeleteIndex + 1} OF {deleteQueue.length}
              </span>
              <span style={{ fontSize: "11px", backgroundColor: "#1e293b", padding: "3px 8px", borderRadius: "4px", border: `1px solid ${theme.accentRed}`, color: theme.accentRed, fontWeight: "900" }}>
                DELETION AUDIT
              </span>
            </div>

            <div style={{ textAlign: "center", marginBottom: "20px" }}>
              <h1 style={{ color: theme.accentRed, fontSize: "28px", fontWeight: "900", margin: "0 0 6px 0", letterSpacing: "1px" }}>
                {extractDisplayName(deleteQueue[currentDeleteIndex], mainDataMap)}
              </h1>
              <p style={{ margin: 0, fontSize: "12px", color: "#cbd5e1" }}>Purging Key: <b>{deleteQueue[currentDeleteIndex]}</b></p>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "12px", fontSize: "13px", marginBottom: "24px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>A. Are you sure you want to delete this stock?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setDeleteAnswers({ ...deleteAnswers, q1: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: deleteAnswers.q1 === opt ? theme.accentRed : "#334155", color: "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>B. By mistake are you not deleting this stock?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setDeleteAnswers({ ...deleteAnswers, q2: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: deleteAnswers.q2 === opt ? theme.accentRed : "#334155", color: "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>C. Do you know deleting this stock will erase history?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setDeleteAnswers({ ...deleteAnswers, q3: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: deleteAnswers.q3 === opt ? theme.accentRed : "#334155", color: "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>D. Enter Today's Date:</span>
                <input
                  type="text"
                  placeholder={todayFormattedDate}
                  value={deleteAnswers.dateInput}
                  onChange={(e) => setDeleteAnswers({ ...deleteAnswers, dateInput: e.target.value.trim() })}
                  style={{ width: "110px", padding: "5px 8px", backgroundColor: "#0f172a", border: "1px solid #475569", color: "#ffffff", borderRadius: "4px", textAlign: "center", fontWeight: "bold", fontSize: "12px" }}
                />
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", borderTop: "1px solid #334155", paddingTop: "14px" }}>
              <button onClick={handleAbortDelete} style={{ backgroundColor: "#334155", color: "#f87171", border: "1px solid #ef4444", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                ABORT
              </button>
              <button onClick={handleCancelDeleteStock} style={{ backgroundColor: "#475569", color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                CANCEL
              </button>

              {deleteAnswers.q1 === "Y" && deleteAnswers.q2 === "N" && deleteAnswers.q3 === "Y" && deleteAnswers.dateInput === todayFormattedDate && (
                <button onClick={handleOkDeleteStock} style={{ backgroundColor: theme.accentRed, color: "#ffffff", border: "none", padding: "8px 24px", borderRadius: "6px", fontWeight: "900", cursor: "pointer" }}>
                  OK
                </button>
              )}
            </div>

          </div>
        </div>
      )}

      {/* 3. UPDATE CONFIRMATION MODAL */}
      {updateQueue.length > 0 && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.85)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `2px solid ${theme.accentCyan}`, borderRadius: "12px", padding: "24px", width: "480px", color: "#f8fafc", boxShadow: "0 10px 40px rgba(0,0,0,0.8)" }}>
            
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #334155", paddingBottom: "10px", marginBottom: "16px" }}>
              <span style={{ fontSize: "12px", fontWeight: "bold", color: "#94a3b8" }}>
                UPDATING ITEM {currentUpdateIndex + 1} OF {updateQueue.length}
              </span>
              <span style={{ fontSize: "11px", backgroundColor: "#1e293b", padding: "3px 8px", borderRadius: "4px", border: `1px solid ${theme.accentCyan}`, color: theme.accentCyan, fontWeight: "900" }}>
                DATABASE SYNC
              </span>
            </div>

            <div style={{ textAlign: "center", marginBottom: "20px" }}>
              <h1 style={{ color: theme.accentCyan, fontSize: "28px", fontWeight: "900", margin: "0 0 6px 0", letterSpacing: "1px" }}>
                {extractDisplayName(updateQueue[currentUpdateIndex], mainDataMap)}
              </h1>
              <p style={{ margin: 0, fontSize: "12px", color: "#cbd5e1" }}>Confirm updating metadata & fundamentals for key: <b>{updateQueue[currentUpdateIndex]}</b></p>
            </div>

            <div style={{ display: "flex", flexDirection: "column", gap: "12px", fontSize: "13px", marginBottom: "24px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>A. Are you sure you want to update this stock?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setUpdateAnswers({ ...updateAnswers, q1: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: updateAnswers.q1 === opt ? theme.accentCyan : "#334155", color: updateAnswers.q1 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>B. Do you update this stock without any purpose?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setUpdateAnswers({ ...updateAnswers, q2: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: updateAnswers.q2 === opt ? theme.accentCyan : "#334155", color: updateAnswers.q2 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", background: "#1e293b", padding: "8px 12px", borderRadius: "6px" }}>
                <span>C. Do you update this stock without any research?</span>
                <div style={{ display: "flex", gap: "8px" }}>
                  {["Y", "N"].map((opt) => (
                    <button key={opt} onClick={() => setUpdateAnswers({ ...updateAnswers, q3: opt })} style={{ width: "32px", height: "28px", fontWeight: "bold", borderRadius: "4px", border: "none", cursor: "pointer", backgroundColor: updateAnswers.q3 === opt ? theme.accentCyan : "#334155", color: updateAnswers.q3 === opt ? "#000" : "#fff" }}>
                      {opt}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", borderTop: "1px solid #334155", paddingTop: "14px" }}>
              <button onClick={handleAbortUpdate} style={{ backgroundColor: "#334155", color: "#f87171", border: "1px solid #ef4444", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                ABORT
              </button>
              <button onClick={handleCancelUpdateStock} style={{ backgroundColor: "#475569", color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>
                CANCEL
              </button>

              {updateAnswers.q1 === "Y" && updateAnswers.q2 === "N" && updateAnswers.q3 === "N" && (
                <button onClick={handleOkUpdateStock} style={{ backgroundColor: theme.accentCyan, color: "#000000", border: "none", padding: "8px 24px", borderRadius: "6px", fontWeight: "900", cursor: "pointer" }}>
                  OK
                </button>
              )}
            </div>

          </div>
        </div>
      )}

      {/* TICKER MODAL */}
      {tickerModal.isOpen && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.8)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `3px solid ${theme.accentCyan}`, borderRadius: "12px", padding: "24px", width: "400px", maxWidth: "100%", boxShadow: "0 10px 40px rgba(0,0,0,0.7)", textAlign: "center" }}>
            <h3 style={{ color: theme.accentCyan, fontSize: "18px", fontWeight: "900", marginBottom: "16px", textTransform: "uppercase" }}>TICKER CONFIGURATION</h3>
            <p style={{ color: "#f8fafc", fontSize: "14px", fontWeight: "bold", marginBottom: "20px" }}>Select ticker mode for <span style={{ color: theme.accentAmber }}>{tickerModal.stockName}</span></p>

            <div style={{ display: "flex", justifyContent: "center", gap: "10px", marginBottom: "20px" }}>
              <button
                onClick={() => {
                  handleFieldChange(tickerModal.stockCode, "TICKER", tickerModal.defaultTicker);
                  setTickerModal({ isOpen: false, stockCode: "", stockName: "", text: "", defaultTicker: "", isManualMode: false });
                }}
                style={{ backgroundColor: "#334155", color: "#ffffff", border: "none", padding: "10px 20px", borderRadius: "6px", fontWeight: "900", cursor: "pointer" }}
              >
                USE DEFAULT
              </button>
              <button
                onClick={() => setTickerModal(prev => ({ ...prev, isManualMode: true }))}
                style={{ backgroundColor: theme.accentAmber, color: "#000000", border: "none", padding: "10px 20px", borderRadius: "6px", fontWeight: "900", cursor: "pointer" }}
              >
                MANUAL
              </button>
            </div>

            {tickerModal.isManualMode && (
              <div style={{ marginTop: "16px", borderTop: "1px solid #334155", paddingTop: "16px" }}>
                <input
                  type="text"
                  value={tickerModal.text}
                  onChange={(e) => setTickerModal(prev => ({ ...prev, text: e.target.value.toUpperCase() }))}
                  placeholder="Enter manual YF ticker..."
                  style={{ width: "100%", padding: "10px", backgroundColor: "#1e293b", color: theme.accentAmber, fontWeight: "900", borderRadius: "6px", border: "1px solid #334155", textAlign: "center", fontSize: "14px", boxSizing: "border-box", outline: "none", marginBottom: "16px" }}
                />
                <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px" }}>
                   <button onClick={() => setTickerModal({ isOpen: false, stockCode: "", stockName: "", text: "", defaultTicker: "", isManualMode: false })} style={{ backgroundColor: "#334155", color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>CANCEL</button>
                   <button onClick={() => {
                      handleFieldChange(tickerModal.stockCode, "TICKER", tickerModal.text);
                      setTickerModal({ isOpen: false, stockCode: "", stockName: "", text: "", defaultTicker: "", isManualMode: false });
                   }} style={{ backgroundColor: theme.accentGreen, color: "#000000", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "bold", cursor: "pointer" }}>SAVE TICKER</button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* MODIFY TOGGLE CONFIRM */}
      {showModifyConfirm && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.8)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `3px solid ${theme.accentAmber}`, borderRadius: "12px", padding: "30px", width: "420px", textAlign: "center", boxShadow: "0 10px 40px rgba(0,0,0,0.7)" }}>
            <h3 style={{ color: theme.accentAmber, fontSize: "20px", fontWeight: "900", marginBottom: "16px", textTransform: "uppercase" }}>MODIFY SECURITY MODE</h3>
            <p style={{ color: "#f8fafc", fontSize: "14px", fontWeight: "bold", marginBottom: "24px" }}>Do you want to unlock column data for manual modification?</p>
            <div style={{ display: "flex", justifyContent: "center", gap: "16px" }}>
              <button onClick={() => { setIsModify(true); setShowModifyConfirm(false); }} style={{ backgroundColor: theme.accentGreen, color: "#000000", border: "none", padding: "10px 24px", borderRadius: "6px", fontWeight: "900", fontSize: "14px", cursor: "pointer", textTransform: "uppercase" }}>YES</button>
              <button onClick={() => { setIsModify(false); setShowModifyConfirm(false); }} style={{ backgroundColor: theme.accentRed, color: "#ffffff", border: "none", padding: "10px 24px", borderRadius: "6px", fontWeight: "900", fontSize: "14px", cursor: "pointer", textTransform: "uppercase" }}>NO</button>
            </div>
          </div>
        </div>
      )}

      {/* NOTEPAD MODAL */}
      {notepadModal.isOpen && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.8)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `3px solid ${theme.accentCyan}`, borderRadius: "12px", padding: "24px", width: "600px", maxWidth: "100%", boxShadow: "0 10px 40px rgba(0,0,0,0.7)" }}>
            <h3 style={{ color: theme.accentCyan, fontSize: "18px", fontWeight: "900", marginBottom: "8px", textTransform: "uppercase" }}>NOTEPAD: REMARK FOR {notepadModal.stockName} ({notepadModal.stockCode})</h3>
            <p style={{ color: "#94a3b8", fontSize: "12px", marginBottom: "16px", fontWeight: "bold" }}>Enter detailed evaluation notes. Maximum 1000 words allowed.</p>
            
            <textarea
              rows="10"
              value={notepadModal.text}
              onChange={(e) => {
                const val = e.target.value;
                const wordCount = val.trim().split(/\s+/).filter(Boolean).length;
                if (wordCount <= 1000) {
                  setNotepadModal({ ...notepadModal, text: val, error: "" });
                } else {
                  setNotepadModal({ ...notepadModal, error: "ERROR: Word limit exceeded! Maximum allowed is 1000 words." });
                }
              }}
              placeholder="Type detailed review here..."
              style={{ width: "100%", backgroundColor: "#1e293b", border: "2px solid #334155", borderRadius: "6px", color: "#ffffff", padding: "12px", fontSize: "14px", fontWeight: "bold", boxSizing: "border-box", outline: "none" }}
            />

            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: "8px", fontSize: "12px", fontWeight: "bold" }}>
              <span style={{ color: notepadModal.text.trim().split(/\s+/).filter(Boolean).length >= 1000 ? theme.accentRed : "#94a3b8" }}>
                {notepadModal.text.trim().split(/\s+/).filter(Boolean).length} / 1000 words
              </span>
              {notepadModal.error && <span style={{ color: theme.accentRed }}>{notepadModal.error}</span>}
            </div>

            <div style={{ display: "flex", justifyContent: "flex-end", gap: "10px", marginTop: "20px" }}>
              <button onClick={() => setNotepadModal({ isOpen: false, stockCode: "", stockName: "", text: "", error: "" })} style={{ backgroundColor: "#334155", color: "#ffffff", border: "none", padding: "8px 16px", borderRadius: "6px", fontWeight: "900", cursor: "pointer", textTransform: "uppercase" }}>CANCEL</button>
              <button onClick={handleSaveNotepad} style={{ backgroundColor: theme.accentAmber, color: "#000000", border: "none", padding: "8px 20px", borderRadius: "6px", fontWeight: "900", cursor: "pointer", textTransform: "uppercase" }}>SAVE REMARK</button>
            </div>
          </div>
        </div>
      )}

      {/* HELP MODAL */}
      {showHelpModal && (
        <div style={{ position: "fixed", top: 0, left: 0, width: "100vw", height: "100vh", backgroundColor: "rgba(0,0,0,0.8)", display: "flex", justifyContent: "center", alignItems: "center", zIndex: 9999, padding: "20px" }}>
          <div style={{ backgroundColor: "#0f172a", border: `3px solid ${theme.accentAmber}`, borderRadius: "12px", padding: "30px", width: "600px", maxWidth: "100%", color: "#f8fafc", boxShadow: "0 10px 40px rgba(0,0,0,0.7)" }}>
            <h3 style={{ color: theme.accentAmber, fontSize: "20px", fontWeight: "900", marginBottom: "16px", textTransform: "uppercase" }}>HELP & COLUMN FILL UP GUIDE</h3>
            <div style={{ fontSize: "13px", fontWeight: "bold", lineHeight: "1.6", display: "flex", flexDirection: "column", gap: "10px", marginBottom: "24px" }}>
              <p>🔹 <b>GROUP:</b> Portfolio grouping label. Maximum 20 characters. Default is <b>GROUP-0</b>.</p>
              <p>🔹 <b>REVIEW:</b> Rating from <b>NR to 5 STAR</b>.</p>
              <p>🔹 <b>DURATION:</b> Investment horizon style.</p>
              <p>🔹 <b>REMARK:</b> Qualitative review notepad up to 1000 words.</p>
              <p>🔹 <b>DATE:</b> Read-only; auto-records update date in DD-MM-YYYY format.</p>
              <p>🔹 <b>TICKER:</b> Yahoo Finance tracking symbol (e.g. MAHABANK.NS).</p>
              <hr style={{ borderColor: "#334155", margin: "10px 0" }} />
              <p style={{ color: theme.accentCyan }}>🟢 All records are keyed by immutable primary key <b>CODE</b> across Firebase nodes.</p>
            </div>
            <div style={{ textAlign: "right" }}>
              <button onClick={() => setShowHelpModal(false)} style={{ backgroundColor: theme.accentAmber, color: "#000000", border: "none", padding: "10px 24px", borderRadius: "6px", fontWeight: "900", cursor: "pointer", textTransform: "uppercase" }}>GOT IT</button>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}