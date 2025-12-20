import { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './App.css';
import { 
  ShieldCheckIcon, ServerStackIcon, MagnifyingGlassIcon, 
  BoltIcon, BanknotesIcon, GlobeAmericasIcon, ArrowPathIcon, TrashIcon, 
  BeakerIcon, ExclamationTriangleIcon, SignalIcon, ChartBarIcon,
  ClockIcon, FireIcon , CheckCircleIcon , SparklesIcon
} from '@heroicons/react/24/outline';




export default function App() {
  const [currentUser, setCurrentUser] = useState('admin'); 
  const [description, setDescription] = useState('');
  const [amount, setAmount] = useState('');
  const [selectedBank, setSelectedBank] = useState('Bank A');
  const [isFraud, setIsFraud] = useState(0);

  const [searchQuery, setSearchQuery] = useState('');
  const [bankFilter, setBankFilter] = useState('All');
  const [minAmount, setMinAmount] = useState(0);
  const [results, setResults] = useState([]);
  
  const [systemStatus, setSystemStatus] = useState('System Online');
  const [lastAction, setLastAction] = useState(null);
  const [flStats, setFlStats] = useState({ accuracy: 0.82, version: 'v1.0' });
  // Add state for RAG
  const [ragReport, setRagReport] = useState(null);
  const [isGeneratingReport, setIsGeneratingReport] = useState(false);
  // ADD BELOW existing RAG state
  const [quickAssessment, setQuickAssessment] = useState(null);
  const [isCheckingThreat, setIsCheckingThreat] = useState(false);


  // NEW: Real-time streaming stats
  const [streamingStats, setStreamingStats] = useState({
    recent_transactions: 0,
    legitimate: 0,
    threats: 0,
    by_bank: { 'Bank A': 0, 'Bank B': 0, 'Bank C': 0 }
  });
  const [isStreaming, setIsStreaming] = useState(false);
  
  // Broadcast state
  const [broadcastBank, setBroadcastBank] = useState('Bank A');
  const [broadcastPattern, setBroadcastPattern] = useState('');
  const [broadcastResult, setBroadcastResult] = useState(null);

  // WebSocket ref
  const ws = useRef(null);

  // Auto-search on role switch
  useEffect(() => {
    setResults([]);
    setSearchQuery(''); 
    setLastAction(null);
    setBroadcastResult(null);

    const defaultQuery = '';
    
    const fetchDefaultData = async () => {
      try {
        setSystemStatus('Loading View...');
        const response = await axios.post('http://127.0.0.1:8000/secure-search', {
          query: defaultQuery, 
          bank_filter: bankFilter,
          min_amount: 0,
          user_id: currentUser
        });
        setResults(response.data.results);
        setSystemStatus('Ready');
      } catch (error) {
        console.error("Auto-load failed", error);
      }
    };

    fetchDefaultData();
  }, [currentUser]); 

  // WebSocket connection for real-time updates
  useEffect(() => {
    // Connect to WebSocket
    ws.current = new WebSocket('ws://127.0.0.1:8000/ws');
    
    ws.current.onopen = () => {
      console.log('WebSocket connected');
      setIsStreaming(true);
    };
    
    ws.current.onmessage = (event) => {
      const message = JSON.parse(event.data);
      
      if (message.type === 'stats_update') {
        setStreamingStats(message.data);
      }
    };
    
    ws.current.onerror = () => {
      setIsStreaming(false);
    };
    
    ws.current.onclose = () => {
      setIsStreaming(false);
    };
    
    // Cleanup
    return () => {
      if (ws.current) {
        ws.current.close();
      }
    };
  }, []);
  /*
  // Auto-refresh search results every 10 seconds when streaming
  useEffect(() => {
    if (!isStreaming) return;
    
    const interval = setInterval(() => {
      handleSearch(true); // Silent refresh
    }, 10000); // Slower refresh: every 10 seconds
    
    return () => clearInterval(interval);
  }, [isStreaming, searchQuery, bankFilter, minAmount, currentUser]);
  */
  // Actions
  const handleIngest = async () => {
    setQuickAssessment(null);
    if(!description || !amount) return;
    try {
      setSystemStatus('Encrypting & Ingesting...');
      
      const bank = currentUser === 'Alice' ? 'Bank A' : 
                   currentUser === 'Bob' ? 'Bank B' : 
                   currentUser === 'Charlie' ? 'Bank C' : 
                   selectedBank;
      
      await axios.post('http://127.0.0.1:8000/secure-ingest', {
        description: description,
        amount: parseFloat(amount),
        bank: bank,
        user_id: currentUser,
        is_fraud: currentUser === 'admin' ? isFraud : 0
      });
      
      setSystemStatus('Ready');
      
      if (currentUser === 'admin' && isFraud === 1) {
        setLastAction('🚨 Fraud Pattern Injected');
      } else if (currentUser === 'admin') {
        setLastAction('⚠️  Test Data Injected');
      } else {
        setLastAction('✅ Transfer Complete');
      }
      
      setDescription('');
      setAmount('');
      
      handleSearch(); 
    } catch (error) { 
      setSystemStatus('Error'); 
      console.error(error);
    }
  };
   
  const handleGenerateReport = async () => {
  try {
    setIsGeneratingReport(true);
    setSystemStatus('Generating AI Analysis...');
    
    const response = await axios.post('http://127.0.0.1:8000/rag-analysis', {
        query: searchQuery || 'fraud patterns',
        bank_filter: bankFilter,
        min_amount: parseFloat(minAmount),
        user_id: currentUser
      });
      
      setRagReport(response.data.report);
      setSystemStatus('Ready');
    } catch (error) {
      setSystemStatus('Error');
      console.error(error);
    } finally {
      setIsGeneratingReport(false);
    }
  };
  const handleQuickThreatCheck = async () => {
    if (!description || !amount) return;

    try {
      setIsCheckingThreat(true);
      setSystemStatus('Analyzing Transaction Risk...');

      const response = await axios.post(
        'http://127.0.0.1:8000/quick-threat-check',
        {
          description: description,
          amount: parseFloat(amount),
          bank: selectedBank
        }
      );

      setQuickAssessment(response.data.assessment);
      setSystemStatus('Ready');

    } catch (error) {
      setSystemStatus('Error');
      console.error(error);
    } finally {
      setIsCheckingThreat(false);
    }
  };



  const handleDelete = async (id) => {
    try {
      await axios.delete(`http://127.0.0.1:8000/secure-delete/${id}`);
      setResults(results.filter(r => r.id !== id));
    } catch (error) { console.error(error); }
  };

  const handleSearch = async (silent = false) => {
    try {
      if (!silent) setSystemStatus('Scanning...');
      const response = await axios.post('http://127.0.0.1:8000/secure-search', {
        query: searchQuery,
        bank_filter: bankFilter,
        min_amount: parseFloat(minAmount),
        user_id: currentUser
      });
      setResults(response.data.results);
      if (!silent) setSystemStatus('Ready');
    } catch (error) { 
      if (!silent) setSystemStatus('Error'); 
      console.error(error);
    }
  };

  const handleBroadcast = async () => {
    if (!broadcastPattern.trim()) return;
    
    try {
      setSystemStatus('Broadcasting Threat...');
      setLastAction('📡 Scanning Network...');
      
      console.log('Sending broadcast:', {
        source_bank: broadcastBank,
        description: broadcastPattern
      });
      
      const response = await axios.post('http://127.0.0.1:8000/secure-broadcast', {
        source_bank: broadcastBank,
        description: broadcastPattern
      }, {
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      console.log('Broadcast response:', response.data);
      
      setBroadcastResult(response.data);
      setSystemStatus('Ready');
      setLastAction(`🛡️  Protected ${response.data.total_protected} transactions`);
      
      setBroadcastPattern('');
      setTimeout(() => handleSearch(), 1000);
      
    } catch (error) {
      setSystemStatus('Error');
      console.error('Broadcast error:', error.response?.data || error.message);
      alert(`Broadcast failed: ${error.response?.data?.detail || error.message}`);
    }
  };

  const handleIndexTrain = async () => {
    setSystemStatus('Optimizing...'); 
    await axios.post('http://127.0.0.1:8000/secure-train'); 
    setSystemStatus('Ready'); 
    setLastAction('⚡ Index Optimized');
  };
  
  const handleFederatedRound = async () => {
    setSystemStatus('Aggregating...'); 
    const res = await axios.post('http://127.0.0.1:8000/federated-round');
    setFlStats({ accuracy: res.data.new_accuracy, version: res.data.round_id });
    setSystemStatus('Ready'); 
    setLastAction(`🚀 Model Updated to ${res.data.round_id}`);
  };
  
  const getRiskColor = (risk) => {
    if (risk.includes("LOW") || risk.includes("✅")) return "badge badge-success";
    if (risk.includes("MEDIUM") || risk.includes("⚠️")) return "badge badge-warning";
    if (risk.includes("BLOCKED") || risk.includes("🚫")) return "badge badge-danger-pulse";
    return "badge badge-danger";
  };


return (
  <div className="app-container">
    
    {/* =======================
        GLASSMORPHISM NAVBAR 
       ======================= */}
    <div style={{
      position: 'sticky',
      top: 0,
      zIndex: 50,
      backdropFilter: 'blur(12px)',
      backgroundColor: 'rgba(0, 0, 0, 0.3)', // Semi-transparent black based on your var(--color-black)
      borderBottom: '1px solid var(--color-dark-gray)'
    }}>
      <div className="container-max">
        <div className="header-bar" style={{ padding: '1rem 0' }}>
          
          {/* Brand */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <div className="bg-teal" style={{ padding: '0.5rem', borderRadius: '0.5rem' }}>
              <ShieldCheckIcon style={{ height: '1.5rem', width: '1.5rem', color: 'black' }} />
            </div>
            <div>
              <h1 className="text-primary" style={{ fontSize: '1.25rem', fontWeight: 'bold', lineHeight: 1 }}>
                Sentinel <span className="text-teal">AI</span>
              </h1>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginTop: '0.25rem' }}>
                <span className={`status-dot ${systemStatus.includes('Error') ? 'error' : 'online'}`}></span>
                <span className="text-muted" style={{ fontSize: '0.75rem', fontFamily: 'monospace' }}>v2.0 • {systemStatus}</span>
              </div>
            </div>
          </div>

          {/* Right Side Controls */}
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
            {/* Streaming Indicator */}
            <div className={`status-indicator ${isStreaming ? 'border-teal' : 'border-primary'}`}>
              <div className={`status-dot ${isStreaming ? 'online' : 'offline'}`}></div>
              <span className="text-primary" style={{ fontSize: '0.75rem', fontWeight: 500 }}>
                {isStreaming ? 'LIVE FEED' : 'OFFLINE'}
              </span>
            </div>

            {/* Role Switcher */}
            <div className={`card-secondary ${currentUser === 'admin' ? 'border-teal' : 'border-primary'}`} 
                 style={{ padding: '0.5rem 1rem', display: 'flex', alignItems: 'center', gap: '0.5rem', border: currentUser === 'admin' ? '1px solid var(--color-teal)' : '1px solid var(--color-dark-gray)' }}>
              <span className={currentUser === 'admin' ? 'text-teal' : 'text-secondary'} style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>
                {currentUser === 'admin' ? 'Admin Mode' : 'View As'}
              </span>
              <select 
                value={currentUser} 
                onChange={(e) => setCurrentUser(e.target.value)}
                className="select-base"
                style={{ border: 'none', padding: 0, width: 'auto', backgroundColor: 'transparent', cursor: 'pointer' }}
              >
                <option value="admin">🛡️ Security Admin</option>
                <option value="Alice">👤 Alice (Bank A)</option>
                <option value="Bob">👤 Bob (Bank B)</option>
                <option value="Charlie">👤 Charlie (Bank C)</option>
              </select>
            </div>
          </div>
        </div>
      </div>
    </div>

    {/* =======================
        MAIN CONTENT 
       ======================= */}
    <main className="container-max" style={{ paddingTop: '2rem', paddingBottom: '4rem' }}>
      
      {/* Toast Notification */}
      {lastAction && (
        <div style={{ display: 'flex', justifyContent: 'center', marginBottom: '2rem' }}>
          <div className="badge-success" style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}>
            {lastAction}
          </div>
        </div>
      )}

      {/* STATS GRID (Admin Only) */}
      {currentUser === 'admin' && isStreaming && (
        <div className="stats-grid" style={{ marginBottom: '2rem' }}>
          <div className="card-secondary" style={{ padding: '1.25rem', borderRadius: '0.75rem', borderLeft: '4px solid #ffffff' }}>
            <p className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>Transactions</p>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '0.5rem' }}>
              <p className="text-primary" style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>{streamingStats.recent_transactions}</p>
              <ChartBarIcon className="text-secondary" style={{ height: '1.5rem', width: '1.5rem' }} />
            </div>
          </div>
          
          <div className="card-secondary" style={{ padding: '1.25rem', borderRadius: '0.75rem', borderLeft: '4px solid var(--color-teal)' }}>
            <p className="text-teal" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>Legitimate</p>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '0.5rem' }}>
              <p className="text-teal" style={{ fontSize: '1.5rem', fontWeight: 'bold' }}>{streamingStats.legitimate}</p>
              <ClockIcon className="text-teal" style={{ height: '1.5rem', width: '1.5rem' }} />
            </div>
          </div>
          
          <div className="card-secondary" style={{ padding: '1.25rem', borderRadius: '0.75rem', borderLeft: '4px solid #dc3545' }}>
            <p style={{ color: '#dc3545', fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>Threats</p>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '0.5rem' }}>
              <p style={{ color: '#dc3545', fontSize: '1.5rem', fontWeight: 'bold' }}>{streamingStats.threats}</p>
              <FireIcon style={{ color: '#dc3545', height: '1.5rem', width: '1.5rem' }} />
            </div>
          </div>
          
          <div className="card-secondary" style={{ padding: '1rem', borderRadius: '0.75rem' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              {Object.entries(streamingStats.by_bank).map(([bank, count]) => (
                <div key={bank} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.75rem' }}>
                  <span className="text-secondary">{bank}</span>
                  <span className="text-primary" style={{ fontFamily: 'monospace' }}>{count}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* CORE LAYOUT GRID */}
      <div className="grid-responsive">
        
        {/* LEFT COLUMN (Actions) - spans 4 cols in CSS */}
        <div style={{ gridColumn: 'span 4' }}>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
            
            {/* INGESTION PANEL */}
            <section
              className={`card-secondary ${currentUser === 'admin' ? 'border-teal' : 'border-primary'}`}
              style={{
                padding: '1.5rem',
                borderRadius: '0.75rem',
                border: currentUser === 'admin'
                  ? '1px solid var(--color-teal)'
                  : '1px solid var(--color-dark-gray)'
              }}
            >
              <h2
                className="text-primary"
                style={{
                  fontSize: '1.125rem',
                  fontWeight: 'bold',
                  marginBottom: '1rem',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '0.5rem'
                }}
              >
                {currentUser === 'admin'
                  ? <BeakerIcon style={{ height: '1.25rem', width: '1.25rem' }} />
                  : <BanknotesIcon className="text-teal" style={{ height: '1.25rem', width: '1.25rem' }} />}
                {currentUser === 'admin' ? 'Fraud Injection' : 'Make Transfer'}
              </h2>

              {currentUser === 'admin' && (
                <div
                  className="admin-warning"
                  style={{ marginBottom: '1rem', display: 'flex', gap: '0.5rem' }}
                >
                  <ExclamationTriangleIcon style={{ height: '1rem', width: '1rem', flexShrink: 0 }} />
                  <p>Inject patterns to test AI detection.</p>
                </div>
              )}

              <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                {currentUser === 'admin' && (
                  <>
                    <div>
                      <label className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>
                        Target DB
                      </label>
                      <select
                        className="select-base"
                        style={{ marginTop: '0.25rem' }}
                        value={selectedBank}
                        onChange={(e) => setSelectedBank(e.target.value)}
                      >
                        <option value="Bank A">Bank A</option>
                        <option value="Bank B">Bank B</option>
                        <option value="Bank C">Bank C</option>
                      </select>
                    </div>

                    <div>
                      <label className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>
                        Pattern Type
                      </label>
                      <select
                        className="select-base"
                        style={{ marginTop: '0.25rem' }}
                        value={isFraud}
                        onChange={(e) => setIsFraud(parseInt(e.target.value))}
                      >
                        <option value={0}>✅ Legitimate (Normal)</option>
                        <option value={1}>🚨 Fraudulent (Threat)</option>
                      </select>
                    </div>
                  </>
                )}

                <input
                  className="input-base"
                  placeholder={currentUser === 'admin' ? "e.g. 'Stolen Card'" : "e.g. 'Coffee Shop'"}
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                />

                <input
                  type="number"
                  className="input-base"
                  placeholder="Amount ($)"
                  value={amount}
                  onChange={(e) => setAmount(e.target.value)}
                />

                <button
                  onClick={handleIngest}
                  className={currentUser === 'admin' ? 'btn-secondary' : 'btn-primary'}
                  style={{ width: '100%', marginTop: '0.5rem' }}
                >
                  {currentUser === 'admin'
                    ? (isFraud === 1 ? 'Inject Threat' : 'Inject Data')
                    : 'Send Funds'}
                </button>

                {/* QUICK THREAT CHECK (NON-ADMIN ONLY) */}
                {currentUser !== 'admin' && (
                  <>
                    <button
                      onClick={handleQuickThreatCheck}
                      disabled={isCheckingThreat}
                      className="btn-secondary"
                      style={{ width: '100%', marginTop: '0.5rem' }}
                    >
                      {isCheckingThreat ? 'Analyzing...' : '⚡ Quick Threat Check'}
                    </button>

                    {/* QUICK THREAT RESULT — LEFT PANEL ONLY */}
                    {quickAssessment && (
                      <div
                        className="rag-report-card"
                        style={{
                          marginTop: '1rem',
                          borderLeft: '3px solid var(--color-teal)'
                        }}
                      >
                        <div
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            marginBottom: '0.5rem'
                          }}
                        >
                          <h3 className="text-teal" style={{ fontWeight: 'bold' }}>
                            ⚡ Quick Threat Assessment
                          </h3>
                          <button
                            onClick={() => setQuickAssessment(null)}
                            className="text-muted"
                            style={{ background: 'none', border: 'none', cursor: 'pointer' }}
                          >
                            ✕
                          </button>
                        </div>

                        <div
                          className="text-secondary"
                          style={{
                            fontSize: '0.875rem',
                            whiteSpace: 'pre-wrap',
                            lineHeight: '1.6'
                          }}
                        >
                          {quickAssessment}
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            </section>


            {/* ADMIN TOOLS */}
            {currentUser === 'admin' && (
              <>
                <section className="broadcast-panel">
                  <div style={{ position: 'absolute', top: 0, right: 0, padding: '0.5rem', opacity: 0.1 }}>
                    <SignalIcon className="text-teal" style={{ height: '6rem', width: '6rem' }} />
                  </div>
                  <h2 className="text-teal" style={{ fontSize: '1.125rem', fontWeight: 'bold', marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                    <SignalIcon style={{ height: '1.25rem', width: '1.25rem' }} /> Threat Broadcast
                  </h2>
                  <p className="text-secondary" style={{ fontSize: '0.75rem', marginBottom: '1rem' }}>Share detected threats with the federation.</p>
                  
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem', position: 'relative', zIndex: 10 }}>
                    <input className="input-base" placeholder="Threat Signature..." value={broadcastPattern} onChange={(e) => setBroadcastPattern(e.target.value)} />
                    <button onClick={handleBroadcast} className="btn-primary" style={{ width: '100%', display: 'flex', justifyContent: 'center', gap: '0.5rem' }}>
                      Broadcast <SignalIcon style={{ height: '1rem', width: '1rem' }} />
                    </button>
                  </div>
                </section>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                   <div className="card-secondary" style={{ padding: '1rem', borderRadius: '0.75rem', border: '1px solid var(--color-dark-gray)' }}>
                      <GlobeAmericasIcon className="text-teal" style={{ height: '1.5rem', width: '1.5rem', marginBottom: '0.5rem' }} />
                      <div className="text-primary" style={{ fontSize: '0.875rem', fontWeight: 'bold' }}>Federated Sync</div>
                      <div className="text-muted" style={{ fontSize: '0.75rem' }}>Acc: {(flStats.accuracy * 100).toFixed(1)}%</div>
                      <button onClick={handleFederatedRound} className="btn-secondary" style={{ width: '100%', marginTop: '0.5rem', fontSize: '0.75rem', padding: '0.5rem' }}>Sync</button>
                   </div>
                   <div className="card-secondary" style={{ padding: '1rem', borderRadius: '0.75rem', border: '1px solid var(--color-dark-gray)' }}>
                      <ServerStackIcon className="text-secondary" style={{ height: '1.5rem', width: '1.5rem', marginBottom: '0.5rem' }} />
                      <div className="text-primary" style={{ fontSize: '0.875rem', fontWeight: 'bold' }}>Re-Index DB</div>
                      <div className="text-muted" style={{ fontSize: '0.75rem' }}>Optimize Vectors</div>
                      <button onClick={handleIndexTrain} className="btn-secondary" style={{ width: '100%', marginTop: '0.5rem', fontSize: '0.75rem', padding: '0.5rem' }}>Optimize</button>
                   </div>
                </div>
              </>
            )}
          </div>
        </div>

        {/* RIGHT COLUMN (Data View) - spans 8 cols in CSS */}
        <div style={{ gridColumn: 'span 8' }}>
          <section className="card-secondary" style={{ padding: '1.5rem', borderRadius: '0.75rem', minHeight: '600px', border: '1px solid var(--color-dark-gray)' }}>
            
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
              <h2 className="text-primary" style={{ fontSize: '1.25rem', fontWeight: 'bold', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <MagnifyingGlassIcon className="text-teal" style={{ height: '1.5rem', width: '1.5rem' }} />
                {currentUser === 'admin' ? 'Global Fraud Monitoring' : 'Recent Activity'}
              </h2>
              <button 
                onClick={handleGenerateReport} 
                disabled={isGeneratingReport}
                className="btn-secondary"
                style={{ padding: '0.5rem 1rem', display: 'flex', alignItems: 'center', gap: '0.5rem', opacity: isGeneratingReport ? 0.7 : 1 }}
              >
                {isGeneratingReport ? <ArrowPathIcon style={{ height: '1rem', width: '1rem', animation: 'spin 1s linear infinite' }} /> : '🤖 AI Analysis'}
              </button>
            </div>

            {/* Search */}
            <div className="search-bar">
              <input type="text" className="search-input" 
                placeholder={currentUser === 'admin' ? "Search for anomalies..." : "Search history..."} 
                value={searchQuery} onChange={(e) => setSearchQuery(e.target.value)}/>
              <button onClick={() => handleSearch(false)} className="btn-primary">
                Search
              </button>
            </div>

            {/* Admin Filters */}
            {currentUser === 'admin' && (
              <div className="filter-row">
                <div>
                  <label className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>Bank Filter</label>
                  <select className="select-base" style={{ marginTop: '0.25rem' }} value={bankFilter} onChange={(e) => setBankFilter(e.target.value)}>
                    <option value="All">All Banks</option>
                    <option value="Bank A">Bank A</option>
                    <option value="Bank B">Bank B</option>
                    <option value="Bank C">Bank C</option>
                  </select>
                </div>
                <div>
                  <label className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 'bold', textTransform: 'uppercase' }}>Min Amount: ${minAmount}</label>
                  <input type="range" min="0" max="10000" step="100" style={{ width: '100%', marginTop: '0.75rem', accentColor: 'var(--color-teal)' }}
                    value={minAmount} onChange={(e) => setMinAmount(e.target.value)} />
                </div>
              </div>
            )}

            {/* RAG Report */}
            {ragReport && (
              <div className="rag-report-card">
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                  <h3 className="text-teal" style={{ fontWeight: 'bold' }}>🤖 AI Intelligence Report</h3>
                  <button onClick={() => setRagReport(null)} className="text-muted" style={{ background: 'none', border: 'none', cursor: 'pointer' }}>✕</button>
                </div>
                <div className="text-secondary" style={{ fontSize: '0.875rem', whiteSpace: 'pre-wrap', lineHeight: '1.6' }}>
                  {ragReport.analysis}
                </div>
              </div>
            )}


            {/* Results List */}
            <div className="results-container" style={{ marginTop: '1rem' }}>
              {results.length === 0 ? (
                <div className="empty-state">
                  <MagnifyingGlassIcon className="text-muted" style={{ height: '3rem', width: '3rem', margin: '0 auto 1rem auto' }} />
                  <p>No transactions found matching your criteria.</p>
                </div>
              ) : (
                results.map((res) => (
                  <div key={res.id} className="transaction-card">
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                      
                      {/* Left: Info */}
                      <div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.25rem' }}>
                          {currentUser === 'admin' && (
                            <span className={
                              res.risk_level === 'High' ? 'badge badge-danger' : 
                              res.risk_level === 'Medium' ? 'badge badge-warning' : 'badge badge-success'
                            }>
                              {res.risk_level}
                            </span>
                          )}
                          <span className="badge" style={{ border: '1px solid #444', color: '#888' }}>{res.metadata.bank}</span>
                        </div>
                        <h4 className="text-primary" style={{ fontWeight: 500 }}>{res.metadata.description}</h4>
                      </div>

                      {/* Right: Amount & Action */}
                      <div style={{ textAlign: 'right' }}>
                        <div className="text-teal" style={{ fontFamily: 'monospace', fontWeight: 'bold', fontSize: '1.125rem' }}>
                          ${res.metadata.amount}
                        </div>
                        {currentUser === 'admin' && (
                          <button onClick={() => handleDelete(res.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', marginTop: '0.5rem', opacity: 0.5 }} title="Delete">
                            <TrashIcon style={{ height: '1rem', width: '1rem', color: '#fff' }} />
                          </button>
                        )}
                      </div>

                    </div>
                  </div>
                ))
              )}
            </div>

          </section>
        </div>

      </div>
    </main>

    {/* =======================
        PROFESSIONAL FOOTER
       ======================= */}
    <footer style={{ 
      borderTop: '1px solid var(--color-dark-gray)', 
      backgroundColor: 'var(--color-black)', 
      paddingTop: '3rem', 
      paddingBottom: '3rem',
      marginTop: 'auto' 
    }}>
      <div className="container-max">
        
        {/* Top Section: Grid Layout */}
        <div className="grid-responsive" style={{ 
          gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', 
          gap: '2rem', 
          marginBottom: '3rem' 
        }}>
          
          {/* Column 1: Brand & Mission */}
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '1rem' }}>
              <ShieldCheckIcon className="text-teal" style={{ height: '1.5rem', width: '1.5rem' }} />
              <span className="text-primary" style={{ fontWeight: 'bold', fontSize: '1.25rem' }}>Sentinel AI</span>
            </div>
            <p className="text-muted" style={{ fontSize: '0.875rem', lineHeight: '1.6', maxWidth: '300px' }}>
              Securing the global financial federation through decentralized intelligence and real-time fraud detection.
            </p>
          </div>

          {/* Column 2: Platform Links */}
          <div>
            <h4 className="text-primary" style={{ fontWeight: 'bold', fontSize: '0.875rem', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '1rem' }}>
              Platform
            </h4>
            <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>System Status</a></li>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>Developer API</a></li>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>Security Protocol</a></li>
            </ul>
          </div>

          {/* Column 3: Legal & Support */}
          <div>
            <h4 className="text-primary" style={{ fontWeight: 'bold', fontSize: '0.875rem', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: '1rem' }}>
              Compliance
            </h4>
            <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>Privacy Policy</a></li>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>Terms of Service</a></li>
              <li><a href="#" className="text-secondary" style={{ textDecoration: 'none', fontSize: '0.875rem', transition: 'color 0.2s' }}>GDPR & Compliance</a></li>
            </ul>
          </div>

        </div>

        {/* Bottom Section: Copyright & Badges */}
        <div style={{ 
          borderTop: '1px solid var(--color-dark-gray)', 
          paddingTop: '1.5rem', 
          display: 'flex', 
          justifyContent: 'space-between', 
          alignItems: 'center', 
          flexWrap: 'wrap',
          gap: '1rem'
        }}>
          <p className="text-muted" style={{ fontSize: '0.75rem' }}>
            © 2024 Sentinel AI Security. All rights reserved.
          </p>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: '1.5rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', opacity: 0.7 }}>
              <div style={{ width: '8px', height: '8px', backgroundColor: 'var(--color-teal)', borderRadius: '50%' }}></div>
              <span className="text-muted" style={{ fontSize: '0.75rem', fontWeight: 500 }}>All Systems Operational</span>
            </div>
            <span className="text-muted" style={{ fontSize: '0.75rem' }}>v2.0.4</span>
          </div>
        </div>

      </div>
    </footer>
  </div>
);
}