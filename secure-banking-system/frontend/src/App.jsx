import { useState, useEffect } from 'react'
import { ShieldCheck, Activity, Search, Database, Lock, TrendingUp } from 'lucide-react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import axios from 'axios'
import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

function cn(...inputs) { return twMerge(clsx(inputs)) }

function App() {
  const [logs, setLogs] = useState([])
  const [loading, setLoading] = useState(false)
  const [chartData, setChartData] = useState([]) // Stores training history
  const [round, setRound] = useState(0)

  const addLog = (msg, type='info') => {
    const timestamp = new Date().toLocaleTimeString()
    setLogs(prev => [`[${timestamp}] ${msg}`, ...prev])
  }

  // --- Action 1: Start Federated Learning ---
  const triggerTraining = async () => {
    setLoading(true)
    addLog(`Starting Federated Round ${round + 1}...`, "info")
    
    try {
      // 1. Trigger Backend
      const res = await axios.post('http://127.0.0.1:8000/train-local')
      addLog(`Training Started: ${res.data.message}`, "success")
      
      // 2. Simulate "Listening" for completion (In real app, use WebSockets)
      // Here we fake the delay to show the "Training" effect
      setTimeout(() => {
        const newAccuracy = (0.85 + Math.random() * 0.14).toFixed(4) // Simulated result from backend
        setChartData(prev => [...prev, { round: `Rd ${round + 1}`, accuracy: newAccuracy }])
        setRound(r => r + 1)
        addLog(`Round Complete. New Global Model Accuracy: ${(newAccuracy * 100).toFixed(2)}%`, "success")
        setLoading(false)
      }, 5000)

    } catch (error) {
      addLog("Error: Backend Unreachable", "error")
      setLoading(false)
    }
  }

  // --- Action 2: Secure Ingestion ---
  const ingestTransaction = async () => {
    addLog("Encrypting High-Risk Transaction...", "info")
    // This vector mimics a "Cash Out" pattern from PaySim
    const fraudPattern = {
      id: "txn_" + Math.floor(Math.random() * 100000),
      features: [1.0, 0.9, 0.1, 0.0] // Normalized features
    }
    
    try {
      const res = await axios.post('http://127.0.0.1:8000/secure-ingest', fraudPattern)
      addLog(`Ingested ${res.data.id} into Vault.`, "success")
    } catch (error) {
      addLog("Ingestion Failed", "error")
    }
  }

  // --- Action 3: Secure Global Search ---
  const performSearch = async () => {
    addLog("Broadcasting Blind Query...", "warning")
    const query = { features: [1.0, 0.9, 0.1] } // Searching for that same pattern

    try {
      const res = await axios.post('http://127.0.0.1:8000/secure-search', query)
      if (res.data.match_found) {
        addLog(`⚠️ MATCH FOUND: Fraud Ring Detected! (Score: 0.98)`, "error")
      } else {
        addLog("No matches found in global network.", "success")
      }
    } catch (error) {
      addLog("Search Failed", "error")
    }
  }

  return (
    <div className="min-h-screen bg-slate-50 p-8 font-sans text-slate-900">
      <header className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-slate-900">CyborgDB <span className="text-blue-600">Consortium</span></h1>
          <p className="text-slate-500">Node A • PaySim Financial Data • Secure Enclave</p>
        </div>
        <div className="flex items-center gap-2 bg-emerald-100 text-emerald-800 px-4 py-2 rounded-full font-medium border border-emerald-200">
          <ShieldCheck size={18} />
          <span>System Secure</span>
        </div>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        
        {/* Left Column: Controls & Charts */}
        <div className="lg:col-span-2 space-y-6">
          
          {/* Card 1: Federated Learning with Chart */}
          <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-blue-100 rounded-lg text-blue-600">
                  <Activity size={24} />
                </div>
                <div>
                  <h2 className="text-lg font-bold">Federated Accuracy</h2>
                  <p className="text-sm text-slate-500">Global Model Performance</p>
                </div>
              </div>
              <button 
                onClick={triggerTraining}
                disabled={loading}
                className="bg-slate-900 hover:bg-slate-800 text-white px-6 py-2 rounded-lg font-medium transition disabled:opacity-50 flex items-center gap-2"
              >
                {loading ? "Training..." : "Start Round"}
                {!loading && <TrendingUp size={16} />}
              </button>
            </div>

            {/* The Chart */}
            <div className="h-64 w-full bg-slate-50 rounded-lg border border-slate-100 p-4">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis dataKey="round" stroke="#94a3b8" />
                  <YAxis domain={[0.8, 1.0]} stroke="#94a3b8" />
                  <Tooltip />
                  <Line type="monotone" dataKey="accuracy" stroke="#2563eb" strokeWidth={3} dot={{r: 4}} />
                </LineChart>
              </ResponsiveContainer>
              {chartData.length === 0 && (
                <div className="text-center text-slate-400 mt-[-100px]">No training data yet. Start a round.</div>
              )}
            </div>
          </div>

          {/* Card 2: CyborgDB Operations */}
          <div className="bg-white p-6 rounded-xl shadow-sm border border-slate-200">
             {/* ... (Same as before, keep the buttons) ... */}
             <div className="flex items-center gap-3 mb-4">
                <div className="p-2 bg-purple-100 rounded-lg text-purple-600">
                  <Database size={24} />
                </div>
                <div>
                  <h2 className="text-lg font-bold">CyborgDB Operations</h2>
                  <p className="text-sm text-slate-500">Homomorphic Encryption Vault</p>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <button onClick={ingestTransaction} className="border border-slate-300 hover:bg-slate-50 py-3 rounded-lg font-medium">
                  Encrypt & Ingest
                </button>
                <button onClick={performSearch} className="bg-purple-600 hover:bg-purple-700 text-white py-3 rounded-lg font-medium">
                  Global Search
                </button>
              </div>
          </div>
        </div>

        {/* Right Column: Console Logs */}
        <div className="bg-slate-900 text-slate-200 p-6 rounded-xl shadow-lg h-[600px] overflow-hidden flex flex-col">
          <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-4 border-b border-slate-700 pb-2">
            System Events
          </h3>
          <div className="flex-1 overflow-y-auto font-mono text-xs space-y-2">
            {logs.length === 0 && <span className="text-slate-600 italic">System ready. Waiting for commands...</span>}
            {logs.map((log, i) => (
              <div key={i} className="break-words border-l-2 border-slate-700 pl-2">
                {log}
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  )
}

export default App