import React, { useCallback, useEffect, useState } from 'react';
import { Layers, RefreshCw } from 'lucide-react';
import { ParserPackSummary } from '../types/pack';
import { apiService } from '../services/api';
import { Badge } from '../components/Badge';

export const Packs: React.FC = () => {
  const [packs, setPacks] = useState<ParserPackSummary[]>([]);
  const [snapshotVersion, setSnapshotVersion] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchPacks = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const data = await apiService.listPacks();
      setPacks(data.packs);
      setSnapshotVersion(data.version);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Failed to load parser packs');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void fetchPacks(); }, [fetchPacks]);

  const rollback = async (name?: string) => {
    try {
      await apiService.rollbackPack(name);
      await fetchPacks();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Parser rollback failed');
    }
  };

  return (
    <div className="space-y-4">
      <div className="bg-[#161b22] border border-[#30363d] rounded-md p-4 flex items-center justify-between gap-3">
        <div>
          <h1 className="text-sm font-mono font-bold text-[#f0f6fc] flex items-center">
            <Layers className="w-4 h-4 mr-2 text-[#58a6ff]" /> Local Parser Packs
          </h1>
          <p className="text-xs font-mono text-[#8b949e] mt-1">Review the active local YAML parsers. Use Source Onboarding to validate, test, and activate a parser.</p>
        </div>
        <div className="flex items-center gap-3">
          <Badge variant="green">Snapshot: {snapshotVersion || 'live'}</Badge>
          <button onClick={() => void fetchPacks()} disabled={loading} aria-label="Refresh parser inventory" className="p-1.5 rounded bg-[#21262d] text-[#8b949e] hover:text-[#c9d1d9] border border-[#30363d]">
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {error && <p role="alert" className="text-xs text-red-400">{error}</p>}
      {!loading && packs.length === 0 && !error && <div className="bg-[#161b22] border border-[#30363d] rounded-md p-6 text-sm text-[#8b949e]">No parser YAML is active in this directory. Use Source Onboarding to author and validate a local parser.</div>}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {packs.map((p) => (
          <article key={p.name} className="bg-[#161b22] border border-[#30363d] rounded-md p-4 space-y-3">
            <div className="flex items-center justify-between pb-2 border-b border-[#21262d]">
              <span className="font-mono font-bold text-xs text-[#58a6ff]">{p.name}</span>
              <Badge variant="blue">v{p.version}</Badge>
            </div>
            <p className="text-xs font-mono text-[#8b949e]">{p.description || 'No description provided.'}</p>
            <div className="space-y-1 text-xs font-mono">
              <div className="flex justify-between"><span className="text-[#8b949e]">Category</span><Badge variant="purple">{p.source_category}</Badge></div>
              <div className="flex justify-between"><span className="text-[#8b949e]">Format</span><Badge variant="gray">{p.format.toUpperCase()}</Badge></div>
              <div className="flex justify-between"><span className="text-[#8b949e]">Rules</span><span>{p.field_count}</span></div>
              {p.author && <div className="flex justify-between"><span className="text-[#8b949e]">Author</span><span>{p.author}</span></div>}
              {p.modified && <p className="text-amber-300">File differs from its recorded digest.</p>}
              {p.filename && <p className="break-all text-[#8b949e]">{p.filename}</p>}
            </div>
            <div className="bg-[#0d1117] border border-[#21262d] rounded p-2 text-[11px] font-mono">
              <div className="text-[#8b949e] mb-0.5 font-semibold">Match signature</div>
              <div className="text-emerald-400 break-words">
                {p.match?.contains && `contains: "${p.match.contains}"`}
                {p.match?.regex && `regex: "${p.match.regex}"`}
                {p.match?.fieldEquals && `fieldEquals: ${JSON.stringify(p.match.fieldEquals)}`}
                {!p.match?.contains && !p.match?.regex && !p.match?.fieldEquals && 'All incoming records'}
              </div>
            </div>
            <button onClick={() => void rollback(p.name)} className="text-xs px-2 py-1 border border-[#30363d] rounded">Rollback this parser</button>
          </article>
        ))}
      </div>
      {packs.length > 0 && <button onClick={() => void rollback()} className="px-3 py-2 text-xs font-mono border border-[#30363d] rounded">Rollback last parser change</button>}
    </div>
  );
};
