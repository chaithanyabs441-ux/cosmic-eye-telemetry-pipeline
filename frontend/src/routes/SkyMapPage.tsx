import { useEffect, useMemo, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { AlertCircle, ArrowLeft, ArrowUpRight, CircleDot, Globe2, Layers, Loader2, Maximize2, Minimize2, Orbit, RotateCcw, Search, Shapes, Sparkles, Star, X } from 'lucide-react';
import SkyMapGlobe from '../components/SkyMapGlobe';
import { getFeaturedTargets, searchObservations } from '../api/observations';
import { fetchJson, observationPreviewUrl } from '../api/client';
import type { FeaturedTarget, Observation } from '../types/observation';
import ObservationViewer from '../components/ObservationViewer';

type Filter = 'all' | 'nebulae' | 'galaxies' | 'stars' | 'planets' | 'other';
const CUSTOM_TARGETS_KEY = 'cosmic-eye-skymap-custom-targets';
const HIPPARCOS_STAR_COUNT = 4992;
const CONFIRMED_PLANET_SYSTEMS = new Set(['toi700', 'trappist1']);
const FILTER_OPTIONS: { value: Filter; label: string; Icon: typeof Layers }[] = [
  { value: 'all', label: 'All', Icon: Layers },
  { value: 'nebulae', label: 'Nebulae', Icon: Sparkles },
  { value: 'galaxies', label: 'Galaxies', Icon: Orbit },
  { value: 'stars', label: 'Stars', Icon: Star },
  { value: 'planets', label: 'Planets', Icon: CircleDot },
  { value: 'other', label: 'Other', Icon: Shapes },
];

function objectKind(type: string) {
  if (/nebula|star-forming|complex|remnant/i.test(type)) return 'nebula';
  if (/planet|exoplanet/i.test(type)) return 'planet';
  if (/galaxy/i.test(type)) return 'galaxy';
  if (/star|stellar|cluster|association/i.test(type) && !/star-forming/i.test(type)) return 'star';
  return 'other';
}

function matchesFilter(target: FeaturedTarget, filter: Filter) {
  if (filter === 'all') return true;
  if (filter === 'other') return objectKind(target.type) === 'other';
  if (filter === 'stars') return objectKind(target.type) === 'star';
  if (filter === 'planets') return objectKind(target.type) === 'planet';
  return objectKind(target.type) === (filter === 'galaxies' ? 'galaxy' : 'nebula');
}

export default function SkyMapPage() {
  const navigate = useNavigate();
  const pageRef = useRef<HTMLElement>(null);
  const [targets, setTargets] = useState<FeaturedTarget[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>('all');
  const [resetSignal, setResetSignal] = useState(0);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [showGrid, setShowGrid] = useState(false);
  const [showConstellations, setShowConstellations] = useState(true);
  const [showStarNames, setShowStarNames] = useState(true);
  const [showTargetPanel, setShowTargetPanel] = useState(true);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [targetQuery, setTargetQuery] = useState('');
  const [listQuery, setListQuery] = useState('');
  const [addingTarget, setAddingTarget] = useState(false);
  const [addMessage, setAddMessage] = useState<{ kind: 'success' | 'error'; text: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [archivePreview, setArchivePreview] = useState<Observation | null>(null);
  const [archivePreviewLoading, setArchivePreviewLoading] = useState(false);
  const [archivePreviewError, setArchivePreviewError] = useState<string | null>(null);
  const [archivePreviewFailed, setArchivePreviewFailed] = useState(false);
  const [viewingObservation, setViewingObservation] = useState<Observation | null>(null);

  useEffect(() => {
    const syncFullscreen = () => setIsFullscreen(document.fullscreenElement === pageRef.current);
    document.addEventListener('fullscreenchange', syncFullscreen);
    return () => document.removeEventListener('fullscreenchange', syncFullscreen);
  }, []);

  useEffect(() => {
    getFeaturedTargets()
      .then((data) => {
        let saved: FeaturedTarget[] = [];
        try {
          const parsed: unknown = JSON.parse(window.localStorage.getItem(CUSTOM_TARGETS_KEY) || '[]');
          if (Array.isArray(parsed)) saved = parsed as FeaturedTarget[];
        } catch { /* Ignore stale or blocked local storage. */ }
        const ids = new Set(data.map((target) => target.id));
        const merged = [...data, ...saved.filter((target) => !ids.has(target.id))];
        setTargets(merged);
        setSelectedId(merged[0]?.id ?? null);
      })
      .catch((err: unknown) => setError(err instanceof Error ? err.message : 'Could not load sky targets.'))
      .finally(() => setLoading(false));
  }, []);

  const visibleTargets = useMemo(() => targets.filter((target) => matchesFilter(target, filter)), [targets, filter]);
  const selectedTarget = targets.find((target) => target.id === selectedId) ?? null;
  const hoveredTarget = targets.find((target) => target.id === hoveredId) ?? null;
  const searchedTargets = useMemo(() => visibleTargets.filter((target) => `${target.name} ${target.type}`.toLowerCase().includes(listQuery.trim().toLowerCase())), [visibleTargets, listQuery]);

  useEffect(() => {
    if (!selectedTarget) return;
    let cancelled = false;
    setArchivePreview(null);
    setArchivePreviewFailed(false);
    setArchivePreviewError(null);
    setArchivePreviewLoading(true);
    searchObservations(selectedTarget.ra, selectedTarget.dec, 0.1, 1, 1)
      .then((data) => {
        if (cancelled) return;
        setArchivePreview(data.items[0] ?? null);
        if (data.items.length === 0) setArchivePreviewError('No released SPHEREx QR3 image tile covers this position yet.');
      })
      .catch((reason: unknown) => {
        if (!cancelled) setArchivePreviewError(reason instanceof Error ? reason.message : 'Could not load a SPHEREx image for this position.');
      })
      .finally(() => { if (!cancelled) setArchivePreviewLoading(false); });
    return () => { cancelled = true; };
  }, [selectedTarget?.id]);

  const setFilterAndSelection = (nextFilter: Filter) => {
    setFilter(nextFilter);
    const nextTargets = targets.filter((target) => matchesFilter(target, nextFilter));
    if (!nextTargets.some((target) => target.id === selectedId)) setSelectedId(nextTargets[0]?.id ?? null);
  };

  const selectTarget = (id: string) => {
    setSelectedId(id);
  };

  const toggleFullscreen = async () => {
    try {
      if (document.fullscreenElement === pageRef.current) await document.exitFullscreen();
      else await pageRef.current?.requestFullscreen();
    } catch {
      // Full-screen mode can be blocked by browser or embedding policy.
    }
  };

  const addTarget = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const query = targetQuery.trim();
    if (!query || addingTarget) return;
    setAddingTarget(true);
    setAddMessage(null);
    try {
      const resolved = await fetchJson<{ ra: number; dec: number; matched_query: string; search_radius?: number; object_type?: string }>(`/api/v1/observations/ai-search?q=${encodeURIComponent(query)}`);
      const archive = await searchObservations(resolved.ra, resolved.dec, resolved.search_radius ?? 0.01, 1, 1);
      if (archive.items.length === 0) {
        setAddMessage({ kind: 'error', text: `Found ${resolved.matched_query}, but no released SPHEREx image covers that position yet.` });
        return;
      }
      const normalized = resolved.matched_query.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
      const id = `simbad-${normalized || `${resolved.ra.toFixed(4)}-${resolved.dec.toFixed(4)}`}`;
      const rawType = resolved.object_type || 'Astronomical object';
      const normalizedName = query.toLowerCase().replace(/[^a-z0-9]/g, '');
      const confirmedPlanetSystem = CONFIRMED_PLANET_SYSTEMS.has(normalizedName);
      const type = confirmedPlanetSystem ? 'Exoplanet system'
        : /neb|hii|remnant|hii|refneb/i.test(rawType) ? 'Nebula'
          : /planet|exoplanet/i.test(rawType) ? 'Planet'
            : /galaxy|seyfert|agn|liner|starburst|gtowards|ginpair/i.test(rawType) ? 'Galaxy'
          : /cluster|association/i.test(rawType) ? 'Star cluster' : rawType;
      const added: FeaturedTarget = {
        id, name: resolved.matched_query, type, ra: resolved.ra, dec: resolved.dec,
        description: `${confirmedPlanetSystem ? 'Confirmed exoplanet system; host position resolved by SIMBAD. ' : 'Resolved by SIMBAD. '}SPHEREx archive image coverage is confirmed at this position.`,
        image_url: '',
      };
      const existing = targets.find((target) => target.id === id || (target.name.toLowerCase() === added.name.toLowerCase() && Math.abs(target.ra - added.ra) < 0.001 && Math.abs(target.dec - added.dec) < 0.001));
      if (existing) {
        setSelectedId(existing.id);
        setFilter('all');
        setAddMessage({ kind: 'success', text: `${existing.name} is already on your map.` });
        return;
      }
      setTargets((current) => [...current, added]);
      setSelectedId(id);
      setFilter('all');
      setTargetQuery('');
      try {
        const oldSaved = JSON.parse(window.localStorage.getItem(CUSTOM_TARGETS_KEY) || '[]') as FeaturedTarget[];
        window.localStorage.setItem(CUSTOM_TARGETS_KEY, JSON.stringify([...oldSaved, added]));
      } catch { /* The current map still works if storage is unavailable. */ }
      setAddMessage({ kind: 'success', text: `Added ${added.name} with verified SPHEREx image coverage.` });
    } catch (error) {
      setAddMessage({ kind: 'error', text: error instanceof Error ? error.message : 'Could not resolve this name or check SPHEREx coverage.' });
    } finally {
      setAddingTarget(false);
    }
  };

  return (
    <div className="dashboard-wrapper skymap-page">
      <section ref={pageRef} className={`skymap-content${isFullscreen ? ' is-fullscreen' : ''}`}>
        <header className="skymap-heading">
          <div>
            <span className="eyebrow">COSMIC EYE · REAL SKY COORDINATES</span>
            <h1>Explore the night sky</h1>
            <p>{HIPPARCOS_STAR_COUNT.toLocaleString()} Hipparcos stars · {targets.length} SPHEREx targets · Drag to rotate, scroll to zoom</p>
          </div>
          <button type="button" className="skymap-exit-button" onClick={() => navigate('/')}><ArrowLeft size={15} /> Exit sky map</button>
        </header>
        <div className={`skymap-layout${showTargetPanel ? ' has-target-panel' : ''}`}>
          <div className="skymap-viewer-card">
            <div className="skymap-toolbar">
              <div className="skymap-map-actions">
                <button type="button" className={showGrid ? 'skymap-map-action active' : 'skymap-map-action'} onClick={() => setShowGrid((visible) => !visible)} title="Toggle the RA/Dec coordinate grid" aria-label="Toggle coordinate grid"><Globe2 size={16} /></button>
                <button type="button" className={showConstellations ? 'skymap-map-action active' : 'skymap-map-action'} onClick={() => setShowConstellations((visible) => !visible)} title="Toggle familiar constellation outlines" aria-label="Toggle constellations"><Sparkles size={16} /></button>
                <button type="button" className={showStarNames ? 'skymap-map-action active' : 'skymap-map-action'} onClick={() => setShowStarNames((visible) => !visible)} title="Toggle labels for bright named stars" aria-label="Toggle star names"><Star size={16} /></button>
                <button type="button" className="skymap-map-action" onClick={() => setResetSignal((value) => value + 1)} title="Reset sky orientation and zoom" aria-label="Reset sky view"><RotateCcw size={16} /></button>
                <button type="button" className={showTargetPanel ? 'skymap-map-action active' : 'skymap-map-action'} onClick={() => setShowTargetPanel((visible) => !visible)} aria-expanded={showTargetPanel} aria-controls="skymap-target-panel" title={showTargetPanel ? 'Hide target panel' : 'Show target panel'} aria-label={showTargetPanel ? 'Hide target panel' : 'Show target panel'}><Layers size={16} /></button>
                <button type="button" className="skymap-map-action" onClick={() => void toggleFullscreen()} aria-pressed={isFullscreen} title={isFullscreen ? 'Exit full-screen sky map' : 'Explore the sky map full screen'} aria-label={isFullscreen ? 'Exit full screen' : 'Enter full screen'}>{isFullscreen ? <Minimize2 size={16} /> : <Maximize2 size={16} />}</button>
                <span className="skymap-coordinate-system">ICRS / J2000</span>
              </div>
            </div>
            <div className="skymap-globe-wrap">
              {!loading && !error && <SkyMapGlobe targets={visibleTargets} selectedId={selectedId} onSelect={selectTarget} resetSignal={resetSignal} showGrid={showGrid} showConstellations={showConstellations} showStarNames={showStarNames} focusedTarget={selectedTarget ? { ra: selectedTarget.ra, dec: selectedTarget.dec } : null} onHover={setHoveredId} />}
              {loading && <div className="skymap-loading"><Loader2 size={24} className="spin" /> Loading observed targets…</div>}
              {error && <div className="skymap-error"><AlertCircle size={20} />{error}</div>}
              {hoveredTarget && <div className="skymap-hover-card"><span>TARGET IN VIEW</span><strong>{hoveredTarget.name}</strong><small>RA {hoveredTarget.ra.toFixed(2)}° · Dec {hoveredTarget.dec.toFixed(2)}°</small></div>}
            </div>
            <div className="skymap-legend">
              <span><i className="legend-dot guide-star" /> Hipparcos catalog stars</span>
              <span><Sparkles className="legend-category-icon nebula" size={14} /> SPHEREx targets</span>
              <span><i className="legend-constellation-line" /> Constellation outlines</span>
              <span><i className="legend-milky-way" /> Milky Way guide band</span>
            </div>
            <p className="skymap-science-note">Catalog stars and SPHEREx targets are plotted at their catalog ICRS coordinates. The Milky Way band is an illustrative guide, not SPHEREx image data.</p>
          </div>

          {showTargetPanel && <aside id="skymap-target-panel" className="skymap-target-panel">
            <div className="skymap-panel-heading">
              <div><h2>SPHEREx targets</h2><p>Catalog targets · live QR3 coverage check</p></div>
              <span>{visibleTargets.length}</span>
            </div>

            <div className="skymap-filters" role="group" aria-label="Filter sky targets">
              {FILTER_OPTIONS.map(({ value, label, Icon }) => (
                <button key={value} type="button" title={`Show ${label.toLowerCase()} targets`} aria-pressed={filter === value} className={filter === value ? 'skymap-filter active' : 'skymap-filter'} onClick={() => setFilterAndSelection(value)}>
                  <Icon size={13} aria-hidden="true" />{label}
                </button>
              ))}
            </div>

            <form className="skymap-object-search" onSubmit={addTarget}>
              <label htmlFor="skymap-target-search">Add any covered object</label>
              <div className="skymap-object-search-row">
                <Search size={16} />
                <input id="skymap-target-search" value={targetQuery} onChange={(event) => setTargetQuery(event.target.value)} placeholder="Try M81, NGC 7000, or a name" />
                {targetQuery && <button type="button" className="skymap-search-clear" onClick={() => setTargetQuery('')} aria-label="Clear target name"><X size={14} /></button>}
                <button type="submit" disabled={!targetQuery.trim() || addingTarget}>{addingTarget ? <Loader2 size={15} className="spin" /> : 'Add'}</button>
              </div>
              {addMessage && <p className={`skymap-search-message ${addMessage.kind}`} role="status">{addMessage.text}</p>}
              <div className="skymap-suggestions" aria-label="Example targets">
                {['NGC 7000', 'Sirius', 'TOI-700', 'TRAPPIST-1'].map((name) => <button key={name} type="button" onClick={() => setTargetQuery(name)}>{name}</button>)}
              </div>
              <small>Names are resolved with the local catalog or SIMBAD, then checked against the current SPHEREx QR3 archive. Planetary systems are plotted at their host-star position.</small>
            </form>

            <div className="skymap-local-search"><Search size={14} /><input aria-label="Filter mapped targets" value={listQuery} onChange={(event) => setListQuery(event.target.value)} placeholder="Filter mapped targets…" /></div>

            <div className="skymap-target-list">
              {searchedTargets.map((target) => (
                <button key={target.id} type="button" className={selectedId === target.id ? 'skymap-target active' : 'skymap-target'} onClick={() => selectTarget(target.id)}>
                  <span className={`skymap-target-marker ${objectKind(target.type)}`} />
                  <span className="skymap-target-name"><strong>{target.name}</strong><small>{target.type}</small></span>
                  <span className="skymap-target-check" aria-label="Catalog target">IR</span>
                </button>
              ))}
            </div>

            {selectedTarget && (
              <div className="skymap-selected-card">
                <span className="eyebrow">SELECTED REGION</span>
                <h3>{selectedTarget.name}</h3>
                <p>{selectedTarget.description}</p>
                <dl>
                  <div><dt>Right ascension</dt><dd>{selectedTarget.ra.toFixed(3)}°</dd></div>
                  <div><dt>Declination</dt><dd>{selectedTarget.dec.toFixed(3)}°</dd></div>
                </dl>
                <div className="skymap-archive-preview">
                  <div className="skymap-archive-preview-heading"><strong>SPHEREx infrared image</strong><span>IRSA · QR3</span></div>
                  {archivePreviewLoading && <div className="skymap-archive-preview-state"><Loader2 size={16} className="spin" /> Searching current archive…</div>}
                  {!archivePreviewLoading && archivePreview && (
                    <button type="button" className="skymap-archive-image-button" onClick={() => setViewingObservation(archivePreview)}>
                      {!archivePreviewFailed ? <img src={observationPreviewUrl(archivePreview.source_url)} alt={`SPHEREx ${archivePreview.band} image tile near ${selectedTarget.name}`} onLoad={() => setArchivePreviewFailed(false)} onError={() => setArchivePreviewFailed(true)} /> : <span className="skymap-archive-preview-state">Image preview failed. Open the archive result for details.</span>}
                      <span>{archivePreview.id} · {archivePreview.band} · Click to zoom</span>
                    </button>
                  )}
                  {!archivePreviewLoading && !archivePreview && archivePreviewError && <p className="skymap-archive-preview-state">{archivePreviewError}</p>}
                </div>
                <button type="button" className="glow-btn skymap-explore-button" onClick={() => navigate('/explore', { state: { ra: selectedTarget.ra.toString(), dec: selectedTarget.dec.toString(), targetName: selectedTarget.name, radius: 0.1 } })}>
                  Open SPHEREx images <ArrowUpRight size={16} />
                </button>
              </div>
            )}
          </aside>}
        </div>
      </section>
      {viewingObservation && <ObservationViewer observation={viewingObservation} onClose={() => setViewingObservation(null)} />}
    </div>
  );
}
