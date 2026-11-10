import { useEffect, useState } from 'react';
import type { Observation } from '../types/observation';
import { observationPreviewUrl } from '../api/client';

type Props = {
  observation: Observation;
  onClick: () => void;
};

export default function ObservationCard({ observation, onClick }: Props) {
  const [previewFailed, setPreviewFailed] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(true);
  const date = new Date(observation.observed_at).toLocaleDateString('en-US', {
    year: 'numeric', month: 'short', day: 'numeric',
  });

  const previewUrl = observationPreviewUrl(observation.source_url);

  useEffect(() => {
    setPreviewFailed(false);
    setPreviewLoading(true);
  }, [previewUrl]);

  return (
    <div
      className="observation-card"
      onClick={onClick}
      onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onClick(); } }}
      role="button"
      tabIndex={0}
      aria-label={`Open ${observation.id}, ${observation.band}`}
      style={{ cursor: 'pointer' }}
    >
      <div className="obs-image-container">
        {!previewFailed ? <>
          <img
            src={previewUrl}
            alt={`Observation ${observation.id}`}
            loading="lazy"
            className={`obs-image${previewLoading ? ' is-loading' : ''}`}
            onLoad={() => setPreviewLoading(false)}
            onError={() => { setPreviewLoading(false); setPreviewFailed(true); }}
          />
          {previewLoading && <div className="preview-loading" aria-live="polite">
            <span className="preview-spinner" aria-hidden="true" />
            <span>Generating infrared preview…</span>
          </div>}
        </> : (
          <div className="preview-fallback" role="img" aria-label="Preview unavailable">
            <span>Preview unavailable</span>
            <small>Open the observation for details or source data</small>
          </div>
        )}
        <div className="obs-badge">{observation.band}</div>
      </div>

      <div className="obs-content">
        <div className="obs-header">
          <h3>{observation.id}</h3>
          <span className="obs-date">{date}</span>
        </div>
        
        <p className="obs-position">RA {observation.ra.toFixed(2)}°, Dec {observation.dec.toFixed(2)}°</p>

        <div className="obs-footer">
          <span className="source-link">Click to view full image →</span>
        </div>
      </div>
    </div>
  );
}
