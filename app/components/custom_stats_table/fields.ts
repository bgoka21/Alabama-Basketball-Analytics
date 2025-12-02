export type OnOffLine = {
  ppp_on: number | null;
  ppp_off: number | null;
  leverage: number | null;
  poss_pct: number | null;
};

export type OnOffSummary = {
  offense: OnOffLine;
  defense: OnOffLine;
};

export type FieldDescriptor = {
  key: string;
  label: string;
  category: string;
  format: (value: number | null) => string;
};

/**
 * Field definitions for the custom on/off summary panel. Each formatter receives
 * a single COOE metric from the backend summary payload and applies the display
 * fallback expected by the UI.
 */
export const customStatsFields: FieldDescriptor[] = [
  {
    key: 'summary.offense.ppp_on',
    label: 'PPP On',
    category: 'Offense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.offense.ppp_off',
    label: 'PPP Off',
    category: 'Offense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.offense.leverage',
    label: 'Leverage',
    category: 'Offense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.offense.poss_pct',
    label: '% Poss',
    category: 'Offense',
    format: (v) => (v == null ? '—' : `${(v * 100).toFixed(1)}%`),
  },
  {
    key: 'summary.defense.ppp_on',
    label: 'PPP On',
    category: 'Defense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.defense.ppp_off',
    label: 'PPP Off',
    category: 'Defense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.defense.leverage',
    label: 'Leverage',
    category: 'Defense',
    format: (v) => (v == null ? '—' : v.toFixed(2)),
  },
  {
    key: 'summary.defense.poss_pct',
    label: '% Poss',
    category: 'Defense',
    format: (v) => (v == null ? '—' : `${(v * 100).toFixed(1)}%`),
  },
];
