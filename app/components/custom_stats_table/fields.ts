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

export type StatPickerField = {
  key: string;
  label: string;
  group: string;
  format: 'count' | 'ratio' | 'percent' | 'shooting_split';
};

export type StatPickerCatalog = Record<string, StatPickerField[]>;

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

/**
 * Field definitions for the Custom Stats Table stat picker. The entries are
 * grouped by the UI category labels, which control the accordion sections in
 * the selector. New COOE fields belong to the "Advanced" group.
 */
export const statPickerCatalog: StatPickerCatalog = {
  Advanced: [
    {
      key: 'adv_ppp_on_offense',
      label: 'Offensive PPP (On)',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_ppp_off_offense',
      label: 'Offensive PPP (Off)',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_offensive_leverage',
      label: 'Offensive Leverage',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_ppp_on_defense',
      label: 'Defensive PPP (On)',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_ppp_off_defense',
      label: 'Defensive PPP (Off)',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_defensive_leverage',
      label: 'Defensive Leverage',
      group: 'Advanced',
      format: 'ratio',
    },
    {
      key: 'adv_off_possession_pct',
      label: '% of Off Possessions',
      group: 'Advanced',
      format: 'percent',
    },
    {
      key: 'adv_def_possession_pct',
      label: '% of Def Possessions',
      group: 'Advanced',
      format: 'percent',
    },
  ],
};
