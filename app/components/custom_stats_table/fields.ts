export type OnOffLine = {
  possessions_on: number;
  team_possessions: number;
  percent_of_team_possessions: number;
  ppp_on: number;
  ppp_off: number;
  leverage: number;
};

export type OnOffSummary = {
  offense: OnOffLine;
  defense: OnOffLine;
};

export type FieldDescriptor = {
  key: string;
  label: string;
  accessor: (summary: OnOffSummary) => string;
};

/**
 * Field definitions for the custom on/off summary panel. Each accessor formats the
 * value already provided by the backend `get_on_off_summary` helper—no local
 * recalculation happens here.
 */
export const customStatsFields: FieldDescriptor[] = [
  {
    key: 'offensive_ppp',
    label: 'Offensive PPP',
    accessor: (summary) => summary.offense.ppp_on.toFixed(2),
  },
  {
    key: 'defensive_ppp',
    label: 'Defensive PPP',
    accessor: (summary) => summary.defense.ppp_on.toFixed(2),
  },
  {
    key: 'offensive_leverage',
    label: 'Offensive Leverage',
    accessor: (summary) => summary.offense.leverage.toFixed(2),
  },
  {
    key: 'defensive_leverage',
    label: 'Defensive Leverage',
    accessor: (summary) => summary.defense.leverage.toFixed(2),
  },
  {
    key: 'percent_possessions',
    label: '% Possessions',
    accessor: (summary) => `${(summary.offense.percent_of_team_possessions * 100).toFixed(1)}%`,
  },
];
