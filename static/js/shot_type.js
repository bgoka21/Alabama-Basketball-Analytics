// Discrete heatmap color helpers for ATR, 2FG and 3FG percentages

function getATRColor(atr) {
  if (atr === 0)                     return '#FFFFFF';
  if (atr > 0   && atr <  35.1)      return '#FF425C';
  if (atr > 35  && atr <  40.1)      return '#FF708A';
  if (atr > 40  && atr <  45.1)      return '#FF9EB8';
  if (atr > 45  && atr <  50.1)      return '#FFFFD9';
  if (atr > 50  && atr <  55.1)      return '#FFFFB3';
  if (atr > 55  && atr <  60.1)      return '#FFFF80';
  if (atr > 60  && atr <  65.1)      return '#B5FFCF';
  if (atr > 65  && atr <  70.1)      return '#9EFFB8';
  if (atr > 70  && atr <  75.1)      return '#87FFA1';
  if (atr > 75)                      return '#00FF00';
  return '#FFFFFF';
}

function get2FGColor(fg2) {
  if (fg2 === 0)                     return '#FFFFFF';
  if (fg2 > 0   && fg2 <  35.1)      return '#FF425C';
  if (fg2 > 35  && fg2 <  40.1)      return '#FF708A';
  if (fg2 > 40  && fg2 <  45.1)      return '#FF9EB8';
  if (fg2 > 45  && fg2 <  50.1)      return '#FFFFD9';
  if (fg2 > 50  && fg2 <  55.1)      return '#FFFFB3';
  if (fg2 > 55  && fg2 <  60.1)      return '#FFFF80';
  if (fg2 > 60  && fg2 <  65.1)      return '#B5FFCF';
  if (fg2 > 65  && fg2 <  70.1)      return '#9EFFB8';
  if (fg2 > 70  && fg2 <  75.1)      return '#87FFA1';
  if (fg2 > 75)                      return '#00FF00';
  return '#FFFFFF';
}

function get3FGColor(fg3) {
  if (fg3 === 0)                     return '#FFFFFF';
  if (fg3 > 0   && fg3 <  18.1)      return '#FF425C';
  if (fg3 > 18  && fg3 <  21.1)      return '#FF708A';
  if (fg3 > 21  && fg3 <  24.1)      return '#FF9EB8';
  if (fg3 > 24  && fg3 <  27.1)      return '#FFFFD9';
  if (fg3 > 27  && fg3 <  30.1)      return '#FFFFB3';
  if (fg3 > 30  && fg3 <  33.1)      return '#FFFF80';
  if (fg3 > 33  && fg3 <  36.1)      return '#CCFFE6';
  if (fg3 > 36  && fg3 <  39.1)      return '#B5FFCF';
  if (fg3 > 39  && fg3 <  42.1)      return '#9EFFB8';
  if (fg3 > 42  && fg3 <  45.1)      return '#87FFA1';
  if (fg3 > 45)                      return '#00FF00';
  return '#FFFFFF';
}
