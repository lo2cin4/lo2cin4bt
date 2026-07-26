type AxisConfig = Record<string, unknown>
type LayoutConfig = Record<string, unknown>

const CHAMPAGNE = '#e8b97d'
const TEXT_MAIN = '#e7eefc'
const TEXT_DIM = '#94a3b8'
const AXIS_TITLE_FONT = { color: CHAMPAGNE, size: 13 }
const AXIS_TICK_FONT = { color: TEXT_DIM, size: 11 }
const GRID_COLOR = 'rgba(45, 226, 230, 0.10)'
const AXIS_LINE_COLOR = 'rgba(45, 226, 230, 0.24)'

export const plotConfig = { displaylogo: false, responsive: true }

export function makeChartLayout(options: {
  xTitle?: string
  yTitle?: string
  xaxis?: AxisConfig
  yaxis?: AxisConfig
  margin?: Record<string, number>
  legend?: LayoutConfig
  [key: string]: unknown
} = {}) {
  const { xTitle, yTitle, xaxis = {}, yaxis = {}, margin, legend, ...rest } = options
  return {
    template: 'plotly_dark',
    paper_bgcolor: 'rgba(5, 7, 10, 0)',
    plot_bgcolor: 'rgba(9, 14, 24, 0.72)',
    font: { color: TEXT_MAIN, family: 'Inter, Noto Sans TC, Segoe UI, sans-serif' },
    margin: margin || { l: 64, r: 24, t: 24, b: 58 },
    hovermode: 'closest',
    hoverlabel: {
      bgcolor: 'rgba(7, 16, 26, 0.96)',
      bordercolor: '#2de2e6',
      font: { color: TEXT_MAIN, size: 12 },
    },
    legend: {
      font: { color: TEXT_DIM, size: 11 },
      bgcolor: 'rgba(0, 0, 0, 0)',
      ...legend,
    },
    xaxis: {
      title: xTitle ? { text: xTitle, font: AXIS_TITLE_FONT, standoff: 10 } : undefined,
      tickfont: AXIS_TICK_FONT,
      gridcolor: GRID_COLOR,
      linecolor: AXIS_LINE_COLOR,
      zerolinecolor: 'rgba(232, 185, 125, 0.22)',
      automargin: true,
      ...xaxis,
    },
    yaxis: {
      title: yTitle ? { text: yTitle, font: AXIS_TITLE_FONT, standoff: 10 } : undefined,
      tickfont: AXIS_TICK_FONT,
      gridcolor: GRID_COLOR,
      linecolor: AXIS_LINE_COLOR,
      zerolinecolor: 'rgba(232, 185, 125, 0.22)',
      automargin: true,
      ...yaxis,
    },
    ...rest,
  }
}
