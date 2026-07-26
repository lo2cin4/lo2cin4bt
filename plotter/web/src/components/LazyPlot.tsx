import { lazy, Suspense } from 'react'
import type { ComponentType } from 'react'

function resolvePlotlyComponent(module: any): ComponentType<any> {
  let candidate = module
  while (candidate && typeof candidate === 'object' && 'default' in candidate) {
    candidate = candidate.default
  }
  return candidate as ComponentType<any>
}

const loadPlotlyChart = () =>
  import('react-plotly.js').then((module) => ({
    default: resolvePlotlyComponent(module),
  }))

const PlotlyChart = lazy(loadPlotlyChart)

export function preloadPlotly() {
  void loadPlotlyChart()
}

export function Plot(props: any) {
  return (
    <Suspense fallback={<div className="chart-loading" />}>
      <PlotlyChart {...props} />
    </Suspense>
  )
}
