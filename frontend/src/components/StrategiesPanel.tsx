import UnifiedStrategiesManager from './UnifiedStrategiesManager'

type StrategiesPanelProps = {
  initialSourceFilter?: string | null
  onSourceFilterApplied?: () => void
}

export default function StrategiesPanel({
  initialSourceFilter,
  onSourceFilterApplied,
}: StrategiesPanelProps) {
  return (
    <div className="h-full min-h-0">
      <UnifiedStrategiesManager
        initialSourceFilter={initialSourceFilter}
        onSourceFilterApplied={onSourceFilterApplied}
      />
    </div>
  )
}
