/**
 * OpenUI renderer for generative AI tool results.
 *
 * Wraps the OpenUI Renderer component to display rich, LLM-generated UI
 * inside chat messages. Used by the assistant-ui tool UI system to render
 * structured tool results as interactive cards, tables, gauges, etc.
 *
 * The backend can emit OpenUI Lang in tool results, and this component
 * renders it using the homerunLibrary component definitions.
 */

import { Renderer, useIsStreaming } from '@openuidev/react-lang'
import { homerunLibrary } from './openui-library'

interface OpenUIRendererProps {
  /** OpenUI Lang response text to render */
  content: string
  /** Whether the content is still being streamed */
  streaming?: boolean
}

export default function OpenUIToolRenderer({ content, streaming = false }: OpenUIRendererProps) {
  if (!content || content.trim().length === 0) {
    return null
  }

  return (
    <div className="openui-tool-result my-2">
      <Renderer
        library={homerunLibrary}
        response={content}
        isStreaming={streaming}
      />
    </div>
  )
}

/**
 * Hook to check if the current OpenUI render is streaming.
 * Re-export for convenience in custom components.
 */
export { useIsStreaming }
