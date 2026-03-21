/**
 * OpenUI component library for generative AI chat responses.
 *
 * Extends the standard @openuidev/react-ui library (50+ components) with
 * Homerun-specific custom components like MarketCard.
 *
 * Usage:
 *   import { homerunLibrary } from './openui-library'
 *   <Renderer library={homerunLibrary} response={openUILangText} isStreaming={isStreaming} />
 */

import { createLibrary } from '@openuidev/react-lang'
import { openuiLibrary, openuiPromptOptions } from '@openuidev/react-ui/genui-lib'
import { MarketCard } from './MarketCard'

export const homerunLibrary = createLibrary({
  components: [...Object.values(openuiLibrary.components), MarketCard],
  componentGroups: openuiLibrary.componentGroups,
  root: openuiLibrary.root,
})

export { openuiPromptOptions }
