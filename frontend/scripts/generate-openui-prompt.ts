/**
 * Generate the OpenUI Lang system prompt from the standard library.
 *
 * This runs in Vite's context (via `npx vite-node`) so React component
 * imports resolve correctly.  If vite-node is unavailable, it falls back
 * to dumping the prompt from a running dev server.
 *
 * Run: cd frontend && npx vite-node scripts/generate-openui-prompt.ts
 * Output: ../backend/services/ai/openui_prompt.txt
 */

import { homerunLibrary, openuiPromptOptions } from '../src/components/ai/openui-library'
import { writeFileSync } from 'fs'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const prompt = homerunLibrary.prompt({
  preamble:
    'You can use openui-lang to render rich interactive UI components inline in your responses. ' +
    'When presenting structured data (market analysis, portfolio summaries, strategy performance, ' +
    'leaderboards, charts, tables, etc.), wrap your openui-lang code in a ```openui fenced code block. ' +
    'You can mix openui-lang blocks with regular markdown text freely. ' +
    'IMPORTANT: openui-lang blocks go inside ```openui fenced code blocks, not as raw text.',
  examples: openuiPromptOptions.examples,
  additionalRules: [
    ...(openuiPromptOptions.additionalRules || []),
    'ALWAYS wrap openui-lang in ```openui fenced code blocks — never output raw openui-lang',
    'Use markdown for explanatory text, openui-lang for structured data visualization',
    'ALWAYS use root = Stack([...]) as the entry point',
    'Use Card for grouping related data, Table for tabular data',
    'Use BarChart/LineChart/PieChart for data visualization',
    'Use CardHeader for card titles, TextContent for styled text',
    'Use TagBlock for status indicators and labels',
    'Use Callout for important alerts or recommendations',
    'You MAY include markdown text before and after openui-lang blocks',
    'When you have structured data from tool calls, ALWAYS present it using openui-lang components',
  ],
})

const outPath = resolve(__dirname, '../../backend/services/ai/openui_prompt.txt')
writeFileSync(outPath, prompt, 'utf-8')
console.log(`Wrote prompt (${prompt.length} chars) to ${outPath}`)
console.log('\n--- Preview (first 2000 chars) ---\n')
console.log(prompt.slice(0, 2000))
