import { lazy, Suspense } from 'react'
import { useAtom } from 'jotai'
import { aiTabSubtabAtom } from '../../store/atoms'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '../ui/tabs'
import { RefreshCw } from 'lucide-react'
import AIChatView from './AIChatView'
import AIAgentsView from './AIAgentsView'
import AIToolsView from './AIToolsView'
import AISystemView from './AISystemView'

// Lazy load the new subtabs to avoid bloating initial bundle
const AIProvidersView = lazy(() => import('./AIProvidersView'))
const AIModelsView = lazy(() => import('./AIModelsView'))
const AIActivityView = lazy(() => import('./AIActivityView'))

function SubtabFallback() {
  return (
    <div className="flex items-center justify-center py-16">
      <RefreshCw className="w-5 h-5 animate-spin text-purple-400" />
    </div>
  )
}

export default function AITab() {
  const [subtab, setSubtab] = useAtom(aiTabSubtabAtom)

  return (
    <div className="flex flex-col h-full">
      <Tabs value={subtab} onValueChange={(v) => setSubtab(v as typeof subtab)} className="flex flex-col h-full">
        <div className="border-b border-white/10 px-4">
          <TabsList className="bg-transparent gap-1">
            <TabsTrigger value="chat" className="data-[state=active]:bg-purple-500/20 data-[state=active]:text-purple-300">Chat</TabsTrigger>
            <TabsTrigger value="agents" className="data-[state=active]:bg-cyan-500/20 data-[state=active]:text-cyan-300">Agents</TabsTrigger>
            <TabsTrigger value="tools" className="data-[state=active]:bg-violet-500/20 data-[state=active]:text-violet-300">Tools</TabsTrigger>
            <TabsTrigger value="providers" className="data-[state=active]:bg-blue-500/20 data-[state=active]:text-blue-300">Providers</TabsTrigger>
            <TabsTrigger value="models" className="data-[state=active]:bg-amber-500/20 data-[state=active]:text-amber-300">Models</TabsTrigger>
            <TabsTrigger value="activity" className="data-[state=active]:bg-emerald-500/20 data-[state=active]:text-emerald-300">Activity</TabsTrigger>
            <TabsTrigger value="system" className="data-[state=active]:bg-rose-500/20 data-[state=active]:text-rose-300">System</TabsTrigger>
          </TabsList>
        </div>
        <TabsContent value="chat" className="flex-1 overflow-hidden mt-0"><AIChatView /></TabsContent>
        <TabsContent value="agents" className="flex-1 overflow-auto mt-0 p-4"><AIAgentsView /></TabsContent>
        <TabsContent value="tools" className="flex-1 overflow-auto mt-0 p-4"><AIToolsView /></TabsContent>
        <TabsContent value="providers" className="flex-1 overflow-auto mt-0 p-4">
          <Suspense fallback={<SubtabFallback />}><AIProvidersView /></Suspense>
        </TabsContent>
        <TabsContent value="models" className="flex-1 overflow-auto mt-0 p-4">
          <Suspense fallback={<SubtabFallback />}><AIModelsView /></Suspense>
        </TabsContent>
        <TabsContent value="activity" className="flex-1 overflow-auto mt-0 p-4">
          <Suspense fallback={<SubtabFallback />}><AIActivityView /></Suspense>
        </TabsContent>
        <TabsContent value="system" className="flex-1 overflow-auto mt-0 p-4"><AISystemView /></TabsContent>
      </Tabs>
    </div>
  )
}
