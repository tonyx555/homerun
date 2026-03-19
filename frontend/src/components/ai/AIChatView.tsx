import { useState, useCallback, useRef, useMemo } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useAtom } from 'jotai'
import {
  useLocalRuntime,
  AssistantRuntimeProvider,
  type ChatModelAdapter,
  ThreadPrimitive,
  ComposerPrimitive,
  MessagePrimitive,
} from '@assistant-ui/react'
import { MarkdownTextPrimitive } from '@assistant-ui/react-markdown'
import {
  MessageSquare,
  Plus,
  Send,
  Trash2,
  Bot,
  User,
  Pencil,
  Check,
  X,
  ChevronLeft,
  ChevronRight,
  Sparkles,
  ArrowDown,
} from 'lucide-react'
import { cn } from '../../lib/utils'
import { Button } from '../ui/button'
import { ScrollArea } from '../ui/scroll-area'
import {
  listAIChatSessions,
  archiveAIChatSession,
  renameChatSession,
  streamAIChat,
  type AIChatSession,
} from '../../services/api'
import { activeChatSessionIdAtom } from '../../store/atoms'

function groupSessionsByDate(sessions: AIChatSession[]): Record<string, AIChatSession[]> {
  const groups: Record<string, AIChatSession[]> = {}
  const now = new Date()
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate())
  const yesterday = new Date(today.getTime() - 86400000)
  const weekAgo = new Date(today.getTime() - 7 * 86400000)

  for (const session of sessions) {
    const date = session.updated_at ? new Date(session.updated_at) : session.created_at ? new Date(session.created_at) : null
    let group: string
    if (!date) {
      group = 'Older'
    } else if (date >= today) {
      group = 'Today'
    } else if (date >= yesterday) {
      group = 'Yesterday'
    } else if (date >= weekAgo) {
      group = 'This Week'
    } else {
      group = 'Older'
    }
    if (!groups[group]) groups[group] = []
    groups[group].push(session)
  }

  return groups
}

function SessionSidebar({
  activeSessionId,
  onSelectSession,
  onNewChat,
  collapsed,
  onToggleCollapse,
}: {
  activeSessionId: string | null
  onSelectSession: (id: string) => void
  onNewChat: () => void
  collapsed: boolean
  onToggleCollapse: () => void
}) {
  const queryClient = useQueryClient()
  const [renamingId, setRenamingId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')

  const { data: sessionsData } = useQuery({
    queryKey: ['ai-chat-sessions'],
    queryFn: () => listAIChatSessions({ limit: 100 }),
    refetchInterval: 15000,
  })

  const sessions = sessionsData?.sessions ?? []
  const grouped = useMemo(() => groupSessionsByDate(sessions), [sessions])
  const groupOrder = ['Today', 'Yesterday', 'This Week', 'Older']

  const handleArchive = async (sessionId: string, e: React.MouseEvent) => {
    e.stopPropagation()
    await archiveAIChatSession(sessionId)
    queryClient.invalidateQueries({ queryKey: ['ai-chat-sessions'] })
    if (activeSessionId === sessionId) {
      onNewChat()
    }
  }

  const startRename = (session: AIChatSession, e: React.MouseEvent) => {
    e.stopPropagation()
    setRenamingId(session.session_id)
    setRenameValue(session.title || '')
  }

  const commitRename = async (sessionId: string) => {
    const trimmed = renameValue.trim()
    if (trimmed) {
      await renameChatSession(sessionId, trimmed)
      queryClient.invalidateQueries({ queryKey: ['ai-chat-sessions'] })
    }
    setRenamingId(null)
  }

  if (collapsed) {
    return (
      <div className="w-10 border-r border-white/10 flex flex-col items-center py-3 gap-2 shrink-0">
        <button
          onClick={onToggleCollapse}
          className="p-1.5 rounded-md hover:bg-white/10 text-muted-foreground hover:text-foreground transition-colors"
        >
          <ChevronRight className="w-4 h-4" />
        </button>
        <button
          onClick={onNewChat}
          className="p-1.5 rounded-md hover:bg-purple-500/20 text-muted-foreground hover:text-purple-300 transition-colors"
        >
          <Plus className="w-4 h-4" />
        </button>
      </div>
    )
  }

  return (
    <div className="w-64 border-r border-white/10 flex flex-col shrink-0">
      <div className="p-3 flex items-center justify-between border-b border-white/5">
        <Button
          size="sm"
          onClick={onNewChat}
          className="h-8 gap-1.5 text-xs bg-purple-500/20 text-purple-300 border border-purple-500/30 hover:bg-purple-500/30"
        >
          <Plus className="w-3.5 h-3.5" />
          New Chat
        </Button>
        <button
          onClick={onToggleCollapse}
          className="p-1.5 rounded-md hover:bg-white/10 text-muted-foreground hover:text-foreground transition-colors"
        >
          <ChevronLeft className="w-4 h-4" />
        </button>
      </div>
      <ScrollArea className="flex-1">
        <div className="p-2 space-y-3">
          {groupOrder.map(group => {
            const items = grouped[group]
            if (!items?.length) return null
            return (
              <div key={group}>
                <p className="text-[10px] uppercase tracking-wider text-muted-foreground/60 px-2 mb-1.5">{group}</p>
                <div className="space-y-0.5">
                  {items.map(session => (
                    <div
                      key={session.session_id}
                      onClick={() => onSelectSession(session.session_id)}
                      className={cn(
                        'group flex items-center gap-2 px-2 py-2 rounded-lg cursor-pointer transition-colors',
                        activeSessionId === session.session_id
                          ? 'bg-purple-500/15 text-purple-200'
                          : 'hover:bg-white/5 text-muted-foreground hover:text-foreground'
                      )}
                    >
                      <MessageSquare className="w-3.5 h-3.5 shrink-0 opacity-60" />
                      {renamingId === session.session_id ? (
                        <div className="flex-1 flex items-center gap-1">
                          <input
                            value={renameValue}
                            onChange={e => setRenameValue(e.target.value)}
                            onKeyDown={e => {
                              if (e.key === 'Enter') commitRename(session.session_id)
                              if (e.key === 'Escape') setRenamingId(null)
                            }}
                            onClick={e => e.stopPropagation()}
                            autoFocus
                            className="flex-1 bg-transparent border-b border-purple-500/40 text-xs outline-none px-0.5"
                          />
                          <button onClick={(e) => { e.stopPropagation(); commitRename(session.session_id) }}>
                            <Check className="w-3 h-3 text-emerald-400" />
                          </button>
                          <button onClick={(e) => { e.stopPropagation(); setRenamingId(null) }}>
                            <X className="w-3 h-3 text-muted-foreground" />
                          </button>
                        </div>
                      ) : (
                        <>
                          <span className="flex-1 text-xs truncate">
                            {session.title || `Chat ${session.session_id.slice(0, 8)}`}
                          </span>
                          <div className="hidden group-hover:flex items-center gap-0.5">
                            <button
                              onClick={e => startRename(session, e)}
                              className="p-0.5 rounded hover:bg-white/10"
                            >
                              <Pencil className="w-3 h-3" />
                            </button>
                            <button
                              onClick={e => handleArchive(session.session_id, e)}
                              className="p-0.5 rounded hover:bg-red-500/20 text-red-400"
                            >
                              <Trash2 className="w-3 h-3" />
                            </button>
                          </div>
                        </>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )
          })}
          {sessions.length === 0 && (
            <div className="text-center py-8">
              <MessageSquare className="w-8 h-8 text-muted-foreground/20 mx-auto mb-2" />
              <p className="text-xs text-muted-foreground/50">No conversations yet</p>
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  )
}

function UserMessage() {
  return (
    <MessagePrimitive.Root className="flex gap-3 py-4 px-4">
      <div className="w-7 h-7 rounded-full bg-blue-500/20 border border-blue-500/30 flex items-center justify-center shrink-0 mt-0.5">
        <User className="w-3.5 h-3.5 text-blue-400" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-[10px] uppercase tracking-wider text-blue-400/70 mb-1">You</p>
        <MessagePrimitive.Parts
          components={{
            Text: () => (
              <div className="text-sm text-foreground/90 leading-relaxed whitespace-pre-wrap">
                <MarkdownTextPrimitive />
              </div>
            ),
          }}
        />
      </div>
    </MessagePrimitive.Root>
  )
}

function AssistantMessage() {
  return (
    <MessagePrimitive.Root className="flex gap-3 py-4 px-4 bg-white/[0.02]">
      <div className="w-7 h-7 rounded-full bg-purple-500/20 border border-purple-500/30 flex items-center justify-center shrink-0 mt-0.5">
        <Bot className="w-3.5 h-3.5 text-purple-400" />
      </div>
      <div className="flex-1 min-w-0">
        <p className="text-[10px] uppercase tracking-wider text-purple-400/70 mb-1">Homerun AI</p>
        <MessagePrimitive.Parts
          components={{
            Text: () => (
              <div className="text-sm text-foreground/90 leading-relaxed prose prose-invert prose-sm max-w-none prose-p:my-2 prose-headings:my-3 prose-li:my-0.5 prose-pre:bg-background/80 prose-pre:border prose-pre:border-border/40 prose-code:text-purple-300 prose-code:text-xs">
                <MarkdownTextPrimitive />
              </div>
            ),
          }}
        />
      </div>
    </MessagePrimitive.Root>
  )
}

function ChatThread() {
  return (
    <ThreadPrimitive.Root className="flex flex-col h-full">
      <ThreadPrimitive.Viewport className="flex-1 overflow-y-auto">
        <ThreadPrimitive.Empty>
          <WelcomeScreen />
        </ThreadPrimitive.Empty>
        <ThreadPrimitive.Messages
          components={{
            UserMessage,
            AssistantMessage,
          }}
        />
        <ThreadPrimitive.ViewportFooter className="sticky bottom-0 flex justify-center pb-2">
          <ThreadPrimitive.ScrollToBottom asChild>
            <button className="p-2 rounded-full bg-purple-500/20 border border-purple-500/30 text-purple-300 hover:bg-purple-500/30 transition-colors shadow-lg">
              <ArrowDown className="w-4 h-4" />
            </button>
          </ThreadPrimitive.ScrollToBottom>
        </ThreadPrimitive.ViewportFooter>
      </ThreadPrimitive.Viewport>

      <div className="border-t border-white/10 p-4">
        <ComposerPrimitive.Root className="flex items-end gap-2 rounded-xl border border-white/10 bg-white/[0.03] p-2 focus-within:border-purple-500/40 transition-colors">
          <ComposerPrimitive.Input
            placeholder="Ask about markets, strategies, opportunities..."
            className="flex-1 bg-transparent text-sm text-foreground resize-none outline-none min-h-[36px] max-h-[120px] px-2 py-1.5 placeholder:text-muted-foreground/50"
            autoFocus
          />
          <ComposerPrimitive.Send asChild>
            <Button
              size="sm"
              className="h-8 w-8 p-0 bg-purple-500 hover:bg-purple-600 text-white rounded-lg shrink-0"
            >
              <Send className="w-3.5 h-3.5" />
            </Button>
          </ComposerPrimitive.Send>
        </ComposerPrimitive.Root>
        <p className="text-[10px] text-muted-foreground/40 text-center mt-2">
          AI responses are not financial advice. Always verify before trading.
        </p>
      </div>
    </ThreadPrimitive.Root>
  )
}

function WelcomeScreen() {
  return (
    <div className="flex flex-col items-center justify-center h-full px-8 py-12">
      <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-purple-500/20 to-blue-500/20 border border-purple-500/20 flex items-center justify-center mb-6">
        <Sparkles className="w-8 h-8 text-purple-400" />
      </div>
      <h2 className="text-xl font-semibold text-foreground mb-2">Homerun AI</h2>
      <p className="text-sm text-muted-foreground text-center max-w-md mb-8">
        Your prediction market copilot. Ask about opportunities, analyze markets, get strategy recommendations, or explore resolution criteria.
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 w-full max-w-lg">
        {[
          { label: 'Analyze top opportunities', icon: '📊' },
          { label: 'Explain resolution risks', icon: '🔍' },
          { label: 'Compare market spreads', icon: '📈' },
          { label: 'Review portfolio exposure', icon: '🎯' },
        ].map(item => (
          <ThreadPrimitive.Suggestion
            key={item.label}
            prompt={item.label}
            method="replace"
            autoSend
          >
            <button className="flex items-center gap-3 px-4 py-3 rounded-xl border border-white/10 bg-white/[0.03] hover:bg-white/[0.06] hover:border-purple-500/20 transition-colors text-left">
              <span className="text-lg">{item.icon}</span>
              <span className="text-sm text-muted-foreground">{item.label}</span>
            </button>
          </ThreadPrimitive.Suggestion>
        ))}
      </div>
    </div>
  )
}

export default function AIChatView() {
  const [activeSessionId, setActiveSessionId] = useAtom(activeChatSessionIdAtom)
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)
  const queryClient = useQueryClient()
  const sessionIdRef = useRef<string | null>(activeSessionId)
  sessionIdRef.current = activeSessionId

  const chatModelAdapter = useMemo<ChatModelAdapter>(() => ({
    async *run({ messages, abortSignal }) {
      const lastUserMessage = [...messages].reverse().find(m => m.role === 'user')
      if (!lastUserMessage) return

      const userText = lastUserMessage.content
        .filter((p): p is { type: 'text'; text: string } => p.type === 'text')
        .map(p => p.text)
        .join('\n')

      let accumulated = ''

      const tokenPromise = new Promise<void>((resolve, reject) => {
        streamAIChat(
          {
            message: userText,
            session_id: sessionIdRef.current || undefined,
          },
          (text) => {
            accumulated += text
          },
          (data) => {
            if (data.session_id && data.session_id !== sessionIdRef.current) {
              setActiveSessionId(data.session_id)
            }
            queryClient.invalidateQueries({ queryKey: ['ai-chat-sessions'] })
            resolve()
          },
          (error) => {
            reject(new Error(error))
          },
          abortSignal,
        )
      })

      // Poll accumulated text and yield updates
      const pollInterval = 50
      let lastYielded = ''
      const startTime = Date.now()
      const maxWait = 120_000

      while (true) {
        await new Promise(r => setTimeout(r, pollInterval))

        if (accumulated !== lastYielded) {
          lastYielded = accumulated
          yield {
            content: [{ type: 'text' as const, text: accumulated }],
          }
        }

        // Check if the stream is done
        try {
          // Use Promise.race to check if done without blocking
          const result = await Promise.race([
            tokenPromise.then(() => 'done' as const),
            new Promise<'pending'>(r => setTimeout(() => r('pending'), 10)),
          ])
          if (result === 'done') {
            // yield final state
            if (accumulated !== lastYielded) {
              yield {
                content: [{ type: 'text' as const, text: accumulated }],
              }
            }
            return
          }
        } catch (err) {
          // Stream errored — yield what we have, then throw
          if (accumulated) {
            yield {
              content: [{ type: 'text' as const, text: accumulated }],
            }
          }
          throw err
        }

        if (Date.now() - startTime > maxWait) {
          if (accumulated) {
            yield {
              content: [{ type: 'text' as const, text: accumulated }],
            }
          }
          return
        }
      }
    },
  }), [setActiveSessionId, queryClient])

  const runtime = useLocalRuntime(chatModelAdapter)

  const handleNewChat = useCallback(() => {
    setActiveSessionId(null)
    // Runtime will naturally start fresh on next message since sessionId is null
  }, [setActiveSessionId])

  const handleSelectSession = useCallback(async (sessionId: string) => {
    setActiveSessionId(sessionId)
  }, [setActiveSessionId])

  return (
    <div className="flex h-full">
      <SessionSidebar
        activeSessionId={activeSessionId}
        onSelectSession={handleSelectSession}
        onNewChat={handleNewChat}
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed(!sidebarCollapsed)}
      />
      <div className="flex-1 flex flex-col min-w-0">
        <AssistantRuntimeProvider runtime={runtime}>
          <ChatThread />
        </AssistantRuntimeProvider>
      </div>
    </div>
  )
}
