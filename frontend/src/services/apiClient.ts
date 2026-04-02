import axios from 'axios'
import { normalizeUtcTimestampsInPlace } from '../lib/timestamps'

export const api = axios.create({
  baseURL: '/api',
  timeout: 60000,
})

api.interceptors.response.use(
  (response) => {
    normalizeUtcTimestampsInPlace(response.data)
    const count = Array.isArray(response.data) ? response.data.length : '?'
    console.debug(`[API] ${response.config.method?.toUpperCase()} ${response.config.url} -> ${response.status} (${count} items)`)
    return response
  },
  (error) => {
    const status = error?.response?.status
    if (status === 423 && typeof window !== 'undefined') {
      window.dispatchEvent(new CustomEvent('ui-lock-required'))
    }
    if (status !== 423) {
      console.error(
        `[API] ${error.config?.method?.toUpperCase()} ${error.config?.url} -> ${status || error.message}`,
        error.response?.data
      )
    }
    return Promise.reject(error)
  }
)

export function unwrapApiData(data: any): any {
  if (Array.isArray(data)) {
    return data
  }
  if (data && typeof data === 'object' && 'items' in data) {
    return data.items
  }
  return data
}

export function unwrapStrategyManagerPayload(data: any): any {
  const first = unwrapApiData(data)
  if (first && typeof first === 'object' && !Array.isArray(first) && 'data' in first) {
    return unwrapApiData((first as any).data)
  }
  return first
}

export function getStrategyManagerItems(data: any): any[] {
  const payload = unwrapStrategyManagerPayload(data)
  if (Array.isArray(payload)) {
    return payload
  }
  if (payload && typeof payload === 'object' && Array.isArray((payload as any).items)) {
    return (payload as any).items
  }
  return []
}
