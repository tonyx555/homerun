import { defineConfig, createLogger } from 'vite'
import react from '@vitejs/plugin-react'

// Suppress benign ECONNRESET/EPIPE errors from the WebSocket proxy.
// These occur normally when WebSocket connections close during page
// reloads or HMR and are logged by Vite's internal upgrade handler
// before any user-space configure() callback can intercept them.
const logger = createLogger()
const loggerError = logger.error.bind(logger)
logger.error = (msg, options) => {
  if (
    msg.includes('ws proxy socket error') &&
    (msg.includes('ECONNRESET') || msg.includes('ECONNABORTED') || msg.includes('EPIPE') || msg.includes('ETIMEDOUT'))
  ) {
    return
  }
  loggerError(msg, options)
}

const vitePort = Number(process.env.VITE_PORT || 3000)
const viteStrictPort = String(process.env.VITE_STRICT_PORT || "").toLowerCase() === "true"
const viteHost = process.env.VITE_HOST || undefined

export default defineConfig({
  plugins: [react()],
  customLogger: logger,
  optimizeDeps: {
    esbuildOptions: {
      target: 'es2022',
    },
  },
  server: {
    host: viteHost,
    port: vitePort,
    strictPort: viteStrictPort,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/ws': {
        target: 'ws://localhost:8000',
        ws: true,
        configure: (proxy) => {
          proxy.on('error', (err) => {
            if (
              err.message.includes('EPIPE') ||
              err.message.includes('ECONNRESET') ||
              err.message.includes('ECONNABORTED') ||
              err.message.includes('ETIMEDOUT')
            ) {
              return
            }
            console.error('WebSocket proxy error:', err.message)
          })
        },
      },
    },
  },
})
