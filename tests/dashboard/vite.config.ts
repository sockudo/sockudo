import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [vue(), tailwindcss()],
  server: {
    port: 3456,
    open: true,
    proxy: {
      '/pusher': {
        target: 'http://localhost:3457',
        changeOrigin: true,
      },
    },
  },
})
