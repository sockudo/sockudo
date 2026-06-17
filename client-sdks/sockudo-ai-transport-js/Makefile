.PHONY: demo demo-nuxt demo-build docs local-nuxt

demo: demo-nuxt

demo-nuxt:
	docker compose -f demo/docker-compose.yml up --build sockudo nuxt

local-nuxt:
	pnpm --filter @sockudo/ai-transport-demo-nuxt dev

demo-build:
	pnpm demo:build

docs:
	pnpm docs:build
