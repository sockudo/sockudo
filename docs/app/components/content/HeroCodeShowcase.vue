<script setup lang="ts">
import { computed, ref } from "vue";

type SnippetId = "docker" | "binstall" | "source";

const snippets = {
    docker: {
        id: "docker",
        label: "docker compose",
        language: "YAML",
        accent:
            "linear-gradient(135deg, rgba(168, 85, 247, 0.28), rgba(34, 211, 238, 0.22))",
        code: `services:
  sockudo:
    image: ghcr.io/sockudo/sockudo:latest
    ports: ["6001:6001"]
    volumes: ["./config.toml:/app/config.toml"]`,
        summary: "Run docker compose up -d to start Sockudo on :6001.",
    },
    binstall: {
        id: "binstall",
        label: "cargo binstall",
        language: "Bash",
        accent:
            "linear-gradient(135deg, rgba(56, 189, 248, 0.24), rgba(45, 212, 191, 0.22))",
        code: `cargo binstall sockudo
sockudo --config config.toml

# websocket server
# listening on :6001`,
        summary: "Install a precompiled binary and start with your config file.",
    },
    source: {
        id: "source",
        label: "build from source",
        language: "Bash",
        accent:
            "linear-gradient(135deg, rgba(251, 191, 36, 0.26), rgba(251, 113, 133, 0.22))",
        code: `git clone https://github.com/sockudo/sockudo.git
cd sockudo
cargo build --release
./target/release/sockudo --config config.toml`,
        summary: "Compile with Cargo when you want full control over features.",
    },
} satisfies Record<
    SnippetId,
    {
        id: SnippetId;
        label: string;
        language: string;
        accent: string;
        code: string;
        summary: string;
    }
>;

const order: SnippetId[] = ["docker", "binstall", "source"];
const activeSnippet = ref<SnippetId>("docker");

const currentSnippet = computed(() => snippets[activeSnippet.value]);
</script>

<template>
    <div class="hero-snippet-shell">
        <div class="hero-snippet-frame">
            <div class="hero-snippet-topbar">
                <div class="hero-snippet-pill">
                    Install in minutes
                </div>
            </div>

            <div class="hero-snippet-tabs" role="tablist" aria-label="Code examples">
                <button
                    v-for="id in order"
                    :key="id"
                    type="button"
                    class="hero-snippet-tab"
                    :class="{
                        'hero-snippet-tab-active': activeSnippet === id,
                    }"
                    :aria-selected="activeSnippet === id"
                    @click="activeSnippet = id"
                >
                    {{ snippets[id].label }}
                </button>
            </div>

            <div class="hero-snippet-intro">
                <div>
                    <p class="hero-snippet-kicker">Choose an install path</p>
                    <p class="hero-snippet-summary">
                        {{ currentSnippet.summary }}
                    </p>
                </div>
            </div>

            <CodePanel
                :code="currentSnippet.code"
                :language="currentSnippet.language"
                :label="currentSnippet.label"
                :show-header="false"
                :show-gutter="false"
            />
        </div>
    </div>
</template>

<style scoped>
.hero-snippet-shell {
    position: relative;
    width: 100%;
}

.hero-snippet-shell::before {
    content: "";
    position: absolute;
    inset: -2rem -1rem auto;
    height: 14rem;
    background:
        radial-gradient(circle at top, rgba(121, 56, 211, 0.34), transparent 60%);
    filter: blur(24px);
    pointer-events: none;
}

.hero-snippet-frame {
    position: relative;
    overflow: hidden;
    border: 1px solid rgba(167, 139, 250, 0.18);
    border-radius: 1.75rem;
    background:
        linear-gradient(180deg, rgba(17, 24, 39, 0.98), rgba(6, 11, 18, 0.98));
    box-shadow:
        0 30px 80px rgba(3, 8, 18, 0.52),
        inset 0 1px 0 rgba(255, 255, 255, 0.05);
}

.hero-snippet-topbar,
.hero-snippet-tabs,
.hero-snippet-intro {
    position: relative;
    z-index: 1;
}

.hero-snippet-topbar {
    display: flex;
    justify-content: flex-end;
    padding: 1rem 1.6rem 0 1.25rem;
}

.hero-snippet-pill {
    border: 1px solid rgba(148, 163, 184, 0.16);
    border-radius: 999px;
    padding: 0.3rem 0.65rem;
    background: rgba(15, 23, 42, 0.72);
    color: rgba(203, 213, 225, 0.88);
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
}

.hero-snippet-tabs {
    display: flex;
    gap: 0.75rem;
    padding: 1rem 1.6rem 0.25rem 1.25rem;
    overflow-x: auto;
}

.hero-snippet-tab {
    flex: 0 0 auto;
    border: 1px solid rgba(148, 163, 184, 0.14);
    border-radius: 999px;
    padding: 0.5rem 0.85rem;
    background: rgba(15, 23, 42, 0.58);
    color: rgba(191, 219, 254, 0.78);
    font-size: 0.82rem;
    font-weight: 600;
    transition:
        border-color 0.2s ease,
        color 0.2s ease,
        background 0.2s ease,
        transform 0.2s ease;
}

.hero-snippet-tab:hover {
    transform: translateY(-1px);
    border-color: rgba(167, 139, 250, 0.34);
    color: rgba(255, 255, 255, 0.92);
}

.hero-snippet-tab-active {
    border-color: rgba(168, 85, 247, 0.42);
    background:
        linear-gradient(180deg, rgba(91, 33, 182, 0.48), rgba(30, 41, 59, 0.7));
    color: #fff;
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.06);
}

.hero-snippet-intro {
    padding: 0.9rem 1.6rem 0.4rem 1.25rem;
}

.hero-snippet-kicker {
    margin: 0 0 0.45rem;
    color: rgba(148, 163, 184, 0.8);
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
}

.hero-snippet-summary {
    margin: 0;
    max-width: 34rem;
    color: rgba(226, 232, 240, 0.9);
    font-size: 0.92rem;
    line-height: 1.55;
}

:deep(.code-panel) {
    margin: 0 1.6rem 1.25rem 1.25rem;
    background:
        linear-gradient(180deg, rgba(5, 10, 27, 0.98), rgba(10, 16, 32, 0.98));
    border-color: rgba(71, 85, 105, 0.6);
}

@media (max-width: 640px) {
    .hero-snippet-topbar,
    .hero-snippet-tabs,
    .hero-snippet-intro {
        padding-left: 0.9rem;
        padding-right: 0.9rem;
    }

    :deep(.code-panel) {
        margin-left: 0.9rem;
        margin-right: 0.9rem;
        margin-bottom: 0.9rem;
    }
}
</style>
