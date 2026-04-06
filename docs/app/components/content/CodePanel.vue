<script setup lang="ts">
import { computed } from "vue";

const props = withDefaults(
    defineProps<{
        code: string;
        language?: string;
        label?: string;
        compact?: boolean;
        showHeader?: boolean;
        showGutter?: boolean;
    }>(),
    {
        language: "JavaScript",
        label: "",
        compact: false,
        showHeader: true,
        showGutter: true,
    },
);

type Token = {
    text: string;
    className?: string;
};

type TokenRule = {
    className: string;
    regex: RegExp;
};

function tokenizeLine(line: string, language: string) {
    const lowerLanguage = language.toLowerCase();
    const rules: TokenRule[] =
        lowerLanguage === "bash"
            ? [
                  { className: "tok-string", regex: /^"(?:[^"\\]|\\.)*"/ },
                  { className: "tok-string", regex: /^'(?:[^'\\]|\\.)*'/ },
                  { className: "tok-link", regex: /^(?:https?:\/\/\S+)/ },
                  { className: "tok-option", regex: /^(?:-X|-H|-d|--[a-z-]+)/ },
                  { className: "tok-keyword", regex: /^\$/ },
                  { className: "tok-keyword", regex: /^(?:curl|import|from)\b/ },
                  { className: "tok-type", regex: /^(?:\[INFO\])/ },
                  {
                      className: "tok-punctuation",
                      regex: /^(?:=>|[=,:;.[\]{}()])/,
                  },
                  { className: "tok-muted", regex: /^\\/ },
              ]
            : [
                  { className: "tok-string", regex: /^"(?:[^"\\]|\\.)*"/ },
                  { className: "tok-keyword", regex: /^(?:import|from|const|new|true|false)\b/ },
                  { className: "tok-type", regex: /^(?:Sockudo|Pusher|Filter)\b/ },
                  { className: "tok-function", regex: /^(?:bind|subscribe|and|eq|gte|log)\b/ },
                  { className: "tok-number", regex: /^(?:\d+)\b/ },
                  { className: "tok-property", regex: /^(?:[A-Za-z_$][\w$]*)(?=:)/ },
                  {
                      className: "tok-punctuation",
                      regex: /^(?:=>|[=,:;.[\]{}()])/,
                  },
                  { className: "tok-muted", regex: /^\\/ },
              ];

    const tokens: Token[] = [];
    let remaining = line;

    while (remaining.length > 0) {
        let matched = false;

        for (const rule of rules) {
            const result = remaining.match(rule.regex);

            if (!result) {
                continue;
            }

            tokens.push({
                text: result[0],
                className: rule.className,
            });
            remaining = remaining.slice(result[0].length);
            matched = true;
            break;
        }

        if (!matched) {
            tokens.push({ text: remaining[0] });
            remaining = remaining.slice(1);
        }
    }

    return tokens;
}

const lines = computed(() => props.code.split("\n"));
const highlightedLines = computed(() =>
    lines.value.map((line) => tokenizeLine(line, props.language)),
);
</script>

<template>
    <div class="code-panel" :class="{ 'code-panel-compact': compact }">
        <div v-if="showHeader && (label || language)" class="code-panel-topbar">
            <div v-if="label" class="code-panel-label">{{ label }}</div>
            <div v-if="language" class="code-panel-language">{{ language }}</div>
        </div>

        <div
            class="code-panel-body"
            :class="{ 'code-panel-body-no-gutter': !showGutter }"
        >
            <div
                v-if="showGutter"
                class="code-panel-gutter"
                aria-hidden="true"
            >
                <span v-for="(_, index) in lines" :key="`gutter-${index}`">
                    {{ index + 1 }}
                </span>
            </div>

            <pre class="code-panel-pre"><code><span
                v-for="(line, index) in highlightedLines"
                :key="`line-${index}`"
                class="code-panel-line"
            ><template
                v-for="(token, tokenIndex) in line"
                :key="`token-${index}-${tokenIndex}`"
            ><span :class="token.className">{{ token.text }}</span></template></span></code></pre>
        </div>
    </div>
</template>

<style scoped>
.code-panel {
    width: 100%;
    text-align: left;
    border: 1px solid rgba(71, 85, 105, 0.72);
    border-radius: 1.2rem;
    background: linear-gradient(180deg, rgba(9, 13, 31, 0.98), rgba(14, 20, 40, 0.98));
    box-shadow:
        inset 0 1px 0 rgba(255, 255, 255, 0.04),
        0 18px 36px rgba(2, 6, 23, 0.28);
    overflow: hidden;
}

.code-panel-compact {
    border-radius: 1rem;
}

.code-panel-topbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.75rem;
    padding: 0.7rem 1.15rem 0.7rem 0.95rem;
    border-bottom: 1px solid rgba(51, 65, 85, 0.68);
    background: rgba(15, 23, 42, 0.88);
}

.code-panel-label,
.code-panel-language {
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
}

.code-panel-label {
    color: rgba(191, 219, 254, 0.92);
}

.code-panel-language {
    color: rgba(148, 163, 184, 0.84);
}

.code-panel-body {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr);
    gap: 0.8rem;
    padding: 0.85rem 1.25rem 0.95rem 0.95rem;
    align-items: start;
}

.code-panel-gutter {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    gap: 0.12rem;
    padding-top: 0.02rem;
    color: rgba(100, 116, 139, 0.72);
    font-family:
        ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
        "Courier New", monospace;
    font-size: 0.78rem;
    line-height: 1.55;
    user-select: none;
}

.code-panel-body-no-gutter {
    grid-template-columns: minmax(0, 1fr);
}

.code-panel-pre {
    margin: 0;
    white-space: pre;
    overflow-x: auto;
    color: #e5e7eb;
    text-align: left;
    background: linear-gradient(180deg, rgba(5, 10, 27, 0.98), rgba(10, 16, 32, 0.98)) !important;
    border-radius: 0.7rem;
    padding: 0.1rem 1rem 0.1rem 0.25rem;
}

.code-panel-pre code {
    display: block;
    font-family:
        ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
        "Courier New", monospace;
    font-size: 0.88rem;
    line-height: 1.55;
    background: transparent !important;
    color: #e5e7eb !important;
    text-align: left;
    padding-right: 0.65rem;
}

.code-panel-line {
    display: block;
    min-height: 1.55em;
    background: transparent !important;
}

.code-panel-line span {
    background: transparent !important;
}

.code-panel :deep(pre),
.code-panel :deep(code),
.code-panel :deep(span) {
    background: transparent !important;
}

.code-panel-compact .code-panel-body {
    gap: 0.75rem;
    padding: 0.8rem 1.05rem 0.8rem 0.8rem;
}

.code-panel-compact .code-panel-gutter {
    font-size: 0.76rem;
    line-height: 1.6;
}

.code-panel-compact .code-panel-pre {
    overflow-x: hidden;
    padding: 0.15rem 0.8rem 0.15rem 0;
}

.code-panel-compact .code-panel-pre code {
    font-size: 0.78rem;
    line-height: 1.6;
    white-space: pre-wrap;
    word-break: break-word;
}

.code-panel-compact .code-panel-line {
    min-height: 1.6em;
}

:deep(.tok-keyword) {
    color: #c084fc;
}

:deep(.tok-type) {
    color: #f8fafc;
}

:deep(.tok-function) {
    color: #7dd3fc;
}

:deep(.tok-string) {
    color: #bef264;
}

:deep(.tok-number) {
    color: #fb7185;
}

:deep(.tok-property) {
    color: #93c5fd;
}

:deep(.tok-option) {
    color: #f9a8d4;
}

:deep(.tok-link) {
    color: #67e8f9;
}

:deep(.tok-punctuation) {
    color: #94a3b8;
}

:deep(.tok-muted) {
    color: #64748b;
}

@media (max-width: 640px) {
    .code-panel-body {
        gap: 0.7rem;
        padding: 0.9rem;
    }

    .code-panel-gutter {
        font-size: 0.74rem;
    }

    .code-panel-pre code {
        font-size: 0.82rem;
    }
}
</style>
