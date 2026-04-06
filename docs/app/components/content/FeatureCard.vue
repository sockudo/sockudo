<script setup lang="ts">
defineProps({
    title: {
        type: String,
        required: true,
    },
    description: {
        type: String,
        required: true,
    },
    gradient: {
        type: [Boolean, String],
        default: false,
    },
    icon: {
        type: String,
        default: null,
    },
    compactDemo: {
        type: [Boolean, String],
        default: false,
    },
});
</script>

<template>
    <div class="relative group h-full flex flex-col">
        <!-- Outer glow effect that bleeds outside the card -->
        <div
            class="absolute -inset-[2px] bg-gradient-to-r from-[#7938d3] to-[#ec4899] rounded-2xl opacity-0 group-hover:opacity-50 blur-lg transition duration-500 group-hover:duration-200"
        ></div>

        <!-- Inner glowing border -->
        <div
            class="absolute -inset-[1px] bg-gradient-to-r from-[#7938d3] to-[#ec4899] rounded-2xl opacity-0 group-hover:opacity-100 transition duration-500 group-hover:duration-200 z-0"
        ></div>

        <!-- Content container -->
        <div
            class="relative flex flex-col h-full z-10 bg-elevated rounded-2xl border border-default group-hover:border-transparent transition-colors duration-500 overflow-hidden"
        >
            <!-- Text section -->
            <div class="p-6 md:p-8 relative z-20 bg-elevated">
                <div
                    v-if="icon"
                    class="mb-4 inline-flex items-center justify-center p-2 bg-primary/10 rounded-lg"
                >
                    <UIcon :name="icon" class="w-6 h-6 text-primary" />
                </div>
                <h3 class="text-xl md:text-2xl font-bold text-highlighted mb-3">
                    {{ title }}
                </h3>
                <p class="text-muted leading-relaxed text-sm md:text-base">
                    {{ description }}
                </p>
            </div>

            <!-- Code/Visual section (Bottom) -->
            <div
                v-if="$slots.default"
                :class="[
                    'relative overflow-hidden border-t border-default group-hover:border-transparent transition-colors duration-500',
                    compactDemo === true || compactDemo === 'true'
                        ? 'feature-card-demo-compact mt-5 pt-5'
                        : 'mt-auto pt-8 flex-1 flex flex-col justify-end',
                    gradient === true || gradient === 'true'
                        ? 'bg-gradient-to-br from-[#7938d3]/40 via-[#a855f7]/20 to-[#7938d3]/5'
                        : 'bg-muted/30',
                ]"
            >
                <!-- Floating overlay effect on hover inside the code area -->
                <div
                    class="absolute inset-0 bg-gradient-to-t from-transparent to-white/5 opacity-0 group-hover:opacity-100 transition-opacity duration-500 pointer-events-none"
                ></div>

                <div
                    :class="[
                        'px-6 md:px-8 pb-0 transition-transform duration-500 ease-out z-20',
                        compactDemo === true || compactDemo === 'true'
                            ? 'translate-y-0'
                            : 'translate-y-3 group-hover:translate-y-0 flex-1 flex flex-col justify-end',
                    ]"
                >
                    <slot />
                </div>
            </div>
        </div>
    </div>
</template>

<style scoped>
/* Deep target for markdown code blocks to make them fit nicely */
:deep(pre) {
    margin-bottom: -4px !important; /* Push it down so bottom border is hidden */
    border-bottom-left-radius: 0 !important;
    border-bottom-right-radius: 0 !important;
    border: 1px solid var(--ui-border) !important;
    border-bottom: none !important;
    box-shadow: 0 -8px 30px rgba(0, 0, 0, 0.3) !important;
    background-color: var(--ui-bg-inverted) !important;
    transition: box-shadow 0.5s ease;
}

.group:hover :deep(pre) {
    box-shadow: 0 -12px 40px rgba(121, 56, 211, 0.4) !important;
}

:deep(.prose) {
    flex: 1;
    display: flex;
    flex-direction: column;
    justify-content: flex-end;
}

.feature-card-demo-compact {
    flex: 0 0 auto;
}

.feature-card-demo-compact :deep(.code-panel) {
    margin-bottom: 0;
    max-width: 100%;
}
</style>
