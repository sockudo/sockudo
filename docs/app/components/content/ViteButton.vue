<script setup lang="ts">
defineProps({
  to: {
    type: String,
    required: true
  },
  icon: {
    type: String,
    default: null
  }
})
</script>

<template>
  <NuxtLink :to="to" class="vite-btn group">
    <span class="vite-btn-inner"></span>
    <span class="relative z-10 flex items-center gap-2">
      <slot />
      <UIcon v-if="icon" :name="icon" class="w-5 h-5 group-hover:translate-x-1 transition-transform" />
    </span>
  </NuxtLink>
</template>
