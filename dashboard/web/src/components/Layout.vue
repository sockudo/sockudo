<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useRouter, RouterLink, RouterView } from "vue-router";
import { Activity, LayoutGrid, LogOut, Radio, Users } from "lucide-vue-next";
import { useAuthStore } from "@/stores/auth";
import { api } from "@/api/client";

const auth = useAuthStore();
const router = useRouter();
const driver = ref("");

onMounted(async () => {
  try {
    const cfg = await api.opsConfig();
    driver.value = cfg.app_manager_driver;
  } catch {
    driver.value = "unknown";
  }
});

async function logout() {
  await auth.logout();
  router.push({ name: "login" });
}
</script>

<template>
  <div class="flex h-screen overflow-hidden">
    <aside class="w-64 shrink-0 border-r border-surface-800 bg-surface-900/50 p-4 flex flex-col">
      <div class="mb-8 px-2">
        <div class="flex items-center gap-2 text-brand-400">
          <Radio class="w-5 h-5" />
          <span class="font-semibold text-surface-100">Sockudo</span>
        </div>
        <p class="text-xs text-surface-500 mt-1 px-0.5">Operator Dashboard</p>
        <p v-if="driver" class="text-[10px] text-surface-600 mt-2 font-mono">
          driver: {{ driver }}
        </p>
      </div>

      <nav class="space-y-1 flex-1">
        <RouterLink
          to="/"
          class="flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-surface-300 hover:bg-surface-800"
          active-class="!bg-brand-600/15 !text-brand-300"
        >
          <LayoutGrid class="w-4 h-4" />
          Apps
        </RouterLink>
        <RouterLink
          to="/metrics"
          class="flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-surface-300 hover:bg-surface-800"
          active-class="!bg-brand-600/15 !text-brand-300"
        >
          <Activity class="w-4 h-4" />
          Metrics
        </RouterLink>
        <RouterLink
          v-if="auth.isAdmin"
          to="/users"
          class="flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-surface-300 hover:bg-surface-800"
          active-class="!bg-brand-600/15 !text-brand-300"
        >
          <Users class="w-4 h-4" />
          Users
        </RouterLink>
      </nav>

      <div class="border-t border-surface-800 pt-4 mt-4">
        <p class="text-xs text-surface-500 px-2 mb-2 truncate">
          {{ auth.user?.name || auth.email }}
          <span v-if="auth.isAdmin" class="text-brand-400">(admin)</span>
        </p>
        <button class="btn-secondary w-full flex items-center justify-center gap-2" @click="logout">
          <LogOut class="w-4 h-4" />
          Sign out
        </button>
      </div>
    </aside>

    <main class="flex-1 overflow-y-auto p-6 lg:p-8">
      <div class="max-w-6xl mx-auto">
        <RouterView />
      </div>
    </main>
  </div>
</template>
