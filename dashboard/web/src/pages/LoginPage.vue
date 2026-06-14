<script setup lang="ts">
import { ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { Radio } from "lucide-vue-next";
import { useAuthStore } from "@/stores/auth";

const auth = useAuthStore();
const router = useRouter();
const route = useRoute();

const email = ref("admin@sockudo.local");
const password = ref("");

async function submit() {
  try {
    await auth.login(email.value, password.value);
    const redirect = (route.query.redirect as string) || "/";
    router.push(redirect);
  } catch {
    // error shown via store
  }
}
</script>

<template>
  <div class="min-h-screen flex items-center justify-center p-4">
    <div class="panel w-full max-w-md p-8">
      <div class="flex items-center gap-2 text-brand-400 mb-2">
        <Radio class="w-6 h-6" />
        <h1 class="text-xl font-semibold text-surface-100">Sockudo Dashboard</h1>
      </div>
      <p class="text-sm text-surface-400 mb-8">
        Sign in to manage apps, webhooks, and metrics.
      </p>

      <form class="space-y-4" @submit.prevent="submit">
        <div>
          <label class="section-title">Email</label>
          <input v-model="email" type="email" class="input-field" required />
        </div>
        <div>
          <label class="section-title">Password</label>
          <input v-model="password" type="password" class="input-field" required />
        </div>
        <p v-if="auth.error" class="text-sm text-red-400">{{ auth.error }}</p>
        <button type="submit" class="btn-primary w-full" :disabled="auth.loading">
          {{ auth.loading ? "Signing in..." : "Sign in" }}
        </button>
      </form>
    </div>
  </div>
</template>
