import { defineStore } from "pinia";
import { computed, ref } from "vue";
import { api, ApiError } from "@/api/client";
import type { DashboardUser } from "@/types/user";

export const useAuthStore = defineStore("auth", () => {
  const user = ref<DashboardUser | null>(null);
  const loading = ref(false);
  const error = ref<string | null>(null);

  const email = computed(() => user.value?.email ?? null);
  const isAdmin = computed(() => user.value?.role === "admin");

  async function bootstrap() {
    try {
      user.value = await api.me();
    } catch {
      user.value = null;
    }
  }

  async function login(loginEmail: string, password: string) {
    loading.value = true;
    error.value = null;
    try {
      user.value = await api.login(loginEmail, password);
    } catch (err) {
      error.value = err instanceof ApiError ? err.message : "Login failed";
      throw err;
    } finally {
      loading.value = false;
    }
  }

  async function logout() {
    await api.logout();
    user.value = null;
  }

  return { user, email, isAdmin, loading, error, bootstrap, login, logout };
});
