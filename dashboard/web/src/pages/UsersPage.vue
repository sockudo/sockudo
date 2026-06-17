<script setup lang="ts">
import { onMounted, ref } from "vue";
import { Plus, Trash2 } from "lucide-vue-next";
import { api } from "@/api/client";
import type { DashboardUser, UserRole } from "@/types/user";

const users = ref<DashboardUser[]>([]);
const loading = ref(true);
const error = ref<string | null>(null);
const showCreate = ref(false);

const form = ref({
  email: "",
  password: "",
  name: "",
  role: "operator" as UserRole,
});

onMounted(load);

async function load() {
  loading.value = true;
  error.value = null;
  try {
    users.value = await api.listUsers();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Failed to load users";
  } finally {
    loading.value = false;
  }
}

async function createUser() {
  try {
    await api.createUser({
      email: form.value.email.trim(),
      password: form.value.password,
      name: form.value.name.trim() || undefined,
      role: form.value.role,
    });
    form.value = { email: "", password: "", name: "", role: "operator" };
    showCreate.value = false;
    await load();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Create failed";
  }
}

async function toggleActive(user: DashboardUser) {
  try {
    await api.updateUser(user.id, { active: !user.active });
    await load();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Update failed";
  }
}

async function removeUser(user: DashboardUser) {
  if (!confirm(`Delete user "${user.email}"?`)) return;
  try {
    await api.deleteUser(user.id);
    await load();
  } catch (err) {
    error.value = err instanceof Error ? err.message : "Delete failed";
  }
}
</script>

<template>
  <div>
    <div class="flex items-center justify-between mb-6">
      <div>
        <h1 class="text-2xl font-semibold text-surface-100">Dashboard users</h1>
        <p class="text-sm text-surface-400 mt-1">
          Manage operator accounts stored in the dashboard database.
        </p>
      </div>
      <button class="btn-primary flex items-center gap-2" @click="showCreate = !showCreate">
        <Plus class="w-4 h-4" />
        New user
      </button>
    </div>

    <div v-if="showCreate" class="panel p-6 mb-6 space-y-4">
      <h2 class="font-medium text-surface-200">Create user</h2>
      <div class="grid md:grid-cols-2 gap-4">
        <div>
          <label class="section-title">Email</label>
          <input v-model="form.email" type="email" class="input-field" />
        </div>
        <div>
          <label class="section-title">Name</label>
          <input v-model="form.name" class="input-field" />
        </div>
        <div>
          <label class="section-title">Password</label>
          <input v-model="form.password" type="password" class="input-field" />
        </div>
        <div>
          <label class="section-title">Role</label>
          <select v-model="form.role" class="input-field">
            <option value="operator">operator</option>
            <option value="admin">admin</option>
          </select>
        </div>
      </div>
      <div class="flex gap-2">
        <button class="btn-primary" @click="createUser">Create</button>
        <button class="btn-secondary" @click="showCreate = false">Cancel</button>
      </div>
    </div>

    <p v-if="error" class="text-red-400 text-sm mb-4">{{ error }}</p>
    <div v-if="loading" class="text-surface-400">Loading users...</div>

    <div v-else class="panel overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-surface-800/50 text-surface-400 text-left">
          <tr>
            <th class="px-4 py-3 font-medium">Email</th>
            <th class="px-4 py-3 font-medium">Name</th>
            <th class="px-4 py-3 font-medium">Role</th>
            <th class="px-4 py-3 font-medium">Status</th>
            <th class="px-4 py-3 font-medium text-right">Actions</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="user in users"
            :key="user.id"
            class="border-t border-surface-800 hover:bg-surface-800/30"
          >
            <td class="px-4 py-3">{{ user.email }}</td>
            <td class="px-4 py-3">{{ user.name }}</td>
            <td class="px-4 py-3 capitalize">{{ user.role }}</td>
            <td class="px-4 py-3">
              <span
                class="text-xs px-2 py-0.5 rounded-full"
                :class="user.active ? 'bg-green-500/15 text-green-400' : 'bg-red-500/15 text-red-400'"
              >
                {{ user.active ? "active" : "disabled" }}
              </span>
            </td>
            <td class="px-4 py-3">
              <div class="flex justify-end gap-2">
                <button class="btn-secondary btn-sm" @click="toggleActive(user)">
                  {{ user.active ? "Disable" : "Enable" }}
                </button>
                <button class="btn-danger btn-sm" @click="removeUser(user)">
                  <Trash2 class="w-3 h-3" />
                </button>
              </div>
            </td>
          </tr>
          <tr v-if="users.length === 0">
            <td colspan="5" class="px-4 py-8 text-center text-surface-500">
              No users found. Run <code class="font-mono">bun run seed:admin</code> first.
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
