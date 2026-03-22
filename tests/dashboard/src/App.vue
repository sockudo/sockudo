<script setup lang="ts">
import { useDashboardStore } from './stores/dashboard'
import Sidebar from './components/Sidebar.vue'
import ConnectionPanel from './components/ConnectionPanel.vue'
import ChannelsPanel from './components/ChannelsPanel.vue'
import EventsPanel from './components/EventsPanel.vue'
import PresencePanel from './components/PresencePanel.vue'
import HttpApiPanel from './components/HttpApiPanel.vue'
import FiltersPanel from './components/FiltersPanel.vue'
import DeltaPanel from './components/DeltaPanel.vue'

const store = useDashboardStore()

const panels: Record<string, any> = {
  connection: ConnectionPanel,
  channels: ChannelsPanel,
  events: EventsPanel,
  presence: PresencePanel,
  api: HttpApiPanel,
  filters: FiltersPanel,
  delta: DeltaPanel,
}
</script>

<template>
  <div class="flex h-screen overflow-hidden">
    <Sidebar />
    <main class="flex-1 overflow-y-auto p-6 lg:p-8">
      <div class="max-w-6xl mx-auto">
        <component :is="panels[store.activeTab]" />
      </div>
    </main>
  </div>
</template>
