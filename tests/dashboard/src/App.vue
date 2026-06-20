<script setup lang="ts">
import { useDashboardStore } from './stores/dashboard'
import Sidebar from './components/Sidebar.vue'
import ShowcasePanel from './components/ShowcasePanel.vue'
import ConnectionPanel from './components/ConnectionPanel.vue'
import ChannelsPanel from './components/ChannelsPanel.vue'
import EventsPanel from './components/EventsPanel.vue'
import PresencePanel from './components/PresencePanel.vue'
import RecoveryPanel from './components/RecoveryPanel.vue'
import HttpApiPanel from './components/HttpApiPanel.vue'
import MutableMessagesPanel from './components/MutableMessagesPanel.vue'
import FiltersPanel from './components/FiltersPanel.vue'
import DeltaPanel from './components/DeltaPanel.vue'
import OpsPanel from './components/OpsPanel.vue'
import PushPanel from './components/PushPanel.vue'
import AiChatPanel from './components/AiChatPanel.vue'
import AiTransportPanel from './components/AiTransportPanel.vue'

const store = useDashboardStore()

const panels: Record<string, any> = {
  showcase: ShowcasePanel,
  connection: ConnectionPanel,
  channels: ChannelsPanel,
  events: EventsPanel,
  presence: PresencePanel,
  recovery: RecoveryPanel,
  api: HttpApiPanel,
  mutable: MutableMessagesPanel,
  filters: FiltersPanel,
  delta: DeltaPanel,
  ops: OpsPanel,
  push: PushPanel,
  'ai-chat': AiChatPanel,
  ai: AiTransportPanel,
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
