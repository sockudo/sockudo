import Pusher from '@sockudo/client'
import { useDashboardStore } from '../stores/dashboard'

let pusherInstance: Pusher | null = null
const SESSION_ID = `session-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

export function usePusher() {
  const store = useDashboardStore()

  function connect() {
    if (pusherInstance) pusherInstance.disconnect()

    store.connectionState = 'connecting'
    store.addEvent({ direction: 'system', event: 'Connecting...', data: { host: store.config.host, port: store.config.port } })

    const cfg = store.config
    const pusher = new Pusher(cfg.appKey, {
      cluster: cfg.cluster,
      wsHost: cfg.host,
      wsPort: cfg.port,
      wssPort: cfg.port,
      forceTLS: cfg.useTLS,
      enabledTransports: ['ws', 'wss'],
      channelAuthorization: {
        endpoint: cfg.authEndpoint,
        transport: 'ajax',
        headers: { 'X-Session-Id': SESSION_ID },
      },
      userAuthentication: {
        endpoint: cfg.userAuthEndpoint,
        transport: 'ajax',
        headers: { 'X-Session-Id': SESSION_ID },
      },
      deltaCompression: {
        enabled: true,
        algorithms: ['fossil', 'xdelta3'],
        debug: false,
        onStats: (stats: any) => {
          store.updateDeltaStats(stats)
        },
        onError: (err: any) => {
          store.addEvent({ direction: 'system', event: 'delta:error', data: err })
        },
      },
    } as any)

    pusher.connection.bind('connected', () => {
      store.connectionState = 'connected'
      store.socketId = pusher.connection.socket_id
      store.addEvent({ direction: 'in', event: 'pusher:connection_established', data: { socket_id: pusher.connection.socket_id } })
    })

    pusher.connection.bind('connecting', () => {
      store.connectionState = 'connecting'
    })

    pusher.connection.bind('disconnected', () => {
      store.connectionState = 'disconnected'
      store.socketId = null
      store.addEvent({ direction: 'system', event: 'Disconnected' })
    })

    pusher.connection.bind('failed', () => {
      store.connectionState = 'failed'
      store.addEvent({ direction: 'system', event: 'Connection failed' })
    })

    pusher.connection.bind('error', (err: any) => {
      store.addEvent({ direction: 'system', event: 'pusher:error', data: err })
    })

    pusher.connection.bind('message', (msg: any) => {
      if (msg?.event && !msg.event.startsWith('pusher_internal:') && msg.event !== 'pusher:connection_established') {
        store.addEvent({ direction: 'in', event: msg.event, channel: msg.channel, data: msg.data })
      }
    })

    pusher.bind_global((eventName: string, data: any) => {
      if (eventName === 'pusher:pong') {
        store.addEvent({ direction: 'in', event: 'pusher:pong' })
      } else if (eventName === 'pusher:delta_compression_enabled') {
        store.addEvent({ direction: 'in', event: 'pusher:delta_compression_enabled', data })
      } else if (eventName === 'pusher:delta') {
        store.addEvent({ direction: 'in', event: 'pusher:delta', data })
      } else if (eventName === 'pusher:delta_cache_sync') {
        store.addEvent({ direction: 'in', event: 'pusher:delta_cache_sync', data })
      }
    })

    pusherInstance = pusher
    pusher.connect()
  }

  function disconnect() {
    if (pusherInstance) {
      pusherInstance.disconnect()
      pusherInstance = null
      store.connectionState = 'disconnected'
      store.socketId = null
    }
  }

  function subscribe(channelName: string) {
    if (!pusherInstance) return null

    const type = channelName.startsWith('presence-')
      ? 'presence'
      : channelName.startsWith('private-encrypted-')
        ? 'encrypted'
        : channelName.startsWith('private-')
          ? 'private'
          : channelName.startsWith('cache-')
            ? 'cache'
            : 'public'

    const channel = pusherInstance.subscribe(channelName)

    channel.bind('pusher:subscription_succeeded', (data: any) => {
      store.addEvent({ direction: 'in', event: 'pusher:subscription_succeeded', channel: channelName, data })
      store.updateChannel(channelName, { subscribed: true })

      if (type === 'presence' && data?.members) {
        const members = Object.entries(data.members).map(([id, info]) => ({
          id,
          info: info as Record<string, unknown>,
        }))
        store.setPresenceMembers(channelName, members)
        store.updateChannel(channelName, { members })
      }
    })

    channel.bind('pusher:subscription_error', (err: any) => {
      store.addEvent({ direction: 'system', event: 'pusher:subscription_error', channel: channelName, data: err })
    })

    if (type === 'presence') {
      channel.bind('pusher:member_added', (member: any) => {
        store.addEvent({ direction: 'in', event: 'pusher:member_added', channel: channelName, data: member })
      })
      channel.bind('pusher:member_removed', (member: any) => {
        store.addEvent({ direction: 'in', event: 'pusher:member_removed', channel: channelName, data: member })
      })
    }

    store.addChannel({ name: channelName, type, subscribed: false })
    store.addEvent({ direction: 'out', event: 'pusher:subscribe', channel: channelName })
    return channel
  }

  function unsubscribe(channelName: string) {
    if (!pusherInstance) return
    pusherInstance.unsubscribe(channelName)
    store.removeChannel(channelName)
    store.addEvent({ direction: 'out', event: 'pusher:unsubscribe', channel: channelName })
  }

  function bindEvent(channelName: string, eventName: string, callback: (data: any) => void) {
    if (!pusherInstance) return
    const channel = pusherInstance.channel(channelName)
    if (channel) channel.bind(eventName, callback)
  }

  function triggerClientEvent(channelName: string, eventName: string, data: unknown) {
    if (!pusherInstance) return
    const channel = pusherInstance.channel(channelName)
    if (channel) {
      ;(channel as any).trigger(eventName, data)
      store.addEvent({ direction: 'out', event: eventName, channel: channelName, data })
    }
  }

  function signin() {
    if (!pusherInstance) return
    ;(pusherInstance as any).signin()
    store.addEvent({ direction: 'out', event: 'pusher:signin' })
  }

  function sendPing() {
    if (!pusherInstance) return
    ;(pusherInstance as any).send_event('pusher:ping', {})
    store.addEvent({ direction: 'out', event: 'pusher:ping' })
  }

  return { connect, disconnect, subscribe, unsubscribe, bindEvent, triggerClientEvent, signin, sendPing }
}
