# frozen_string_literal: true

require 'base64'
require 'securerandom'
require 'pusher-signature'

module Sockudo
  class Client
    attr_accessor :scheme, :host, :port, :app_id, :key, :secret, :encryption_master_key
    attr_reader :http_proxy, :proxy, :base_id, :publish_serial
    attr_writer :connect_timeout, :send_timeout, :receive_timeout,
                :keep_alive_timeout

    ## CONFIGURATION ##
    DEFAULT_CONNECT_TIMEOUT = 5
    DEFAULT_SEND_TIMEOUT = 5
    DEFAULT_RECEIVE_TIMEOUT = 5
    DEFAULT_KEEP_ALIVE_TIMEOUT = 30
    DEFAULT_CLUSTER = 'mt1'

    # Loads the configuration from an url in the environment
    def self.from_env(key = 'SOCKUDO_URL')
      url = ENV[key] || raise(ConfigurationError, key)
      from_url(url)
    end

    # Loads the configuration from a url
    def self.from_url(url)
      client = new
      client.url = url
      client
    end

    def initialize(options = {})
      @scheme = 'https'
      @port = options[:port] || 443

      if options.key?(:encrypted)
        warn '[DEPRECATION] `encrypted` is deprecated and will be removed in the next major version. Use `use_tls` instead.'
      end

      if options[:use_tls] == false || options[:encrypted] == false
        @scheme = 'http'
        @port = options[:port] || 80
      end

      @app_id = options[:app_id]
      @key = options[:key]
      @secret = options[:secret]
      @auto_idempotency_key = options.fetch(:auto_idempotency_key, true)
      @base_id = Base64.urlsafe_encode64(SecureRandom.random_bytes(12), padding: false)
      @publish_serial = 0
      @max_retries = 3

      @host = options[:host]
      @host ||= "api-#{options[:cluster]}.sockudo.com" unless options[:cluster].nil? || options[:cluster].empty?
      @host ||= "api-#{DEFAULT_CLUSTER}.sockudo.com"

      @encryption_master_key = Base64.strict_decode64(options[:encryption_master_key_base64]) if options[:encryption_master_key_base64]

      @http_proxy = options[:http_proxy]

      # Default timeouts
      @connect_timeout = DEFAULT_CONNECT_TIMEOUT
      @send_timeout = DEFAULT_SEND_TIMEOUT
      @receive_timeout = DEFAULT_RECEIVE_TIMEOUT
      @keep_alive_timeout = DEFAULT_KEEP_ALIVE_TIMEOUT
    end

    # @private Returns the authentication token for the client
    def authentication_token
      raise ConfigurationError, :key unless @key
      raise ConfigurationError, :secret unless @secret

      Pusher::Signature::Token.new(@key, @secret)
    end

    # @private Builds a url for this app, optionally appending a path
    def url(path = nil)
      raise ConfigurationError, :app_id unless @app_id

      URI::Generic.build({
                           scheme: @scheme,
                           host: @host,
                           port: @port,
                           path: "/apps/#{@app_id}#{path}"
                         })
    end

    # Configure Sockudo connection by providing a url rather than specifying
    # scheme, key, secret, and app_id separately.
    #
    # @example
    #   Sockudo.url = http://KEY:SECRET@localhost/apps/APP_ID
    #
    def url=(url)
      uri = URI.parse(url)
      @scheme = uri.scheme
      @app_id = uri.path.split('/').last
      @key    = uri.user
      @secret = uri.password
      @host   = uri.host
      @port   = uri.port
    end

    def http_proxy=(http_proxy)
      @http_proxy = http_proxy
      uri = URI.parse(http_proxy)
      @proxy = {
        scheme: uri.scheme,
        host: uri.host,
        port: uri.port,
        user: uri.user,
        password: uri.password
      }
    end

    # Configure whether Sockudo API calls should be made over SSL
    # (default false)
    #
    # @example
    #   Sockudo.encrypted = true
    #
    def encrypted=(boolean)
      @scheme = boolean ? 'https' : 'http'
      # Configure port if it hasn't already been configured
      @port = boolean ? 443 : 80
    end

    def encrypted?
      @scheme == 'https'
    end

    def cluster=(cluster)
      cluster = DEFAULT_CLUSTER if cluster.nil? || cluster.empty?

      @host = "api-#{cluster}.sockudo.com"
    end

    # Convenience method to set all timeouts to the same value (in seconds).
    # For more control, use the individual writers.
    def timeout=(value)
      @connect_timeout = value
      @send_timeout = value
      @receive_timeout = value
    end

    # Set an encryption_master_key to use with private-encrypted channels from
    # a base64 encoded string.
    def encryption_master_key_base64=(str)
      @encryption_master_key = str ? Base64.strict_decode64(str) : nil
    end

    ## INTERACT WITH THE API ##

    def resource(path)
      Resource.new(self, path)
    end

    # GET arbitrary REST API resource using a synchronous http client.
    # All request signing is handled automatically.
    #
    # @example
    #   begin
    #     Sockudo.get('/channels', filter_by_prefix: 'private-')
    #   rescue Sockudo::Error => e
    #     # Handle error
    #   end
    #
    # @param path [String] Path excluding /apps/APP_ID
    # @param params [Hash] API params (see http://sockudo.com/docs/rest_api)
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def get(path, params = {}, headers = {})
      resource(path).get(params, headers)
    end

    # GET arbitrary REST API resource using an asynchronous http client.
    # All request signing is handled automatically.
    #
    # When the eventmachine reactor is running, the em-http-request gem is used;
    # otherwise an async request is made using httpclient. See README for
    # details and examples.
    #
    # @param path [String] Path excluding /apps/APP_ID
    # @param params [Hash] API params (see http://sockudo.com/docs/rest_api)
    #
    # @return Either an EM::DefaultDeferrable or a HTTPClient::Connection
    #
    def get_async(path, params = {}, headers = {})
      resource(path).get_async(params, headers)
    end

    # POST arbitrary REST API resource using a synchronous http client.
    # Works identially to get method, but posts params as JSON in post body.
    def post(path, params = {}, headers = {})
      resource(path).post(params, headers)
    end

    # DELETE arbitrary REST API resource using a synchronous http client.
    # All request signing is handled automatically.
    def delete(path, params = {}, headers = {})
      resource(path).delete(params, headers)
    end

    # POST arbitrary REST API resource using an asynchronous http client.
    # Works identially to get_async method, but posts params as JSON in post
    # body.
    def post_async(path, params = {}, headers = {})
      resource(path).post_async(params, headers)
    end

    ## HELPER METHODS ##

    # Convenience method for creating a new WebHook instance for validating
    # and extracting info from a received WebHook
    #
    # @param request [Rack::Request] Either a Rack::Request or a Hash containing :key, :signature, :body, and optionally :content_type.
    #
    def webhook(request)
      WebHook.new(request, self)
    end

    # Return a convenience channel object by name that delegates operations
    # on a channel. No API request is made.
    #
    # @example
    #   Sockudo['my-channel']
    # @return [Channel]
    # @raise [Sockudo::Error] if the channel name is invalid.
    #   Channel names should be less than 200 characters, and
    #   should not contain anything other than letters, numbers, or the
    #   characters "_\-=@,.;"
    def channel(channel_name)
      Channel.new(nil, channel_name, self)
    end

    alias [] channel

    # Request a list of occupied channels from the API
    #
    # GET /apps/[id]/channels
    #
    # @param params [Hash] Hash of parameters for the API - see REST API docs
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def channels(params = {})
      get('/channels', params)
    end

    # Request info for a specific channel
    #
    # GET /apps/[id]/channels/[channel_name]
    #
    # @param channel_name [String] Channel name (max 200 characters)
    # @param params [Hash] Hash of parameters for the API - see REST API docs
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def channel_info(channel_name, params = {})
      get("/channels/#{channel_name}", params)
    end

    # Request durable history for a specific channel
    #
    # GET /apps/[id]/channels/[channel_name]/history
    #
    # @param channel_name [String] Channel name (max 200 characters)
    # @param params [Hash] Hash of parameters for the API. Supported keys include:
    #   :limit, :direction, :cursor, :start_serial, :end_serial, :start_time_ms, :end_time_ms
    #
    # @return [Hash] See Sockudo history API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def channel_history(channel_name, params = {})
      get("/channels/#{channel_name}/history", params)
    end

    # Request presence history for a specific presence channel
    #
    # GET /apps/[id]/channels/[channel_name]/presence/history
    #
    # @param channel_name [String] Presence channel name (max 200 characters)
    # @param params [Hash] Hash of parameters for the API. Supported keys include:
    #   :limit, :direction, :cursor, :start_serial, :end_serial, :start_time_ms, :end_time_ms
    #
    # @return [Hash] See Sockudo presence history API docs
    #
    def channel_presence_history(channel_name, params = {})
      get("/channels/#{channel_name}/presence/history", params)
    end

    # Request a reconstructed presence snapshot for a specific presence channel
    #
    # GET /apps/[id]/channels/[channel_name]/presence/history/snapshot
    #
    # @param channel_name [String] Presence channel name (max 200 characters)
    # @param params [Hash] Hash of parameters for the API. Supported keys include:
    #   :at_time_ms, :at_serial
    #
    # @return [Hash] See Sockudo presence snapshot API docs
    #
    def channel_presence_snapshot(channel_name, params = {})
      get("/channels/#{channel_name}/presence/history/snapshot", params)
    end

    # Request the latest visible version of a mutable message
    def get_message(channel_name, message_serial, params = {})
      get("/channels/#{channel_name}/messages/#{message_serial}", params)
    end

    # Request preserved versions of a mutable message
    def get_message_versions(channel_name, message_serial, params = {})
      get("/channels/#{channel_name}/messages/#{message_serial}/versions", params)
    end

    # Apply a mutable-message update
    def update_message(channel_name, message_serial, params = {})
      post("/channels/#{channel_name}/messages/#{message_serial}/update", params)
    end

    # Apply a mutable-message delete
    def delete_message(channel_name, message_serial, params = {})
      post("/channels/#{channel_name}/messages/#{message_serial}/delete", params)
    end

    # Apply a mutable-message append
    def append_message(channel_name, message_serial, params = {})
      post("/channels/#{channel_name}/messages/#{message_serial}/append", params)
    end

    # Publish an annotation for a versioned message
    def publish_annotation(channel_name, message_serial, params = {})
      post("/channels/#{channel_name}/messages/#{message_serial}/annotations", params)
    end

    # Delete an annotation from a versioned message
    def delete_annotation(channel_name, message_serial, annotation_serial, params = {})
      delete("/channels/#{channel_name}/messages/#{message_serial}/annotations/#{annotation_serial}", params)
    end

    # List raw annotation events for a versioned message
    def list_annotations(channel_name, message_serial, params = {})
      get("/channels/#{channel_name}/messages/#{message_serial}/annotations", params)
    end

    # Request info for users of a presence channel
    #
    # GET /apps/[id]/channels/[channel_name]/users
    #
    # @param channel_name [String] Channel name (max 200 characters)
    # @param params [Hash] Hash of parameters for the API - see REST API docs
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def channel_users(channel_name, params = {})
      get("/channels/#{channel_name}/users", params)
    end

    # Activate or create a push device registration with admin scope
    def activate_device(device, options = {})
      post(push_path('/deviceRegistrations'), device, push_headers('push-admin', nil, options[:rotate_device_identity_token]))
    end

    # Alias of activate_device
    def create_device_activation(device, options = {})
      activate_device(device, options)
    end

    # Update a push device registration with push-subscribe scope
    def update_device_registration(device, device_identity_token)
      post(push_path('/deviceRegistrations'), device, push_headers('push-subscribe', device_identity_token))
    end

    # List push device registrations with cursor pagination
    def list_device_registrations(params = {})
      get(push_path('/deviceRegistrations'), params, push_headers('push-admin'))
    end

    # Get a push device registration
    def get_device_registration(device_id, device_identity_token = nil)
      capability = device_identity_token ? 'push-subscribe' : 'push-admin'
      get(push_path("/deviceRegistrations/#{device_id}"), {}, push_headers(capability, device_identity_token))
    end

    # Delete a push device registration
    def delete_device_registration(device_id, device_identity_token = nil)
      capability = device_identity_token ? 'push-subscribe' : 'push-admin'
      delete(push_path("/deviceRegistrations/#{device_id}"), {}, push_headers(capability, device_identity_token))
    end

    # Delete all device registrations for a client identifier
    def remove_device_registrations_by_client(client_id)
      delete(push_path('/deviceRegistrations'), { clientId: client_id }, push_headers('push-admin'))
    end

    # Upsert a push channel subscription
    def upsert_channel_push_subscription(subscription, device_identity_token = nil)
      capability = device_identity_token ? 'push-subscribe' : 'push-admin'
      post(push_path('/channelSubscriptions'), subscription, push_headers(capability, device_identity_token))
    end

    # List push channel subscriptions with cursor pagination
    def list_channel_push_subscriptions(params = {}, device_identity_token = nil)
      capability = device_identity_token ? 'push-subscribe' : 'push-admin'
      get(push_path('/channelSubscriptions'), params, push_headers(capability, device_identity_token))
    end

    # Delete push channel subscriptions
    def delete_channel_push_subscriptions(params = {}, device_identity_token = nil)
      capability = device_identity_token ? 'push-subscribe' : 'push-admin'
      delete(push_path('/channelSubscriptions'), params, push_headers(capability, device_identity_token))
    end

    # List subscribed channels with cursor pagination
    def list_channel_push_subscription_channels(params = {})
      get(push_path('/channelSubscriptions/channels'), params, push_headers('push-admin'))
    end

    # List stored push provider credentials with cursor pagination
    def list_push_credentials(params = {})
      get(push_path('/credentials'), params, push_headers('push-admin'))
    end

    # Store or update a provider credential payload
    def put_push_credential(provider, credential)
      post(push_path("/credentials/#{provider}"), credential, push_headers('push-admin'))
    end

    # Publish push asynchronously by default
    def publish_push(request)
      post(push_path('/publish'), request.merge(sync: false), push_headers('push-admin'))
    end

    # Alias of publish_push
    def publish_push_direct(request)
      publish_push(request)
    end

    # Publish a batch of push notifications asynchronously by default
    def publish_push_batch(requests)
      post(push_path('/batch/publish'), requests.map { |request| request.merge(sync: false) }, push_headers('push-admin'))
    end

    # Schedule a push publish; requires notBeforeMs in the request
    def schedule_push(request)
      raise Sockudo::Error, 'scheduled push requires notBeforeMs' unless request.key?(:notBeforeMs) || request.key?('notBeforeMs')

      publish_push(request)
    end

    # Get the status for a publish id
    def get_publish_status(publish_id)
      get(push_path("/publish/#{publish_id}/status"), {}, push_headers('push-admin'))
    end

    # Cancel a scheduled publish
    def cancel_scheduled_push(publish_id)
      delete(push_path("/scheduled/#{publish_id}"), {}, push_headers('push-admin'))
    end

    # Submit a provider delivery status event
    def post_push_delivery_status(event)
      post(push_path('/deliveryStatus'), event, push_headers('push-admin'))
    end

    # Trigger an event on one or more channels
    #
    # POST /apps/[app_id]/events
    #
    # @param channels [String or Array] 1-10 channel names
    # @param event_name [String]
    # @param data [Object] Event data to be triggered in javascript.
    #   Objects other than strings will be converted to JSON
    # @param params [Hash] Additional parameters to send to api, e.g socket_id.
    #   May include :extras => { headers: Hash, ephemeral: Boolean, idempotency_key: String, echo: Boolean }
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def trigger(channels, event_name, data, params = {})
      params = inject_auto_idempotency_key(params)
      body, headers = trigger_params_with_headers(channels, event_name, data, params)
      post_with_retry('/events', body, headers)
    end

    # Trigger multiple events at the same time
    #
    # POST /apps/[app_id]/batch_events
    #
    # @param events [Array] List of events to publish. Each event hash may
    #   include an :idempotency_key field for at-most-once delivery.
    #
    # @return [Hash] See Sockudo API docs
    #
    # @raise [Sockudo::Error] Unsuccessful response - see the error message
    # @raise [Sockudo::HTTPError] Error raised inside http client. The original error is wrapped in error.original_error
    #
    def trigger_batch(*events)
      flat_events = events.flatten
      inject_auto_idempotency_keys_batch!(flat_events)
      post_with_retry('/batch_events', trigger_batch_params(flat_events))
    end

    # Trigger an event on one or more channels asynchronously.
    # For parameters see #trigger
    #
    def trigger_async(channels, event_name, data, params = {})
      params = inject_auto_idempotency_key(params)
      body, headers = trigger_params_with_headers(channels, event_name, data, params)
      post_async('/events', body, headers)
    end

    # Trigger multiple events asynchronously.
    # For parameters see #trigger_batch
    #
    def trigger_batch_async(*events)
      flat_events = events.flatten
      inject_auto_idempotency_keys_batch!(flat_events)
      post_async('/batch_events', trigger_batch_params(flat_events))
    end

    # Generate the expected response for an authentication endpoint.
    # See https://sockudo.com/docs/channels/server_api/authorizing-users for details.
    #
    # @example Private channels
    #   render :json => Sockudo.authenticate('private-my_channel', params[:socket_id])
    #
    # @example Presence channels
    #   render :json => Sockudo.authenticate('presence-my_channel', params[:socket_id], {
    #     :user_id => current_user.id, # => required
    #     :user_info => { # => optional - for example
    #       :name => current_user.name,
    #       :email => current_user.email
    #     }
    #   })
    #
    # @param socket_id [String]
    # @param custom_data [Hash] used for example by private channels
    #
    # @return [Hash]
    #
    # @raise [Sockudo::Error] if channel_name or socket_id are invalid
    #
    # @private Custom data is sent to server as JSON-encoded string
    #
    def authenticate(channel_name, socket_id, custom_data = nil)
      channel_instance = channel(channel_name)
      r = channel_instance.authenticate(socket_id, custom_data)
      if channel_name.match(/^private-encrypted-/)
        r[:shared_secret] = Base64.strict_encode64(
          channel_instance.shared_secret(encryption_master_key)
        )
      end
      r
    end

    # Generate the expected response for a user authentication endpoint.
    # See https://sockudo.com/docs/authenticating_users for details.
    #
    # @example
    #   user_data = { id: current_user.id.to_s, company_id: current_user.company_id }
    #   render :json => Sockudo.authenticate_user(params[:socket_id], user_data)
    #
    # @param socket_id [String]
    # @param user_data [Hash] user's properties (id is required and must be a string)
    #
    # @return [Hash]
    #
    # @raise [Sockudo::Error] if socket_id or user_data is invalid
    #
    # @private Custom data is sent to server as JSON-encoded string
    #
    def authenticate_user(socket_id, user_data)
      validate_user_data(user_data)

      custom_data = MultiJson.encode(user_data)
      auth = authentication_string(socket_id, custom_data)

      { auth: auth, user_data: custom_data }
    end

    # @private Construct a net/http http client
    def sync_http_client
      require 'httpclient'

      @sync_http_client ||= HTTPClient.new(@http_proxy).tap do |c|
        c.connect_timeout = @connect_timeout
        c.send_timeout = @send_timeout
        c.receive_timeout = @receive_timeout
        c.keep_alive_timeout = @keep_alive_timeout
      end
    end

    # @private Construct an em-http-request http client
    def em_http_client(uri)
      unless defined?(EventMachine) && EventMachine.reactor_running?
        raise Error, 'In order to use async calling you must be running inside an eventmachine loop'
      end

      require 'em-http' unless defined?(EventMachine::HttpRequest)

      connection_opts = {
        connect_timeout: @connect_timeout,
        inactivity_timeout: @receive_timeout
      }

      if defined?(@proxy)
        proxy_opts = {
          host: @proxy[:host],
          port: @proxy[:port]
        }
        proxy_opts[:authorization] = [@proxy[:user], @proxy[:password]] if @proxy[:user]
        connection_opts[:proxy] = proxy_opts
      end

      EventMachine::HttpRequest.new(uri, connection_opts)
    end

    private

    include Sockudo::Utils

    def inject_auto_idempotency_key(params)
      return params if params.key?(:idempotency_key) || !@auto_idempotency_key

      serial = (@publish_serial += 1)
      params.merge(idempotency_key: "#{@base_id}:#{serial}")
    end

    def inject_auto_idempotency_keys_batch!(events)
      return false unless @auto_idempotency_key

      serial = (@publish_serial += 1)
      injected = false
      events.each_with_index do |event, index|
        next if event.key?(:idempotency_key)

        event[:idempotency_key] = "#{@base_id}:#{serial}:#{index}"
        injected = true
      end
      injected
    end

    def post_with_retry(path, body, headers = {})
      last_error = nil
      @max_retries.times do |attempt|
        return post(path, body, headers)
      rescue Sockudo::HTTPError => e
        last_error = e
        raise unless attempt < @max_retries - 1
      rescue Sockudo::Error => e
        raise unless e.respond_to?(:status) && e.status.is_a?(Integer) && e.status >= 500 && e.status < 600

        last_error = e
        raise unless attempt < @max_retries - 1
      end
      raise last_error
    end

    def trigger_params(channels, event_name, data, params)
      channels = Array(channels).map(&:to_s)
      raise Sockudo::Error, "Too many channels (#{channels.length}), max 100" if channels.length > 100

      encoded_data = if channels.any? { |c| c.match(/^private-encrypted-/) }
                       if channels.length > 1
                         raise Sockudo::Error,
                               'Cannot trigger to multiple channels if any are encrypted'
                       end

                       encrypt(channels[0], encode_data(data))
                     else
                       encode_data(data)
                     end

      params.merge({
                     name: event_name,
                     channels: channels,
                     data: encoded_data
                   })
    end

    def trigger_params_with_headers(channels, event_name, data, params)
      params = params.dup
      idempotency_key = params.delete(:idempotency_key)
      body = trigger_params(channels, event_name, data, params)
      headers = {}
      if idempotency_key
        body[:idempotency_key] = idempotency_key
        headers['X-Idempotency-Key'] = idempotency_key
      end
      [body, headers]
    end

    def trigger_batch_params(events)
      {
        batch: events.map do |event|
          event.dup.tap do |e|
            e[:data] = if e[:channel].match(/^private-encrypted-/)
                         encrypt(e[:channel], encode_data(e[:data]))
                       else
                         encode_data(e[:data])
                       end
          end
        end
      }
    end

    # JSON-encode the data if it's not a string
    def encode_data(data)
      return data if data.is_a? String

      MultiJson.encode(data)
    end

    # Encrypts a message with a key derived from the master key and channel
    # name
    def encrypt(channel_name, encoded_data)
      raise ConfigurationError, :encryption_master_key unless @encryption_master_key

      # Only now load rbnacl, so that people that aren't using it don't need to
      # install libsodium
      require_rbnacl

      secret_box = RbNaCl::SecretBox.new(
        channel(channel_name).shared_secret(@encryption_master_key)
      )

      nonce = RbNaCl::Random.random_bytes(secret_box.nonce_bytes)
      ciphertext = secret_box.encrypt(nonce, encoded_data)

      MultiJson.encode({
                         'nonce' => Base64.strict_encode64(nonce),
                         'ciphertext' => Base64.strict_encode64(ciphertext)
                       })
    end

    def configured?
      host && scheme && key && secret && app_id
    end

    def require_rbnacl
      require 'rbnacl'
    rescue LoadError => e
      warn "You don't have rbnacl installed in your application. Please add it to your Gemfile and run bundle install"
      raise e
    end

    # Compute authentication string required as part of the user authentication
    # endpoint response. Generally the authenticate method should be used in
    # preference to this one.
    #
    # @param socket_id [String] Each Sockudo socket connection receives a
    #   unique socket_id. This is sent from sockudo.js to your server when
    #   channel authentication is required.
    # @param custom_string [String] Allows signing additional data
    # @return [String]
    #
    # @raise [Sockudo::Error] if socket_id or custom_string invalid
    #
    def authentication_string(socket_id, custom_string = nil)
      string_to_sign = [socket_id, 'user', custom_string].compact.join('::')

      _authentication_string(socket_id, string_to_sign, authentication_token, string_to_sign)
    end

    def validate_user_data(user_data)
      return if user_data_valid?(user_data)

      raise Sockudo::Error, "Invalid user data #{user_data.inspect}"
    end

    def user_data_valid?(data)
      data.is_a?(Hash) && data.key?(:id) && !data[:id].empty? && data[:id].is_a?(String)
    end

    def push_headers(capability = 'push-admin', device_identity_token = nil, rotate_device_identity_token = false)
      headers = { 'X-Sockudo-Push-Capability' => capability }
      headers['X-Sockudo-Device-Identity-Token'] = device_identity_token if device_identity_token
      headers['X-Sockudo-Rotate-Device-Identity-Token'] = 'true' if rotate_device_identity_token
      headers
    end

    def push_path(path)
      "/push#{path}"
    end
  end
end
