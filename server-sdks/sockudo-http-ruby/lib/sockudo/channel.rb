# frozen_string_literal: true

require 'openssl'
require 'multi_json'

module Sockudo
  # Delegates operations for a specific channel from a client
  class Channel
    attr_reader :name

    INVALID_CHANNEL_REGEX = /[^A-Za-z0-9_\-=@,.;:]/.freeze

    def initialize(_, name, client = Sockudo)
      if Sockudo::Channel::INVALID_CHANNEL_REGEX.match(name)
        raise Sockudo::Error, "Illegal channel name '#{name}'"
      elsif name.length > 200
        raise Sockudo::Error, "Channel name too long (limit 164 characters) '#{name}'"
      end

      @name = name
      @client = client
    end

    # Trigger event asynchronously using EventMachine::HttpRequest
    #
    # [Deprecated] This method will be removed in a future gem version. Please
    # switch to Sockudo.trigger_async or Sockudo::Client#trigger_async instead
    #
    # @param (see #trigger!)
    # @return [EM::DefaultDeferrable]
    #   Attach a callback to be notified of success (with no parameters).
    #   Attach an errback to be notified of failure (with an error parameter
    #   which includes the HTTP status code returned)
    # @raise [LoadError] unless em-http-request gem is available
    # @raise [Sockudo::Error] unless the eventmachine reactor is running. You
    #   probably want to run your application inside a server such as thin
    #
    def trigger_async(event_name, data, socket_id = nil)
      params = {}
      if socket_id
        validate_socket_id(socket_id)
        params[:socket_id] = socket_id
      end
      @client.trigger_async(name, event_name, data, params)
    end

    # Trigger event
    #
    # [Deprecated] This method will be removed in a future gem version. Please
    # switch to Sockudo.trigger or Sockudo::Client#trigger instead
    #
    # @example
    #   begin
    #     Sockudo['my-channel'].trigger!('an_event', {:some => 'data'})
    #   rescue Sockudo::Error => e
    #     # Do something on error
    #   end
    #
    # @param data [Object] Event data to be triggered in javascript.
    #   Objects other than strings will be converted to JSON
    # @param socket_id Allows excluding a given socket_id from receiving the
    #   event - see http://sockudo.com/docs/publisher_api_guide/publisher_excluding_recipients for more info
    #
    # @raise [Sockudo::Error] on invalid Sockudo response - see the error message for more details
    # @raise [Sockudo::HTTPError] on any error raised inside http client - the original error is available in the original_error attribute
    #
    def trigger!(event_name, data, socket_id = nil)
      params = {}
      if socket_id
        validate_socket_id(socket_id)
        params[:socket_id] = socket_id
      end
      @client.trigger(name, event_name, data, params)
    end

    # Trigger event, catching and logging any errors.
    #
    # [Deprecated] This method will be removed in a future gem version. Please
    # switch to Sockudo.trigger or Sockudo::Client#trigger instead
    #
    # @note CAUTION! No exceptions will be raised on failure
    # @param (see #trigger!)
    #
    def trigger(event_name, data, socket_id = nil)
      trigger!(event_name, data, socket_id)
    rescue Sockudo::Error => e
      Sockudo.logger.error("#{e.message} (#{e.class})")
      Sockudo.logger.debug(e.backtrace.join("\n"))
    end

    # Request info for a channel
    #
    # @example Response
    #   [{:occupied=>true, :subscription_count => 12}]
    #
    # @param info [Array] Array of attributes required (as lowercase strings)
    # @return [Hash] Hash of requested attributes for this channel
    # @raise [Sockudo::Error] on invalid Sockudo response - see the error message for more details
    # @raise [Sockudo::HTTPError] on any error raised inside http client - the original error is available in the original_error attribute
    #
    def info(attributes = [])
      @client.channel_info(name, info: attributes.join(','))
    end

    # Request durable history for this channel.
    #
    # @param params [Hash] History API query params. Supported keys include:
    #   :limit, :direction, :cursor, :start_serial, :end_serial, :start_time_ms, :end_time_ms
    # @return [Hash] History page payload for this channel
    #
    def history(params = {})
      @client.channel_history(name, params)
    end

    # Request presence history for a presence channel.
    #
    # @param params [Hash] Presence history API query params. Supported keys include:
    #   :limit, :direction, :cursor, :start_serial, :end_serial, :start_time_ms, :end_time_ms
    # @return [Hash] Presence history page payload for this channel
    #
    def presence_history(params = {})
      @client.channel_presence_history(name, params)
    end

    # Request a reconstructed presence snapshot for a presence channel.
    #
    # @param params [Hash] Snapshot API query params. Supported keys include:
    #   :at_time_ms, :at_serial
    # @return [Hash] Presence snapshot payload for this channel
    #
    def presence_snapshot(params = {})
      @client.channel_presence_snapshot(name, params)
    end

    # Fetch the latest visible version of a mutable message.
    def get_message(message_serial)
      @client.get_message(name, message_serial)
    end

    # Fetch preserved versions of a mutable message.
    def get_message_versions(message_serial, params = {})
      @client.get_message_versions(name, message_serial, params)
    end

    # Apply a mutable-message update.
    def update_message(message_serial, params = {})
      @client.update_message(name, message_serial, params)
    end

    # Apply a mutable-message delete.
    def delete_message(message_serial, params = {})
      @client.delete_message(name, message_serial, params)
    end

    # Apply a mutable-message append.
    def append_message(message_serial, params = {})
      @client.append_message(name, message_serial, params)
    end

    # Publish an annotation for a versioned message.
    def publish_annotation(message_serial, params = {})
      @client.publish_annotation(name, message_serial, params)
    end

    # Delete an annotation from a versioned message.
    def delete_annotation(message_serial, annotation_serial, params = {})
      @client.delete_annotation(name, message_serial, annotation_serial, params)
    end

    # List raw annotation events for a versioned message.
    def list_annotations(message_serial, params = {})
      @client.list_annotations(name, message_serial, params)
    end

    # Request users for a presence channel
    # Only works on presence channels (see: http://sockudo.com/docs/client_api_guide/client_presence_channels and https://sockudo.com/docs/rest_api)
    #
    # @example Response
    #   [{:id=>"4"}]
    #
    # @param params [Hash] Hash of parameters for the API - see REST API docs
    # @return [Hash] Array of user hashes for this channel
    # @raise [Sockudo::Error] on invalid Sockudo response - see the error message for more details
    # @raise [Sockudo::HTTPError] on any error raised inside Net::HTTP - the original error is available in the original_error attribute
    #
    def users(params = {})
      @client.channel_users(name, params)[:users]
    end

    # Compute authentication string required as part of the authentication
    # endpoint response. Generally the authenticate method should be used in
    # preference to this one
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
      string_to_sign = [socket_id, name, custom_string].compact.join(':')

      _authentication_string(socket_id, string_to_sign, @client.authentication_token, custom_string)
    end

    # Generate the expected response for an authentication endpoint.
    # See http://sockudo.com/docs/authenticating_users for details.
    #
    # @example Private channels
    #   render :json => Sockudo['private-my_channel'].authenticate(params[:socket_id])
    #
    # @example Presence channels
    #   render :json => Sockudo['presence-my_channel'].authenticate(params[:socket_id], {
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
    # @raise [Sockudo::Error] if socket_id or custom_data is invalid
    #
    # @private Custom data is sent to server as JSON-encoded string
    #
    def authenticate(socket_id, custom_data = nil)
      custom_data = MultiJson.encode(custom_data) if custom_data
      auth = authentication_string(socket_id, custom_data)
      r = { auth: auth }
      r[:channel_data] = custom_data if custom_data
      r
    end

    def shared_secret(encryption_master_key)
      return unless encryption_master_key

      secret_string = @name + encryption_master_key
      digest = OpenSSL::Digest.new('SHA256')
      digest << secret_string
      digest.digest
    end

    include Sockudo::Utils
  end
end
