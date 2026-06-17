# frozen_string_literal: true

require 'multi_json'
require 'openssl'

module Sockudo
  # Used to parse and authenticate WebHooks
  #
  # @example Sinatra
  #   post '/webhooks' do
  #     webhook = Sockudo::WebHook.new(request)
  #     if webhook.valid?
  #       webhook.events.each do |event|
  #         case event["name"]
  #         when 'channel_occupied'
  #           puts "Channel occupied: #{event["channel"]}"
  #         when 'channel_vacated'
  #           puts "Channel vacated: #{event["channel"]}"
  #         end
  #       end
  #     else
  #       status 401
  #     end
  #     return
  #   end
  #
  class WebHook
    attr_reader :key, :signature

    # Provide either a Rack::Request or a Hash containing :key, :signature,
    # :body, and :content_type (optional)
    #
    def initialize(request, client = Sockudo)
      @client = client
      # For Rack::Request and ActionDispatch::Request
      if request.respond_to?(:env) && request.respond_to?(:content_type)
        @key = request.env['HTTP_X_PUSHER_KEY']
        @signature = request.env['HTTP_X_PUSHER_SIGNATURE']
        @content_type = request.content_type

        request.body.rewind
        @body = request.body.read
        request.body.rewind
      else
        @key, @signature, @body = request.values_at(:key, :signature, :body)
        @content_type = request[:content_type] || 'application/json'
      end
    end

    # Returns whether the WebHook is valid by checking that the signature
    # matches the configured key & secret. In the case that the webhook is
    # invalid, the reason is logged
    #
    # @param extra_tokens [Hash] If you have extra tokens for your Sockudo app, you can specify them
    #   so that they're used to attempt validation.
    #
    def valid?(extra_tokens = nil)
      extra_tokens = [extra_tokens] if extra_tokens.is_a?(Hash)
      if @key == @client.key
        return signature_valid?(@client.secret)
      elsif extra_tokens
        extra_tokens.each do |token|
          return signature_valid?(token[:secret]) if @key == token[:key]
        end
      end

      Sockudo.logger.warn "Received webhook with unknown key: #{key}"
      false
    end

    # Array of events (as Hashes) contained inside the webhook
    #
    def events
      data['events']
    end

    # The time at which the WebHook was initially triggered by Sockudo, i.e.
    # when the event occurred
    #
    # @return [Time]
    #
    def time
      Time.at(data['time_ms'].to_f / 1000)
    end

    # Access the parsed WebHook body
    #
    def data
      @data ||= case @content_type
                when 'application/json'
                  MultiJson.decode(@body)
                else
                  raise "Unknown Content-Type (#{@content_type})"
                end
    end

    private

    # Checks signature against secret and returns boolean
    #
    def signature_valid?(secret)
      digest = OpenSSL::Digest.new('SHA256')
      expected = OpenSSL::HMAC.hexdigest(digest, secret, @body)
      return true if @signature == expected

      Sockudo.logger.warn "Received WebHook with invalid signature: got #{@signature}, expected #{expected}"
      false
    end
  end
end
