# frozen_string_literal: true

require 'pusher-signature'
require 'digest/md5'
require 'multi_json'
require 'openssl'

module Sockudo
  class Request
    attr_reader :body, :params

    def initialize(client, verb, uri, params, body = nil, extra_headers = {})
      @client = client
      @verb = verb
      @uri = uri
      @head = {
        'X-Pusher-Library' => "sockudo-http-ruby #{Sockudo::VERSION}"
      }
      @head.merge!(extra_headers) if extra_headers && !extra_headers.empty?

      @body = body
      params = params.each_with_object({}) { |(key, value), result| result[key.to_s] = value }
      if body
        params['body_md5'] = Digest::MD5.hexdigest(body)
        @head['Content-Type'] = 'application/json'
      end

      @params = signed_params(params)
    end

    def send_sync
      http = @client.sync_http_client

      begin
        response = http.request(@verb, @uri, @params, @body, @head)
      rescue HTTPClient::BadResponseError, HTTPClient::TimeoutError,
             SocketError, Errno::ECONNREFUSED => e
        error = Sockudo::HTTPError.new("#{e.message} (#{e.class})")
        error.original_error = e
        raise error
      end

      body = response.body&.chomp

      handle_response(response.code.to_i, body)
    end

    def send_async
      if defined?(EventMachine) && EventMachine.reactor_running?
        http_client = @client.em_http_client(@uri)
        df = EM::DefaultDeferrable.new

        http = case @verb
               when :post
                 http_client.post({
                                    query: @params, body: @body, head: @head
                                  })
               when :get
                 http_client.get({
                                   query: @params, head: @head
                                 })
               when :delete
                 http_client.delete({
                                      query: @params, head: @head
                                    })
               else
                 raise 'Unsupported verb'
               end
        http.callback do
          df.succeed(handle_response(http.response_header.status, http.response.chomp))
        rescue StandardError => e
          df.fail(e)
        end
        http.errback do |_e|
          message = "Network error connecting to sockudo (#{http.error})"
          Sockudo.logger.debug(message)
          df.fail(Error.new(message))
        end

        df
      else
        http = @client.sync_http_client

        http.request_async(@verb, @uri, @params, @body, @head)
      end
    end

    private

    def signed_params(params)
      @client.authentication_token
      params = params.merge({
                              'auth_key' => @client.key,
                              'auth_timestamp' => Time.now.to_i.to_s,
                              'auth_version' => '1.0'
                            })
      params_for_signing = params.each_with_object({}) do |(key, value), result|
        result[key.downcase] = value
      end
      canonical_query = params_for_signing.sort.map { |key, value| "#{key}=#{value}" }.join('&')
      string_to_sign = [@verb.to_s.upcase, @uri.path, canonical_query].join("\n")
      params.merge('auth_signature' => OpenSSL::HMAC.hexdigest('sha256', @client.secret, string_to_sign))
    end

    def handle_response(status_code, body)
      case status_code
      when 200, 201
        symbolize_first_level(MultiJson.decode(body))
      when 202
        body.empty? || symbolize_first_level(MultiJson.decode(body))
      when 204
        {}
      when 400
        raise Error, "Bad request: #{body}"
      when 401
        raise AuthenticationError, body
      when 404
        raise Error, "404 Not found (#{@uri.path})"
      when 407
        raise Error, 'Proxy Authentication Required'
      when 413
        raise Error, 'Payload Too Large > 10KB'
      else
        raise Error, "Unknown error (status code #{status_code}): #{body}"
      end
    end

    def symbolize_first_level(hash)
      hash.each_with_object({}) do |(key, value), result|
        result[key.to_sym] = deep_symbolize(value)
      end
    end

    def deep_symbolize(value)
      case value
      when Hash
        value.each_with_object({}) do |(key, nested_value), result|
          result[key.to_sym] = deep_symbolize(nested_value)
        end
      when Array
        value.map { |item| deep_symbolize(item) }
      else
        value
      end
    end
  end
end
