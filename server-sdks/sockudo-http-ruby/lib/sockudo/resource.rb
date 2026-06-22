# frozen_string_literal: true

module Sockudo
  class Resource
    def initialize(client, path)
      @client = client
      @path = path
    end

    def get(params, headers = {})
      create_request(:get, params, nil, headers).send_sync
    end

    def get_async(params, headers = {})
      create_request(:get, params, nil, headers).send_async
    end

    def post(params, headers = {})
      body = MultiJson.dump(params)
      create_request(:post, {}, body, headers).send_sync
    end

    def post_async(params, headers = {})
      body = MultiJson.dump(params)
      create_request(:post, {}, body, headers).send_async
    end

    def delete(params, headers = {})
      create_request(:delete, params, nil, headers).send_sync
    end

    private

    def create_request(verb, params, body = nil, extra_headers = {})
      Request.new(@client, verb, url, params, body, extra_headers)
    end

    def url
      @url ||= @client.url(@path)
    end
  end
end
