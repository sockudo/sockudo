# frozen_string_literal: true

require 'spec_helper'

require 'rack'
require 'stringio'

describe Sockudo::WebHook do
  def forward_compat_fixture(name)
    directory = __dir__
    until directory == File.dirname(directory)
      candidate = File.join(directory, 'tests', 'ai-conformance', 'fixtures', 'forward-compat', name)
      return File.read(candidate) if File.file?(candidate)

      directory = File.dirname(directory)
    end

    raise "Forward compatibility fixture not found: #{name}"
  end

  before :each do
    @hook_data = {
      'time_ms' => 123_456,
      'events' => [
        { 'name' => 'foo' }
      ]
    }
  end

  describe 'initialization' do
    it 'can be initialized with Rack::Request' do
      request = Rack::Request.new({
                                    'HTTP_X_PUSHER_KEY' => '1234',
                                    'HTTP_X_PUSHER_SIGNATURE' => 'asdf',
                                    'CONTENT_TYPE' => 'application/json',
                                    'rack.input' => StringIO.new(MultiJson.dump(@hook_data))
                                  })
      wh = Sockudo::WebHook.new(request)
      expect(wh.key).to eq('1234')
      expect(wh.signature).to eq('asdf')
      expect(wh.data).to eq(@hook_data)
    end

    it 'can be initialized with a hash' do
      request = {
        key: '1234',
        signature: 'asdf',
        content_type: 'application/json',
        body: MultiJson.dump(@hook_data)
      }
      wh = Sockudo::WebHook.new(request)
      expect(wh.key).to eq('1234')
      expect(wh.signature).to eq('asdf')
      expect(wh.data).to eq(@hook_data)
    end
  end

  describe 'after initialization' do
    before :each do
      @body = MultiJson.dump(@hook_data)
      request = {
        key: '1234',
        signature: hmac('asdf', @body),
        content_type: 'application/json',
        body: @body
      }

      @client = Sockudo::Client.new
      @wh = Sockudo::WebHook.new(request, @client)
    end

    it 'should validate' do
      @client.key = '1234'
      @client.secret = 'asdf'
      expect(@wh).to be_valid
    end

    it 'should not validate if key is wrong' do
      @client.key = '12345'
      @client.secret = 'asdf'
      expect(Sockudo.logger).to receive(:warn).with('Received webhook with unknown key: 1234')
      expect(@wh).not_to be_valid
    end

    it 'should not validate if secret is wrong' do
      @client.key = '1234'
      @client.secret = 'asdfxxx'
      digest = OpenSSL::Digest.new('SHA256')
      expected = OpenSSL::HMAC.hexdigest(digest, @client.secret, @body)
      expect(Sockudo.logger).to receive(:warn).with("Received WebHook with invalid signature: got #{@wh.signature}, expected #{expected}")
      expect(@wh).not_to be_valid
    end

    it 'should validate with an extra token' do
      @client.key = '12345'
      @client.secret = 'xxx'
      expect(@wh.valid?({ key: '1234', secret: 'asdf' })).to be_truthy
    end

    it 'should validate with an array of extra tokens' do
      @client.key = '123456'
      @client.secret = 'xxx'
      expect(@wh.valid?([
                          { key: '12345', secret: 'wtf' },
                          { key: '1234', secret: 'asdf' }
                        ])).to be_truthy
    end

    it 'should not validate if all keys are wrong with extra tokens' do
      @client.key = '123456'
      @client.secret = 'asdf'
      expect(Sockudo.logger).to receive(:warn).with('Received webhook with unknown key: 1234')
      expect(@wh.valid?({ key: '12345', secret: 'asdf' })).to be_falsey
    end

    it 'should not validate if secret is wrong with extra tokens' do
      @client.key = '123456'
      @client.secret = 'asdfxxx'
      expect(Sockudo.logger).to receive(:warn).with(/Received WebHook with invalid signature/)
      expect(@wh.valid?({ key: '1234', secret: 'wtf' })).to be_falsey
    end

    it 'should expose events' do
      expect(@wh.events).to eq(@hook_data['events'])
    end

    it 'should expose time' do
      expect(@wh.time).to eq(Time.at(123.456))
    end

    it 'accepts the shared future webhook event fixture' do
      body = forward_compat_fixture('future-webhook-events.json')
      request = {
        key: '1234',
        signature: hmac('asdf', body),
        content_type: 'application/json',
        body: body
      }

      wh = Sockudo::WebHook.new(request, @client)
      @client.key = '1234'
      @client.secret = 'asdf'

      expect(wh).to be_valid
      expect(wh.time).to eq(Time.at(1_710_000_000))
      expect(wh.events[0]['name']).to eq('member_updated')
      expect(wh.events[0]['future_field']).to eq('must-pass-through')
      expect(wh.events[1]['name']).to eq('ai_run_started')
      expect(wh.events[1]['run_id']).to eq('run-1')
      expect(wh.events[2]['version_serial']).to eq('ver-1')
    end

    it 'preserves nested future webhook values' do
      body = MultiJson.dump({
                              'time_ms' => 1_710_000_000_000,
                              'events' => [
                                {
                                  'name' => 'ai_turn_started',
                                  'channel' => 'private-ai-forward',
                                  'data' => {
                                    'turn_id' => 'turn-1',
                                    'tokens' => %w[hello world],
                                    'done' => false,
                                    'nullable' => nil
                                  },
                                  'future_field' => { 'nested' => true }
                                }
                              ]
                            })
      request = {
        key: '1234',
        signature: hmac('asdf', body),
        content_type: 'application/json',
        body: body
      }

      wh = Sockudo::WebHook.new(request, @client)
      @client.key = '1234'
      @client.secret = 'asdf'

      expect(wh).to be_valid
      expect(wh.events[0]['name']).to eq('ai_turn_started')
      expect(wh.events[0]['data']['turn_id']).to eq('turn-1')
      expect(wh.events[0]['data']['tokens']).to eq(%w[hello world])
      expect(wh.events[0]['data']['done']).to be(false)
      expect(wh.events[0]['data']['nullable']).to be_nil
      expect(wh.events[0]['future_field']['nested']).to be(true)
    end
  end
end
