# frozen_string_literal: true

require 'spec_helper'

describe Sockudo::Channel do
  before do
    @client = Sockudo::Client.new({
                                    app_id: '20',
                                    key: '12345678900000001',
                                    secret: '12345678900000001',
                                    host: 'localhost',
                                    port: 80
                                  })
    @channel = @client['test_channel']
  end

  let(:sockudo_url_regexp) { %r{/apps/20/events} }

  def stub_post(status, body = nil)
    options = { status: status }
    options.merge!({ body: body }) if body

    stub_request(:post, sockudo_url_regexp).to_return(options)
  end

  def stub_post_to_raise(err)
    stub_request(:post, sockudo_url_regexp).to_raise(err)
  end

  describe '#trigger!' do
    it 'should use @client.trigger internally' do
      expect(@client).to receive(:trigger)
      @channel.trigger!('new_event', 'Some data')
    end
  end

  describe '#trigger' do
    it 'should log failure if error raised in http call' do
      stub_post_to_raise(HTTPClient::BadResponseError)

      expect(Sockudo.logger).to receive(:error).with('Exception from WebMock (HTTPClient::BadResponseError) (Sockudo::HTTPError)')
      expect(Sockudo.logger).to receive(:debug) # backtrace
      @channel.trigger('new_event', 'Some data')
    end

    it 'should log failure if Sockudo returns an error response' do
      stub_post 401, 'some signature info'
      expect(Sockudo.logger).to receive(:error).with('some signature info (Sockudo::AuthenticationError)')
      expect(Sockudo.logger).to receive(:debug) # backtrace
      @channel.trigger('new_event', 'Some data')
    end
  end

  describe '#initialization' do
    it 'should not be too long' do
      expect { @client['b' * 201] }.to raise_error(Sockudo::Error)
    end

    it 'should not use bad characters' do
      expect { @client['*^!±`/""'] }.to raise_error(Sockudo::Error)
    end
  end

  describe '#trigger_async' do
    it 'should use @client.trigger_async internally' do
      expect(@client).to receive(:trigger_async)
      @channel.trigger_async('new_event', 'Some data')
    end
  end

  describe '#info' do
    it 'should call the Client#channel_info' do
      expect(@client).to receive(:get)
        .with('/channels/mychannel', anything)
        .and_return({ occupied: true, subscription_count: 12 })
      @channel = @client['mychannel']
      @channel.info
    end

    it 'should assemble the requested attributes into the info option' do
      expect(@client).to receive(:get)
        .with(anything, { info: 'user_count,connection_count' })
        .and_return({ occupied: true, subscription_count: 12, user_count: 12 })
      @channel = @client['presence-foo']
      @channel.info(%w[user_count connection_count])
    end
  end

  describe '#users' do
    it 'should call the Client#channel_users' do
      expect(@client).to receive(:get).with('/channels/presence-mychannel/users',
                                            {}).and_return({ users: { 'id' => '4' } })
      @channel = @client['presence-mychannel']
      @channel.users
    end
  end

  describe '#history' do
    it 'should call the Client#channel_history' do
      expect(@client).to receive(:get)
        .with('/channels/mychannel/history', { limit: 2, direction: 'newest_first' })
        .and_return({ items: [{ serial: 2 }, { serial: 1 }] })
      @channel = @client['mychannel']
      @channel.history(limit: 2, direction: 'newest_first')
    end
  end

  describe '#presence_history' do
    it 'should call the Client#channel_presence_history' do
      expect(@client).to receive(:get)
        .with('/channels/presence-mychannel/presence/history', { limit: 2,
                                                                 direction: 'newest_first' })
        .and_return({ items: [{ serial: 2 }, { serial: 1 }] })
      @channel = @client['presence-mychannel']
      @channel.presence_history(limit: 2, direction: 'newest_first')
    end
  end

  describe '#presence_snapshot' do
    it 'should call the Client#channel_presence_snapshot' do
      expect(@client).to receive(:get)
        .with('/channels/presence-mychannel/presence/history/snapshot', { at_serial: 4 })
        .and_return({ member_count: 1 })
      @channel = @client['presence-mychannel']
      @channel.presence_snapshot(at_serial: 4)
    end
  end

  describe '#authentication_string' do
    def authentication_string(*data)
      -> { @channel.authentication_string(*data) }
    end

    it 'should return an authentication string given a socket id' do
      auth = @channel.authentication_string('1.1')

      expect(auth).to eq('12345678900000001:02259dff9a2a3f71ea8ab29ac0c0c0ef7996c8f3fd3702be5533f30da7d7fed4')
    end

    it 'should raise error if authentication is invalid' do
      [nil, ''].each do |invalid|
        expect(authentication_string(invalid)).to raise_error Sockudo::Error
      end
    end

    describe 'with extra string argument' do
      it 'should be a string or nil' do
        expect(authentication_string('1.1', 123)).to raise_error Sockudo::Error
        expect(authentication_string('1.1', {})).to raise_error Sockudo::Error

        expect(authentication_string('1.1', 'boom')).not_to raise_error
        expect(authentication_string('1.1', nil)).not_to raise_error
      end

      it 'should return an authentication string given a socket id and custom args' do
        auth = @channel.authentication_string('1.1', 'foobar')

        expect(auth).to eq("12345678900000001:#{hmac(@client.secret, '1.1:test_channel:foobar')}")
      end
    end
  end

  describe '#authenticate' do
    before :each do
      @custom_data = { uid: 123, info: { name: 'Foo' } }
    end

    it 'should return a hash with signature including custom data and data as json string' do
      allow(MultiJson).to receive(:encode).with(@custom_data).and_return 'a json string'

      response = @channel.authenticate('1.1', @custom_data)

      expect(response).to eq({
                               auth: "12345678900000001:#{hmac(@client.secret, '1.1:test_channel:a json string')}",
                               channel_data: 'a json string'
                             })
    end

    it 'should fail on invalid socket_ids' do
      expect do
        @channel.authenticate('1.1:')
      end.to raise_error Sockudo::Error

      expect do
        @channel.authenticate('1.1foo', 'channel')
      end.to raise_error Sockudo::Error

      expect do
        @channel.authenticate(':1.1')
      end.to raise_error Sockudo::Error

      expect do
        @channel.authenticate('foo1.1', 'channel')
      end.to raise_error Sockudo::Error

      expect do
        @channel.authenticate('foo', 'channel')
      end.to raise_error Sockudo::Error
    end
  end

  describe `#shared_secret` do
    before(:each) do
      @channel.instance_variable_set(:@name, 'private-encrypted-1')
    end

    it 'should return a shared_secret based on the channel name and encryption master key' do
      key = '3W1pfB/Etr+ZIlfMWwZP3gz8jEeCt4s2pe6Vpr+2c3M='
      shared_secret = @channel.shared_secret(key)
      expect(Base64.strict_encode64(shared_secret)).to eq(
        '6zeEp/chneRPS1cbK/hGeG860UhHomxSN6hTgzwT20I='
      )
    end

    it 'should return nil if missing encryption master key' do
      shared_secret = @channel.shared_secret(nil)
      expect(shared_secret).to be_nil
    end
  end
end
