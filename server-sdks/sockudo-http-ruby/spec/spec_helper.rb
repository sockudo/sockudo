# frozen_string_literal: true

begin
  require 'bundler/setup'
rescue LoadError
  puts 'although not required, it is recommended that you use bundler when running the tests'
end

ENV['SOCKUDO_URL'] = 'http://some:secret@api.secret.localhost:441/apps/54'

require 'rspec'
require 'em-http' # As of webmock 1.4.0, em-http must be loaded first
require 'webmock/rspec'

require 'sockudo'
require 'eventmachine'

RSpec.configure do |config|
  config.before(:each) do
    WebMock.reset!
    WebMock.disable_net_connect!
  end
end

def hmac(key, data)
  digest = OpenSSL::Digest.new('SHA256')
  OpenSSL::HMAC.hexdigest(digest, key, data)
end
