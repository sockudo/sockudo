# frozen_string_literal: true

require 'rubygems'
require 'sockudo'
require 'eventmachine'
require 'em-http-request'

# To get these values:
# - Go to your Sockudo dashboard
# - Click on Choose App.
# - Click on one of your apps
# - Click API Access
Sockudo.app_id = 'your_app_id'
Sockudo.key = 'your_key'
Sockudo.secret = 'your_secret'

EM.run do
  deferrable = Sockudo['test_channel'].trigger_async('my_event', 'hi')

  deferrable.callback do # called on success
    puts 'Message sent successfully.'
    EM.stop
  end
  deferrable.errback do |error| # called on error
    puts 'Message could not be sent.'
    puts error
    EM.stop
  end
end
