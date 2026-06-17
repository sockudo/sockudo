# frozen_string_literal: true

require 'sinatra'
require 'sinatra/cookies'
require 'sinatra/json'
require 'sockudo'

# You can get these variables from http://dashboard.sockudo.com
sockudo = Sockudo::Client.new(
  app_id: 'your-app-id',
  key: 'your-app-key',
  secret: 'your-app-secret',
  cluster: 'your-app-cluster'
)

set :public_folder, 'public'

get '/' do
  redirect '/presence_channels.html'
end

# Emulate rails behaviour where this information would be stored in session
get '/signin' do
  cookies[:user_id] = 'example_cookie'
  'Ok'
end

# Auth endpoint: https://sockudo.com/docs/channels/server_api/authenticating-users
post '/sockudo/auth' do
  channel_data = {
    user_id: 'example_user',
    user_info: {
      name: 'example_name',
      email: 'example_email'
    }
  }

  if cookies[:user_id] == 'example_cookie'
    response = sockudo.authenticate(params[:channel_name], params[:socket_id], channel_data)
    json response
  else
    status 403
  end
end

get '/sockudo_trigger' do
  channels = ['presence-channel-test']

  begin
    sockudo.trigger(channels, 'test-event', {
                      message: 'hello world'
                    })
  rescue Sockudo::Error
    # For example, Sockudo::AuthenticationError, Sockudo::HTTPError, or Sockudo::Error
  end

  'Triggered!'
end
