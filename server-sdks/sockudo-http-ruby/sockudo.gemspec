# frozen_string_literal: true

require File.expand_path('lib/sockudo/version', __dir__)

Gem::Specification.new do |s|
  s.name        = 'sockudo'
  s.version     = Sockudo::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ['Sockudo']
  s.email       = ['support@sockudo.com']
  s.homepage    = 'http://github.com/sockudo/sockudo-http-ruby'
  s.summary     = 'Sockudo API client'
  s.description = 'Wrapper for Sockudo REST API'
  s.license     = 'MIT'

  s.required_ruby_version = '>= 2.6'

  s.add_dependency 'httpclient', '~> 2.8'
  s.add_dependency 'jruby-openssl' if defined?(JRUBY_VERSION)
  s.add_dependency 'logger', '~> 1.7'
  s.add_dependency 'multi_json', '~> 1.15'
  s.add_dependency 'pusher-signature', '~> 0.1.8'

  s.add_development_dependency 'addressable', '~> 2.7'
  s.add_development_dependency 'em-http-request', '~> 1.1'
  s.add_development_dependency 'json', '~> 2.3'
  s.add_development_dependency 'rack', '~> 2.2'
  s.add_development_dependency 'rake', '~> 13.0'
  s.add_development_dependency 'rbnacl', '~> 7.1'
  s.add_development_dependency 'rspec', '~> 3.9'
  s.add_development_dependency 'webmock', '~> 3.9'

  s.files         = Dir['lib/**/*'] + %w[CHANGELOG.md LICENSE README.md]
  s.require_paths = ['lib']
  s.metadata['rubygems_mfa_required'] = 'true'
end
