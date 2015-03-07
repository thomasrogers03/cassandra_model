source 'http://rubygems.org'

gemspec

group :development do
  require 'yaml'
  gem 'cassandra-driver', '~> 1.1'
  gem 'activesupport', require: 'active_support/all'
  gem 'thomas_utils', '~> 0.1.4', git: 'https://github.com/thomasrogers03/thomas_utils.git'
  gem 'concurrent-ruby', require: 'concurrent'
end

group :test do
  gem 'rspec', '~> 3.1.0'
  gem 'guard-rspec'
  gem 'guard-bundler'
  gem 'guard'
end
