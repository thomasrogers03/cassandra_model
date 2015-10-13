source 'https://rubygems.org'

group :development do
  require 'yaml'
  require 'logger'
  gem 'rdoc'
  gem 'cassandra-driver', '~> 1.1', require: 'cassandra'
  gem 'activesupport', require: 'active_support/all'
  require 'active_support/core_ext/class/attribute_accessors'
  gem 'concurrent-ruby', require: 'concurrent'
  gem 'thomas_utils', '~> 0.1.4', git: 'https://github.com/thomasrogers03/thomas_utils.git'
  gem 'batch_reactor', github: 'thomasrogers03/batch_reactor'
end

group :test do
  gem 'rspec', '~> 3.1.0', require: false
  gem 'rspec-its'
  gem 'guard-rspec'
  gem 'guard-bundler'
  gem 'guard'
  gem 'pry'
  gem 'timecop'
  gem 'simplecov', require: false
end

gemspec
