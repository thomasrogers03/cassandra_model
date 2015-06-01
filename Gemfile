source 'https://rubygems.org'

gemspec

group :development do
  require 'yaml'
  gem 'cassandra-driver', '~> 1.1', require: 'cassandra'
  gem 'activesupport', require: 'active_support/all'
  gem 'concurrent-ruby', require: 'concurrent'
  gem 'thomas_utils', '~> 0.1.4', git: 'https://github.com/thomasrogers03/thomas_utils.git'
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
