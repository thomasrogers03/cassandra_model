source 'https://rubygems.org'

group :development do
  require 'yaml'
  require 'logger'
  gem 'rdoc'
  gem 'cassandra-driver', '~> 1.1'
  gem 'activesupport'
  gem 'concurrent-ruby'
  gem 'thomas_utils', '~> 0.1.4', github: 'thomasrogers03/thomas_utils'
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
  gem 'faker'
end

gemspec
