source 'https://rubygems.org'

group :development do
  gem 'rdoc', require: false
  gem 'cassandra-driver', '~> 1.1', require: false
  gem 'activesupport', require: false
  gem 'concurrent-ruby', require: false
  gem 'thomas_utils', '~> 0.1.4', github: 'thomasrogers03/thomas_utils', require: false
  gem 'batch_reactor', github: 'thomasrogers03/batch_reactor', require: false
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
  gem 'cassandra_mocks', github: 'thomasrogers03/cassandra_mocks', require: false
end

gemspec
