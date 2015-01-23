require 'pp'
require 'cassandra'
require 'benchmark'
require 'thwait'
require 'bundler'
require 'active_support/all'

Bundler.require(:default)
Dir['./lib/**/*.rb'].each { |f| require f }
puts '=> loaded'