require 'pp'
require 'cassandra'
require 'benchmark'
require 'thwait'
require 'bundler'

Bundler.require(:default)
Dir['./lib/**/*.rb'].each { |f| require f }
puts '=> loaded'