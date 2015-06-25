require 'bundler'
Bundler.require(:default, :development)
require './lib/cassandra_model'

CassandraModel::ConnectionCache[nil].config = {hosts: %w(cassandra.dev), keyspace: 'test'}

at_exit { CassandraModel::ConnectionCache.clear }