module CassandraModel
  module Scopes
    attr_reader :scopes

    def self.extended(base)
      base.instance_variable_set(:@scopes, {})
    end

    def scope(name, callback)
      define_singleton_method(name, &callback)
      scopes[name] = callback
    end
  end
end
