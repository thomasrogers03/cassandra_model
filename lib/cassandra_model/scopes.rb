module CassandraModel
  module Scopes
    attr_reader :scopes

    def scope(name, callback)
      define_singleton_method(name, &callback)
      scopes[name] = callback
    end

    def scopes
      @scopes ||= {}
    end
  end
end
