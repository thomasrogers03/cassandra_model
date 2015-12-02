module CassandraModel
  module ConcurrencyHelper
    private

    def safe_getset_variable(mutex, name, &block)
      result = instance_variable_get(name)
      return result if result

      mutex.synchronize do
        raise Cassandra::Errors::InvalidError.new('Connection invalidated!', 'Dummy') if !!@shutdown

        result = instance_variable_get(name)
        return result if result

        instance_variable_set(name, block.call)
      end
    end
  end
end
