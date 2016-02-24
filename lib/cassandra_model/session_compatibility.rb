module Cassandra
  class Session

    alias :__execute_async :execute_async

    def execute_async(statement, *args)
      options = args.last.is_a?(Hash) ? args.pop : {}
      __execute_async(statement, options.merge(arguments: args))
    end

  end
end
