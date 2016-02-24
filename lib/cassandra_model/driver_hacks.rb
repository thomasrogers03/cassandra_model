module Cassandra
  class Session
    alias :__execute_async :execute_async

    def execute_async(statement, *args)
      if args.last.is_a?(::Hash)
        options = args.pop
      else
        options = {}
      end

      options[:arguments] = args
      __execute_async(statement, options)
    end
  end
end
