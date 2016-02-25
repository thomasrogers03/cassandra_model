# only install this hack if our driver is incompatible with the old interface
if Gem::Version.new(Gem.loaded_specs['cassandra-driver'].version) >= Gem::Version.new('2')
  module Cassandra
    class Session

      alias :__execute_async :execute_async

      def execute_async(statement, *args)
        options = args.last.is_a?(Hash) ? args.pop : {}
        options = options.merge(arguments: args) unless options[:arguments]
        __execute_async(statement, options)
      end

      def execute(statement, *args)
        execute_async(statement, *args).get
      end

    end
  end
end
