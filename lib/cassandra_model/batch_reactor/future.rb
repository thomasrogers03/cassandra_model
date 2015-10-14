module CassandraModel
  class BatchReactor
    class Future
      extend Forwardable

      def_delegators :@future, :on_complete, :on_failure, :get, :then
      def_delegator :@future, :on_value, :on_success

      def initialize(ione_future)
        @future = ione_future
      end

      def promise
        raise NotImplementedError
      end

      def add_listener(*_)
        raise NotImplementedError
      end

      def fallback(&_)
        raise NotImplementedError
      end

      def join
        @future.get
        self
      end

    end
  end
end