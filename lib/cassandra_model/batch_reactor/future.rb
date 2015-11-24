module CassandraModel
  class BatchReactor
    class Future < Cassandra::Future
      extend Forwardable

      def self.define_handler(internal_name, external_name = internal_name)
        define_method(external_name) do |&block|
          @future.public_send(internal_name, &block)
          self
        end
      end

      define_handler :on_complete
      define_handler :on_failure
      define_handler :on_value, :on_success
      def_delegator :@future, :get

      def initialize(ione_future)
        @future = ione_future
      end

      def add_listener(listener)
        @future.on_complete do |value, error|
          error ? listener.failure(error) :  listener.success(value)
        end
        self
      end

      def promise
        raise NotImplementedError
      end

      def fallback(&_)
        raise NotImplementedError
      end

      def then(&block)
        internal_future = @future.then(&block)
        Future.new(internal_future)
      end

      def join
        @future.get
        self
      end

    end
  end
end