module CassandraModel
  class Observable
    class Listener
      def initialize(observer, func)
        @observer = observer
        @func = func
      end

      def success(value)
        @observer.public_send(@func, Time.now, value, nil)
      end

      def failure(error)
        @observer.public_send(@func, Time.now, nil, error)
      end
    end

    def self.create_observation(cassandra_future)
      observable = new(cassandra_future)
      ThomasUtils::Observation.new(ThomasUtils::Future::IMMEDIATE_EXECUTOR, observable)
    end

    def initialize(cassandra_future)
      @future = cassandra_future
    end

    def value
      value!
    rescue Exception
      nil
    end

    def value!
      @future.get
    end

    def add_observer(observer = nil, func = :update, &block)
      if block
        observer = block
        func = :call
      end
      @future.add_listener(Listener.new(observer, func))
    end

    def with_observer(observer = nil, func = :update, &block)
      add_observer(observer, func, &block)
      self
    end

    def delete_observer(_)
      raise NotImplementedError
    end

    def delete_observers
      raise NotImplementedError
    end

    def count_observers
      raise NotImplementedError
    end
  end
end
