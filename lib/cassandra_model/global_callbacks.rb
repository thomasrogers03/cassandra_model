module CassandraModel
  class GlobalCallbacks

    class << self
      def add_listener(listener)
        listeners << listener
      end

      def call(callback, *params)
        listeners.each do |listener|
          callback_name = callback_name(callback)
          listener.public_send(callback_name, *params) if listener.respond_to?(callback_name)
        end
      end

      private

      def callback_name(callback)
        "on_#{callback}"
      end

      def listeners
        @listeners ||= []
      end
    end

  end
end
