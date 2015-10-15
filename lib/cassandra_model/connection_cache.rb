module CassandraModel
  class ConnectionCache
    MUTEX = Mutex.new

    class << self
      def build_cache
        Hash.new do |hash, key|
          MUTEX.synchronize { hash[key] = RawConnection.new(key) }
        end
      end

      def [](key)
        @@cache[key]
      end

      def clear
        @@cache.values.map(&:shutdown)
        @@cache.clear
      end
    end

    @@cache = build_cache
  end
end