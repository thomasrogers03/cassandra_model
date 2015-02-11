require 'rspec'

module CassandraModel
  describe MetaColumns do

    class MockRecord
      extend MetaColumns

      attr_reader :attributes

      def initialize(attributes)
        @attributes = attributes
        MockRecord.after_initialize(self)
      end

      def self.reset!
        @deferred_column_writers = nil
        @async_deferred_column_readers = nil
        @async_deferred_column_writers = nil
      end

      def save_async
        MockRecord.save_deferred_columns(self)
        futures = MockRecord.save_async_deferred_columns(self)
        futures.map(&:get) if futures
      end
    end

    subject { MockRecord }

    before { MockRecord.reset! }

    it_behaves_like 'a record defining meta columns'

  end
end