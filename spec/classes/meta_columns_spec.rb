require 'rspec'

module CassandraModel
  describe MetaColumns do

    class MockRecord
      include MetaColumns

      attr_reader :attributes

      def initialize(attributes)
        @attributes = attributes
        MockRecord.after_initialize(self)
      end

      def self.reset!
        @table_data = nil
      end

      def self.table_data
        @table_data ||= OpenStruct.new
      end

      def self.table_config
        @table_data ||= OpenStruct.new
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
