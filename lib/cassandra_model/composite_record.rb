module CassandraModel
  module CompositeCounterRecord
    def increment_async!(counts)
      futures = composite_rows.map { |record| record.internal_increment_async!(counts) }

      futures << internal_increment_async!(counts)
      ThomasUtils::Future.all(futures).then { self }
    end
  end

  module CompositeRecord
    def self.included(klass)
      klass.extend CompositeRecordStatic
      klass.send(:include, CompositeCounterRecord) if klass < CounterRecord
    end

    def save_async(options = {})
      futures = composite_rows.map { |record| record.internal_save_async(options) }

      futures << internal_save_async(options)
      ThomasUtils::Future.all(futures).then { self }
    end

    def delete_async
      futures = composite_rows.map { |record| record.internal_delete_async }

      futures << internal_delete_async
      ThomasUtils::Future.all(futures).then { self }
    end

    def update_async(new_attributes)
      futures = composite_rows.map { |record| record.internal_update_async(new_attributes) }

      futures << internal_update_async(new_attributes)
      ThomasUtils::Future.all(futures).then { self }
    end

    private

    def composite_rows
      (self.class.composite_defaults || []).map do |row|
        merged_attributes = self.class.deferred_columns.inject(attributes.merge(row)) do |memo, column|
          memo.merge!(column => public_send(column))
        end
        self.class.new(merged_attributes, validate: false)
      end
    end

    def attribute(column)
      attributes[column] ||
          attributes[self.class.composite_ck_map[column]] ||
          attributes[self.class.composite_pk_map[column]]
    end

    def internal_attributes
      internal_columns.inject({}) do |memo, column|
        memo.merge!(column => attribute(column))
      end
    end
  end
end
