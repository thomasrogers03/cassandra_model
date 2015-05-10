module CassandraModel
  module CompositeRecordInstance
    def save_async
      futures = composite_rows.map { |record| record.send(:internal_save_async) }

      futures << internal_save_async
      futures = ThomasUtils::MultiFutureWrapper.new(futures) { }
      ThomasUtils::FutureWrapper.new(futures) { self }
    end

    def delete_async
      futures = composite_rows.map { |record| record.send(:internal_delete_async) }

      futures << internal_delete_async
      futures = ThomasUtils::MultiFutureWrapper.new(futures) { |result| result }
      ThomasUtils::FutureWrapper.new(futures) { self }
    end

    def update_async(new_attributes)
      futures = composite_rows.map { |record| record.send(:internal_update_async, new_attributes) }

      futures << internal_update_async(new_attributes)
      futures = ThomasUtils::MultiFutureWrapper.new(futures) { |result| result }
      ThomasUtils::FutureWrapper.new(futures) { self }
    end

    private

    def composite_rows
      (self.class.composite_defaults || []).map do |row|
        merged_attributes = attributes.merge(row)
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
        memo.merge(column => attribute(column))
      end
    end
  end
end