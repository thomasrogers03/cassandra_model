module CassandraModel
  module RecordDebug
    DebugDump = Struct.new(
        :record,
        :klass,
        :table,
        :table_config,
        :table_data,
        :attributes,
        :internal_attributes,
    )

    def debug
      DebugDump.new(
          self,
          self.class,
          self.class.table,
          self.class.send(:table_config),
          self.class.send(:table_data),
          attributes,
          internal_attributes,
      )
    end
  end
end
