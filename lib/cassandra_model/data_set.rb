module CassandraModel
  class DataSet
    include TypeGuessing

    attr_reader :columns, :clustering_columns, :data_rotation

    def initialize
      @columns = Hash.new { |hash, key| hash[key] = :text }
      @data_rotation = {}
      @clustering_columns = []
    end

    def knows_about(*columns)
      columns.each do |column|
        if @guess_data_types
          guess_data_type(column)
        else
          self.columns[column]
        end
      end
    end

    def counts(*columns)
      if columns.empty?
        count_column(:count)
      else
        columns.map { |column| count_column(column) }
      end
    end

    def rotates_storage_across(slices)
      TableRotation.new(slices, self)
    end

    def is_defined_by(*columns)
      knows_about(*columns)
      @clustering_columns = columns
    end

    def change_type_of(column)
      raise "Cannot retype unknown column #{column}" unless columns.include?(column)
      ColumnType.new(column, self)
    end

    private

    def count_column(column)
      @columns[column] = :counter
    end

    ColumnType = Struct.new(:column, :data_set) do
      def to(type)
        data_set.columns[column] = type
      end
    end

    TableRotation = Struct.new(:slices, :data_set) do
      def tables
        define_table_slicing
        define_rotation_frequency(1.week)
      end

      def tables_every(interval)
        define_table_slicing
        define_rotation_frequency(interval)
      end

      private

      def define_rotation_frequency(frequency)
        data_set.data_rotation[:frequency] = frequency
      end

      def define_table_slicing
        data_set.data_rotation[:slices] = slices
      end
    end

    def guess_data_type(column)
      columns[column] = guessed_data_type(column, :counter)
    end

  end
end
