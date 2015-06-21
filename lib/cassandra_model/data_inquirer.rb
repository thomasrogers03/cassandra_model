module CassandraModel
  class DataInquirer
    attr_reader :partition_key

    def initialize
      @partition_key = Hash.new { |hash, key| hash[key] = :string }
    end

    def knows_about(*columns)
      columns.each { |column| partition_key[column] }
      self
    end

  end
end