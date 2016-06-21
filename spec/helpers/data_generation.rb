module DataGeneration
  def generate_columns(prefix)
    Faker::Lorem.words.map { |word| :"#{prefix}_#{word}" }
  end

  def generate_partition_key
    generate_columns(:part)
  end

  def generate_clustering_columns
    generate_columns(:cluster)
  end

  def generate_fields
    generate_columns(:field)
  end

  def generate_attributes
    columns.inject({}) { |memo, column| memo.merge!(column => Faker::Lorem.sentence) }
  end

  def generate_options
    Faker::Lorem.words.inject({}) { |memo, key| memo.merge!(key.to_sym => Faker::Lorem.sentence) }
  end
end
