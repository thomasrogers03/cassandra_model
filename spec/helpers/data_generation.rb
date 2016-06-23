module DataGeneration
  def generate_columns(prefix)
    Faker::Lorem.words.map { |word| :"#{prefix}_#{word}" }
  end

  def generate_partition_key
    generate_columns(:part)
  end

  def generate_partition_key_with_types
    generate_partition_key.inject({}) { |memo, column| memo.merge!(column => :text) }
  end

  def generate_partition_key_with_random_types
    generate_partition_key.inject({}) { |memo, column| memo.merge!(column => random_type) }
  end

  def generate_clustering_columns
    generate_columns(:cluster)
  end

  def generate_clustering_columns_with_types
    generate_clustering_columns.inject({}) { |memo, column| memo.merge!(column => :text) }
  end

  def generate_clustering_columns_with_random_types
    generate_clustering_columns.inject({}) { |memo, column| memo.merge!(column => random_type) }
  end

  def generate_fields
    generate_columns(:field)
  end

  def generate_fields_with_types
    generate_fields.inject({}) { |memo, column| memo.merge!(column => :text) }
  end

  def generate_fields_with_random_types
    generate_fields.inject({}) { |memo, column| memo.merge!(column => random_type) }
  end

  def generate_counter_fields(columns)
    columns.inject({}) { |memo, column| memo.merge!(column => :counter) }
  end

  def generate_primary_key
    (partition_key + clustering_columns).inject({}) { |memo, column| memo.merge!(column => Faker::Lorem.sentence) }
  end

  def generate_attributes
    columns.inject({}) { |memo, column| memo.merge!(column => Faker::Lorem.sentence) }
  end

  def generate_options
    Faker::Lorem.words.inject({}) { |memo, key| memo.merge!(key.to_sym => Faker::Lorem.sentence) }
  end

  def random_type
    [:text, :blob, :int, :double].sample
  end
end
