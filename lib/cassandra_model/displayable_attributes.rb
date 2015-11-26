module CassandraModel
  module DisplayableAttributes
    def as_json(*_)
      attributes
    end
  end
end
