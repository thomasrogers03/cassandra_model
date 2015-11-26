module CassandraModel
  module DisplayableAttributesStatic
    def display_attributes(*columns)
      table_config.display_attributes = columns
    end

    def displayable_attributes
      table_config.display_attributes
    end
  end

  module DisplayableAttributes
    def self.included(base)
      base.extend(DisplayableAttributesStatic)
    end

    def as_json(*_)
      displayable_attributes ? attributes.slice(*displayable_attributes) : attributes
    end

    private

    def displayable_attributes
      self.class.displayable_attributes
    end
  end
end
