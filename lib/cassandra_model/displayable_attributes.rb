module CassandraModel
  module DisplayableAttributesStatic
    def display_attributes(*columns)
      map = (columns.first if columns.first.is_a?(Hash))
      table_config.display_attributes = map ? map : columns
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
      if displayable_attributes
        if displayable_attributes.is_a?(Hash)
          attributes.slice(*displayable_attributes.keys).inject({}) { |memo, (key, value)| memo.merge!(displayable_attributes[key] => value) }
        else
          attributes.slice(*displayable_attributes)
        end
      else
        attributes
      end
    end

    private

    def displayable_attributes
      self.class.displayable_attributes
    end
  end
end
