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
        displayable_attributes.is_a?(Hash) ? mapped_as_json : sliced_displayable_attributes
      else
        json_attributes
      end
    end

    private

    def json_attributes
      json_attributes = attributes.keys.inject({}) do |memo, column|
        case self.class.cassandra_columns[self.class.select_column(column).to_s]
          when :blob
          when :uuid
            memo[column] = attributes[column].to_s
          else
            memo[column] = attributes[column]
        end
        memo
      end
      self.class.deferred_columns.inject(json_attributes) do |memo, column|
        memo.merge!(column => public_send(column))
      end
    end

    def mapped_as_json
      sliced_displayable_attributes.inject({}) { |memo, (key, value)| memo.merge!(displayable_attributes[key] => value) }
    end

    def sliced_displayable_attributes
      json_attributes.slice(*displayable_attributes_slice)
    end

    def displayable_attributes_slice
      displayable_attributes.is_a?(Hash) ? displayable_attributes.keys : displayable_attributes
    end

    def displayable_attributes
      self.class.displayable_attributes
    end
  end
end
