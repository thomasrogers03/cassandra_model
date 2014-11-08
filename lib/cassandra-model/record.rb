class Record
  class << self
    def table_name
      self.to_s.underscore.pluralize
    end
  end
end