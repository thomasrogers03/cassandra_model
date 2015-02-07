module CassandraModel
  module MetaColumns

    def deferred_column(name, options)
      name = name.to_sym
      create_attr_accessor(name, options)
      create_save_method(name, options)
    end

    private

    def create_save_method(name, options)
      on_save = options[:on_save]
      if on_save
        define_method(:"save_#{name}") { on_save.call(@attributes, send(name)) }
      end
    end

    def create_attr_accessor(name, options)
      create_attr_reader(name, options)
      create_attr_write(name)
    end

    def create_attr_write(name)
      define_method(:"#{name}=") do |value|
        @attributes[name] = value
      end
    end

    def create_attr_reader(name, options)
      on_load = options[:on_load]
      raise 'No on_load method provided' unless on_load
      define_method(name) do
        if @attributes.include?(name)
          @attributes[name]
        else
          @attributes[name] = on_load.call(@attributes)
        end
      end
    end
  end
end