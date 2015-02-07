module CassandraModel
  module MetaColumns

    def deferred_column(name, options)
      name = name.to_sym

      create_attr_accessor(name, options)
      create_save_method(name, options)
    end

    def async_deferred_column(name, options)
      name = name.to_sym
      async_create_attr_accessor(name, options)
      async_create_save_method(name, options)
    end

    def after_initialize(record)
      futures = if @async_deferred_column_readers
                  @async_deferred_column_readers.inject({}) do |memo, (column, callback)|
                    memo.merge!(column => callback.call(record.attributes))
                  end
                end
      record.instance_variable_set(:@deferred_futures, futures)
    end

    def after_save(record)
      save_deferred_columns(record) if @deferred_column_writers
      save_async_deferred_columns(record)
    end

    def after_save_async(record)
      @async_deferred_column_writers.map { |column, callback| callback.call(record.attributes, record.send(column)) }
    end

    private

    def save_async_deferred_columns(record)
      after_save_async(record).map(&:get) if @async_deferred_column_writers
    end

    def save_deferred_columns(record)
      @deferred_column_writers.each { |column, callback| callback.call(record.attributes, record.send(column)) }
    end

    def create_save_method(name, options)
      on_save = options[:on_save]
      if on_save
        @deferred_column_writers ||= {}
        @deferred_column_writers[name] = on_save

        define_method(:"save_#{name}") { on_save.call(@attributes, send(name)) }
      end
    end

    def async_create_save_method(name, options)
      on_save = options[:on_save]
      if on_save
        @async_deferred_column_writers ||= {}
        @async_deferred_column_writers[name] = on_save

        define_method(:"save_#{name}") { on_save.call(@attributes, send(name)) }
      end
    end

    def create_attr_accessor(name, options)
      create_attr_reader(name, options)
      create_attr_writer(name)
    end

    def async_create_attr_accessor(name, options)
      async_create_attr_reader(name, options)
      create_attr_writer(name)
    end

    def create_attr_writer(name)
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

    def async_create_attr_reader(name, options)
      on_load = options[:on_load]
      raise 'No on_load method provided' unless on_load

      @async_deferred_column_readers ||= {}
      @async_deferred_column_readers[name] = on_load

      define_method(name) do
        if @attributes.include?(name)
          @attributes[name]
        else
          future = @deferred_futures[name]
          @attributes[name] = if future
                                future.get
                              end
        end
      end
    end

  end
end