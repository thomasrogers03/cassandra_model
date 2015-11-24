module CassandraModel
  module MetaColumns
    extend Forwardable

    #attr_reader :deferred_column_writers, :async_deferred_column_writers
    def_delegators :table_config, :deferred_column_writers, :async_deferred_column_writers

    def deferred_column(name, options)
      name = name.to_sym
      deferred_columns << name

      create_attr_accessor(name, options)
      create_save_method(name, options)
    end

    def async_deferred_column(name, options)
      name = name.to_sym
      deferred_columns << name

      async_create_attr_accessor(name, options)
      async_create_save_method(name, options)
    end

    def after_initialize(record)
      futures = if table_config.async_deferred_column_readers
                  table_config.async_deferred_column_readers.inject({}) do |memo, (column, callback)|
                    memo.merge!(column => callback.call(record.attributes))
                  end
                end
      record.instance_variable_set(:@deferred_futures, futures)
    end

    def save_deferred_columns(record)
      do_save_deferred_columns(record) if table_config.deferred_column_writers
    end

    def save_async_deferred_columns(record)
      do_save_async_deferred_columns(record) if table_config.async_deferred_column_writers
    end

    def deferred_columns
      table_config.deferred_columns ||= []
    end

    private

    def do_save_deferred_columns(record)
      table_config.deferred_column_writers.each { |column, callback| callback.call(record.attributes, record.send(column)) }
    end

    def do_save_async_deferred_columns(record)
      table_config.async_deferred_column_writers.map { |column, callback| callback.call(record.attributes, record.send(column)) }
    end

    def create_save_method(name, options)
      on_save = options[:on_save]
      if on_save
        table_config.deferred_column_writers ||= {}
        table_config.deferred_column_writers[name] = on_save

        define_method(:"save_#{name}") { on_save.call(@attributes, send(name)) }
      end
    end

    def async_create_save_method(name, options)
      on_save = options[:on_save]
      if on_save
        table_config.async_deferred_column_writers ||= {}
        table_config.async_deferred_column_writers[name] = on_save

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

      table_config.async_deferred_column_readers ||= {}
      table_config.async_deferred_column_readers[name] = on_load

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
