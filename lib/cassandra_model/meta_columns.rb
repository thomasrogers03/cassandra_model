module CassandraModel
  module MetaColumns

    def self.included(base)
      base.extend MetaColumnsStatic
    end

    private

    def deferred_columns
      self.class.deferred_columns
    end

    def after_initialize
      self.class.after_initialize(self)
      deferred_columns.each do |column|
        column_value = @attributes.delete(column)
        send("#{column}=", column_value) if column_value
      end
    end

    def deferred_getset(name, on_load)
      if instance_variable_defined?(defered_column_name(name))
        deferred_get(name)
      else
        deferred_set_with_callback(name, on_load)
      end
    end

    def deferred_get(name)
      instance_variable_get(defered_column_name(name))
    end

    def async_deferred_set(name)
      future = @deferred_futures[name]
      result = (future.get if future)
      deferred_set(name, result)
    end

    def deferred_set_with_callback(name, on_load)
      result = on_load.call(@attributes)
      deferred_set(name, result)
    end

    def deferred_set(name, value)
      instance_variable_set(defered_column_name(name), value)
    end

    def defered_column_name(name)
      "@deferred_#{name}"
    end
  end

  module MetaColumnsStatic
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
        deferred_set(name, value)
      end
    end

    def create_attr_reader(name, options)
      on_load = options[:on_load]
      raise 'No on_load method provided' unless on_load

      define_method(name) { deferred_getset(name, on_load) }
    end

    def async_create_attr_reader(name, options)
      on_load = options[:on_load]
      raise 'No on_load method provided' unless on_load

      table_config.async_deferred_column_readers ||= {}
      table_config.async_deferred_column_readers[name] = on_load

      define_method(name) do
        deferred_get(name) || async_deferred_set(name)
      end
    end
  end
end
