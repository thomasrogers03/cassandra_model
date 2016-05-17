module CassandraModel
  class QueryBuilder
    include Enumerable
    extend Forwardable

    EMPTY_OPTION = [].freeze

    def initialize(record_klass, params = {}, options = {}, extra_options = {})
      @record_klass = record_klass
      @params = params
      @options = options
      @extra_options = extra_options
    end

    def async
      @record_klass.request_async(@params, @options)
    end

    def get
      @record_klass.request(@params, @options)
    end

    def to_cql
      @record_klass.request_meta(@params, @options).first
    end

    def inspect
      results = limit(@options[:limit] || 10).get
      "#<#{self.class.to_s}: #{inspected_results(results)}>"
    end

    def first_async
      @record_klass.first_async(@params, @options)
    end

    def first
      first_async.get
    end

    def create_async(attributes = {}, create_options = {})
      @record_klass.create_async(@params.merge(attributes), @options.merge(create_options))
    end

    def create(attributes = {}, create_options = {})
      @record_klass.create(@params.merge(attributes), @options.merge(create_options))
    end

    def new(attributes)
      @record_klass.new(@params.merge(attributes))
    end

    def first_or_new_async(attributes)
      first_async.then do |result|
        result || new(attributes)
      end
    end

    def first_or_new(attributes)
      first_or_new_async(attributes).get
    end

    def check_exists
      new_instance(@params, @options.merge(check_exists: true), @extra_options)
    end

    def pluck(*columns)
      query = select(*columns)
      if columns.length == 1
        query.map { |result| pluck_values(columns, result).first }
      else
        query.map { |result| pluck_values(columns, result) }
      end
    end

    def each_slice(slice_size = nil, &block)
      raise NotImplementedError if @extra_options[:cluster]
      paginate(slice_size).async.each_slice(&block)
    end

    def each(&block)
      if @record_klass.predecessor && !@extra_options[:skip_predecessor]
        ResultCombiner.new(
            new_instance(@params, @options, @extra_options.merge(skip_predecessor: true)),
            self.class.new(@record_klass.predecessor, @params, @options, @extra_options)
        ).each(&block)
      else
        each_internal(&block)
      end
    end

    def cluster(*columns)
      new_instance(@params, @options, @extra_options.merge(cluster: columns))
    end

    def cluster_except(*columns)
      cluster(*(@record_klass.primary_key - columns))
    end

    def where(params)
      new_instance(@params.merge(params.symbolize_keys), @options, @extra_options)
    end

    def select(*columns)
      append_option(columns, :select)
    end

    def order(*columns)
      append_option(columns, :order_by)
    end

    def limit(limit)
      if @extra_options[:cluster]
        new_instance(@params, @options, @extra_options.merge(cluster_limit: limit))
      else
        new_instance(@params, @options.merge(limit: limit), @extra_options)
      end
    end

    def trace(trace)
      new_instance(@params, @options.merge(trace: trace), @extra_options)
    end

    def paginate(page_size)
      new_instance(@params, @options.merge(page_size: page_size), @extra_options)
    end

    def ==(rhs)
      rhs.is_a?(QueryBuilder) &&
          rhs.record_klass == record_klass &&
          rhs.params == params &&
          rhs.options == options &&
          rhs.extra_options == extra_options
    end

    def method_missing(method, *args)
      scope = record_klass.scopes[method]
      scope ? instance_exec(*args, &scope) : super
    end

    protected

    attr_reader :record_klass, :params, :options, :extra_options

    private

    def each_internal(&block)
      if @extra_options[:cluster]
        enum = ResultChunker.new(async, @extra_options[:cluster])
        enum = if @extra_options[:cluster_limit]
                 ResultLimiter.new(enum, @extra_options[:cluster_limit])
               else
                 enum
               end
        block_given? ? enum.each(&block) : enum
      else
        async.each(&block)
      end
    end

    def new_instance(params, options, extra_options)
      self.class.new(record_klass, params, options, extra_options)
    end

    def append_option(columns, option)
      new_option = (@options[option] || EMPTY_OPTION).dup
      if columns.first.is_a?(Hash)
        columns = columns.first.map do |column, direction|
          {column.to_sym => direction}
        end
        new_option.concat(columns)
      else
        new_option.concat(columns.map(&:to_sym))
      end
      new_instance(@params, @options.merge(option => new_option), @extra_options)
    end

    def pluck_values(columns, result)
      result.attributes.slice(*columns).values
    end

    def inspected_results(results)
      "[#{(results.map(&:to_s) + %w(...)) * ', '}]"
    end

  end
end
