module CassandraModel
  class QueryBuilder
    include Enumerable
    extend Forwardable

    def_delegator :async, :each

    def initialize(record_klass)
      @record_klass = record_klass
      @params = {}
      @options = {}
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

    def first_async
      @record_klass.first_async(@params, @options)
    end

    def first
      @record_klass.first(@params, @options)
    end

    def create_async(attributes = {}, create_options = {})
      @record_klass.create_async(@params.merge(attributes), @options.merge(create_options))
    end

    def create(attributes = {}, create_options = {})
      @record_klass.create(@params.merge(attributes), @options.merge(create_options))
    end

    def check_exists
      @options.merge!(check_exists: true)
      self
    end

    def pluck(*columns)
      select(*columns).get.map { |result| result.attributes.slice(*columns).values }
    end

    def each_slice(slice_size = nil, &block)
      paginate(slice_size).async.each_slice(&block)
    end

    def where(params)
      @params.merge!(params)
      self
    end

    def select(*columns)
      @options[:select] ||= []
      @options[:select].concat(columns)
      self
    end

    def order(*columns)
      @options[:order_by] ||= []
      @options[:order_by].concat(columns)
      self
    end

    def limit(limit)
      @options[:limit] = limit
      self
    end

    def paginate(page_size)
      @options[:page_size] = page_size
      self
    end
  end
end