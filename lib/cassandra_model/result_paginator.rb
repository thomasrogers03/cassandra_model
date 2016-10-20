module CassandraModel
  class ResultPaginator
    include Enumerable

    def initialize(first_page, model_klass, &callback)
      @page = first_page
      @model_klass = model_klass
      @callback = callback
    end

    def each(&block)
      return to_enum(:each) unless block_given?

      each_slice { |slice| slice.each(&block) }
    end

    def with_index(&block)
      each.with_index(&block)
    end

    def each_slice(&block)
      return to_enum(:each_slice) unless block_given?

      current_page_future = @page
      page_count = 0
      while current_page_future
        current_page_future.on_timed do |_, _, duration, value, _|
          Logging.logger.debug("#{@model_klass} Load (Page #{page_count += 1} with count #{value.count}): #{duration * 1000}ms")
        end
        current_page_future = iterate_page(current_page_future, &block)
      end
    end

    alias :get :to_a

    private

    def iterate_page(current_page, &block)
      page_results = current_page.get
      unless page_results.empty?
        next_page(page_results, &block)
      end
    end

    def next_page(page_results, &block)
      if page_results.last_page?
        modify_and_yield_page_results(page_results, &block)
        nil
      else
        next_page_future = page_results.next_page_async
        next_page_future = Observable.create_observation(next_page_future)
        modify_and_yield_page_results(page_results, &block)
        next_page_future
      end
    end

    def modify_and_yield_page_results(page_results)
      yield modified_page_results(page_results)
    end

    def modified_page_results(page_results)
      page_results.map { |result| @callback[result, page_results.execution_info] }
    end

  end
end
