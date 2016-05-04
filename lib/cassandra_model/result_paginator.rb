module CassandraModel
  class ResultPaginator
    include Enumerable

    def initialize(first_page, &callback)
      @page = first_page
      @callback = callback
    end

    def each(&block)
      return to_enum(:each) unless block_given?

      each_slice { |slice| slice.each(&block) }
    end

    def each_slice(&block)
      return to_enum(:each_slice) unless block_given?

      current_page = @page
      while current_page
        current_page = iterate_page(current_page, &block)
      end
    end

    alias :get :to_a

    private

    def iterate_page(current_page, &block)
      page_results = current_page.get
      unless page_results.empty?
        modified_results = modified_page_results(page_results)
        next_page(page_results, modified_results, &block)
      end
    end

    def next_page(page_results, modified_results)
      if page_results.last_page?
        yield modified_results
        nil
      else
        current_page = page_results.next_page_async
        yield modified_results
        current_page
      end
    end

    def modified_page_results(page_results)
      page_results.map { |result| @callback[result, page_results.execution_info] }
    end

  end
end
