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

    def each_slice
      return to_enum(:each_slice) unless block_given?

      current_page = @page
      loop do
        page_results = current_page.get
        modified_results = page_results.map(&@callback)
        if page_results.last_page?
          yield modified_results
          break
        else
          current_page = page_results.next_page_async
          yield modified_results
        end
      end
    end

    alias :get :to_a

  end
end