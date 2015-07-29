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

      each_slice_async(@page, &block).join
    end

    alias :get :to_a

    private

    def each_slice_async(future, &block)
      future.then do |page|
        next if page.empty?
        modified_results = page.map(&@callback)

        if page.last_page?
          yield modified_results
        else
          next_future = page.next_page_async
          yield modified_results
          each_slice_async(next_future, &block)
        end
      end
    end

  end
end