class ResultPaginator
  include Enumerable

  def initialize(first_page, &callback)
    @page = first_page
    @callback = callback
  end

  def each(&block)
    current_page = @page
    loop do
      page_results = current_page.get
      modified_results = page_results.map(&@callback)
      if page_results.last_page?
        modified_results.each(&block)
        break
      else
        current_page = page_results.next_page_async
        modified_results.each(&block)
      end
    end
  end
end