class MockPage
  attr_reader :next_page_async

  def initialize(last_page, next_page_async, results)
    @last_page = last_page
    @next_page_async = next_page_async
    @results = results
  end

  def last_page?
    @last_page
  end

  def get
    @results
  end

  def map(&block)
    @results.map(&block)
  end
end