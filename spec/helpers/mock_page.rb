class MockPage
  include Enumerable
  extend Forwardable

  attr_reader :next_page_async
  def_delegators :@results, :each, :empty?

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
end