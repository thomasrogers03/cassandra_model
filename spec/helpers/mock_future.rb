class MockFuture
  def initialize(result)
    @result = result
  end

  def join

  end

  def get
    @result
  end
end
