class MockFuture
  def initialize(result)
    @result = result
  end

  def join
    self
  end

  def get
    @result
  end
end
