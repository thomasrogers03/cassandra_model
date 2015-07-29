class MockFuture
  def initialize(result_or_options)
    if result_or_options.is_a?(Hash)
      @result = result_or_options[:result]
      @error = result_or_options[:error]
    else
      @result = result_or_options
    end
  end

  def join
    self
  end

  def then
    unless @error
      MockFuture.new(yield @result)
    end
  end

  def on_success
    yield @result unless @error
    self
  end

  def on_failure
    yield @error if @error
    self
  end

  def get
    @result
  end
end
