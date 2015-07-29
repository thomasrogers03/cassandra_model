class MockFuture
  def initialize(result_or_options)
    if result_or_options.is_a?(Hash) &&
        (result_or_options.include?(:result) || result_or_options.include?(:error))
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
      updated_result = yield @result
      updated_result = updated_result.get if updated_result.respond_to?(:get)
      MockFuture.new(updated_result)
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
