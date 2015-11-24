class MockFuture < Cassandra::Future
  def initialize(result_or_options)
    if result_or_options.is_a?(Hash) &&
        (result_or_options.include?(:result) || result_or_options.include?(:error))
      @result = result_or_options[:result]
      @error = result_or_options[:error]
    else
      @result = result_or_options
    end
  end

  def add_listener(listener)
    @error ? listener.failure(@error) : listener.success(@result)
    self
  end

  def join
    self
  end

  def then
    if @error
      MockFuture.new(error: @error)
    else
      updated_result = yield @result
      updated_result = updated_result.get if updated_result.respond_to?(:get)
      MockFuture.new(updated_result)
    end
  end

  def on_complete
    yield(@result, @error)
    self
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
    if @error
      raise @error
    else
      @result
    end
  end
end
