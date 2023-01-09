# frozen_string_literal: true

module Reactor
  # It wraps hash objects to HashWithIndifferentAccess instances recursively.
  # If it is an array its elements are recursively wrapped.
  # Else it is returned as is.
  def self.indif_access_recursive(obj)
    if obj.is_a? Hash
      indiffed = obj.map do |k, v|
        [k, indif_access_recursive(v)]
      end.to_h
      Bundler::Thor::CoreExt::HashWithIndifferentAccess.new(indiffed)
    elsif obj.is_a? Array
      obj.map do |v|
        indif_access_recursive(v)
      end
    else
      obj
    end
  end
end

class Bundler::Thor::CoreExt::HashWithIndifferentAccess
  alias :has_key? :key?
end
