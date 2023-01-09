# frozen_string_literal: true

module Reactor
  class Reaction
    def initialize(name, *streams)
      if name.is_a?(String) && name != '*'
        raise Reactor::Error.new("You should use symbol instead of string for this reaction name: #{name}")
      end
      @name = name
      @streams = streams
    end

    def subscribe(id, stream, packet = '*', &block)
      raise Reactor::Error.new("Reaction has not a stream #{stream}") unless @streams.include?(stream)
      stream.subscribe(id:, action: @name, packet:, &block)
    end

    def subscribe_batch(id, stream, packet = '*', &block)
      raise Reactor::Error.new("Reaction has not a stream #{stream}") unless @streams.include?(stream)
      stream.subscribe_batch(id:, action: @name, packet:, &block)
    end

    def unsubscribe(id, stream, packet = '*')
      raise Reactor::Error.new("Reaction has not a stream #{stream}") unless @streams.include?(stream)
      stream.unsubscribe(id, action: @name, packet:)
    end

    def unsubscribe_batch(id, stream, packet = '*')
      raise Reactor::Error.new("Reaction has not a stream #{stream}") unless @streams.include?(stream)
      stream.unsubscribe_batch(id, action: @name, packet:)
    end

    def publish(message = nil, packet: Reactor::Packet::DEFAULT, streams: nil)
      run_on_streams = streams.nil? ? @streams : streams
      run_on_streams.each do |stream|
        stream.publish(message, action: @name, packet:)
      end
    end

    def publish_unique(id, message = nil, force = false, packet: Reactor::Packet::DEFAULT, streams: nil)
      run_on_streams = streams.nil? ? @streams : streams
      run_on_streams.each do |stream|
        stream.publish_unique(id, message, force, action: @name, packet:)
      end
    end

    def publish_batch(messages = [], packet: Reactor::Packet::DEFAULT, streams: nil)
      run_on_streams = streams.nil? ? @streams : streams
      run_on_streams.each do |stream|
        stream.publish_batch(messages, action: @name, packet:)
      end
    end
  end

  Reactor::Reaction::DEFAULT = :default
end
