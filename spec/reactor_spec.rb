# frozen_string_literal: true

module ReactiveStream
  APP = Reactor::Stream.new
  APP2 = Reactor::Stream.new
  APP3 = Reactor::Stream.new
  APP4 = Reactor::Stream.new
  APP5 = Reactor::Stream.new
end

module Packet
  ANY = '*'
  INT = :int
  module Batch
    INT = :batch_int
  end
end

module Reaction
  REACTED = Reactor::Reaction.new(:reacted, ReactiveStream::APP, ReactiveStream::APP2)
  REACTED_BATCH = Reactor::Reaction.new(:reacted, ReactiveStream::APP5)
  REACTED_2 = Reactor::Reaction.new(:reacted2, ReactiveStream::APP4)
end

GLOBALS = Class.new do
  attr_accessor :stream_packet_any_method
  attr_accessor :stream_packet_batch_method
  attr_accessor :stream_packet_type_method
  attr_accessor :everytime_method
  attr_accessor :action_packet_any_method
  attr_accessor :action_packet_type_method
  attr_accessor :action_method
  attr_accessor :every3, :every4
end.new

GLOBALS.methods
       .select { |m| m.match(/^[a-z].+=$/) }
       .each { |var| GLOBALS.send(var, 0) }

class ConsumerTestClass2 < Reactor::Consumer
  use ReactiveStream::APP do
    on_stream_packet do
      type Packet::ANY, :stream_packet_any1, :stream_packet_any2
      type Packet::INT, :stream_packet_type1, :stream_packet_type2
    end

    def stream_packet_any1(msg)
      GLOBALS.stream_packet_any_method += msg
    end
    def stream_packet_any2
      GLOBALS.stream_packet_any_method += 1
    end
    def stream_packet_type1(msg)
      GLOBALS.stream_packet_type_method += msg
    end
    def stream_packet_type2
      GLOBALS.stream_packet_type_method += 1
    end

    on_everytime :everytime1, :everytime2

    def everytime1(msg)
      GLOBALS.everytime_method += msg
    end
    def everytime2
      GLOBALS.everytime_method += 1
    end

    on_action_packet Reaction::REACTED do
      type Packet::ANY, :action_packet_any1, :action_packet_any2
      type Packet::INT, :action_packet_type1, :action_packet_type2
    end

    def action_packet_any1(msg)
      GLOBALS.action_packet_any_method += msg
    end
    def action_packet_any2
      GLOBALS.action_packet_any_method += 1
    end
    def action_packet_type1(msg)
      GLOBALS.action_packet_type_method += msg
    end
    def action_packet_type2
      GLOBALS.action_packet_type_method += 1
    end

    on_action Reaction::REACTED, :action1, :action2

    def action1(msg)
      GLOBALS.action_method += msg
    end
    def action2
      GLOBALS.action_method += 1
    end
  end
end

class ConsumerTestClass3 < Reactor::Consumer
  use ReactiveStream::APP3 do
    @sub_indexes = on_everytime :everytime_handler

    def everytime_handler
      GLOBALS.every3 += 1
    end
  end

  def self.unsub
    @sub_indexes.each do |index|
      @__stream__.unsubscribe(index, action: '*', packet: '*')
    end
  end
end

class ConsumerTestClass4 < Reactor::Consumer
  use ReactiveStream::APP4 do
    @indexes = on_action Reaction::REACTED_2, :every_action_handler

    def every_action_handler
      GLOBALS.every4 += 1
    end
  end

  def self.unsub
    @indexes.each do |index|
      Reaction::REACTED_2.unsubscribe(index, ReactiveStream::APP4, '*')
    end
  end
end

RSpec.describe Reactor do
  it 'has a version number' do
    expect(Reactor::VERSION).not_to be nil
  end

  it 'consumer test' do
    Reaction::REACTED.publish(4, packet: Packet::INT)
    Reaction::REACTED_BATCH.publish_batch([1, 2, 3], packet: Packet::Batch::INT)
    Reaction::REACTED.publish(5)
    ReactiveStream::APP.publish(7, packet: :unknown)
    ReactiveStream::APP.publish(9)
    ReactiveStream::APP2.publish(11)

    expect(GLOBALS.stream_packet_any_method).to eq(29)
    expect(GLOBALS.stream_packet_type_method).to eq(5)
    expect(GLOBALS.everytime_method).to eq(29)
    expect(GLOBALS.action_packet_any_method).to eq(11)
    expect(GLOBALS.action_packet_type_method).to eq(5)
    expect(GLOBALS.action_method).to eq(11)
  end

  it 'should unsubscribe on high level' do
    ReactiveStream::APP3.publish
    expect(GLOBALS.every3).to eq(1)
    ConsumerTestClass3.unsub
    ReactiveStream::APP3.publish
    expect(GLOBALS.every3).to eq(1)
  end

  it 'should unsubscribe on high level' do
    Reaction::REACTED_2.publish
    expect(GLOBALS.every4).to eq(1)
    ConsumerTestClass4.unsub
    Reaction::REACTED_2.publish
    expect(GLOBALS.every3).to eq(1)
  end
end
