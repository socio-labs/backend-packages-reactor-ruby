# frozen_string_literal: true

module Reactor
  class Stream
    def initialize
      @subscriptions = {}
    end

    def subscribe(id:, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT, &block)
      make_subscriptions(id, action, packet, false, &block)
    end

    def subscribe_batch(id:, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT, &block)
      make_subscriptions(id, action, packet, true, &block)
    end

    def publish_unique(_id, message = nil, _force = false, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
      call_subscriptions(message, action, packet)
    end

    def publish(message = nil, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
      call_subscriptions(message, action, packet)
    end

    def publish_batch(messages = [], action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
      call_subscriptions_batch(messages, action, packet)
    end

    def unsubscribe(id, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
      @subscriptions[[action, packet, false]].delete_if { |item| item[:id] == id }
    end

    def unsubscribe_batch(id, action: Reactor::Reaction::DEFAULT, packet: Reactor::Packet::DEFAULT)
      @subscriptions[[action, packet, true]].delete_if { |item| item[:id] == id }
    end

    private

      def make_subscriptions(id, action, packet, batch_mode, &block)
        packet_subscriptions = @subscriptions[[action, packet, batch_mode]] || []
        @subscriptions[[action, packet, batch_mode]] = packet_subscriptions << { id: id, proc: block }
        id
      end

      def subscription_types(action, packet, batch_mode)
        [
          [action, packet, batch_mode],
          ['*', packet, batch_mode],
          ['*', '*', batch_mode],
          [action, '*', batch_mode]
        ]
      end

      def call_subscriptions_batch(messages, action, packet)
        subscription_types(action, packet, true)
          .select { |key| @subscriptions.has_key?(key) }
          .each { |key|
            @subscriptions[key].each do |act|
              act[:proc].call(messages)
            end
          }

        subscription_types(action, packet, false)
          .select { |key| @subscriptions.has_key?(key) }
          .each { |key|
            @subscriptions[key].each do |act|
              messages.each { |message| act[:proc].call(message) }
            end
          }
      end

      def call_subscriptions(message, action, packet)
        subscription_types(action, packet, false)
          .select { |key| @subscriptions.has_key?(key) }
          .each { |key| @subscriptions[key].each { |act| act[:proc].call(message) } }

        subscription_types(action, packet, true)
          .select { |key| @subscriptions.has_key?(key) }
          .each { |key|
            @subscriptions[key].each { |act| act[:proc].call([message]) }
          }
      end
  end
end
