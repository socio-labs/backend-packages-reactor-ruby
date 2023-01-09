# frozen_string_literal: true

module Reactor
  class Consumer
    class << self
      def use(stream, &block)
        raise ArgumentError.new('using can only be used with a block') unless block_given?
        @__stream__ = stream
        @__all_reaction__ = Reaction.new('*', stream)
        self.instance_eval(&block)
      end

      def on_action_packet(*reactions)
        reactions.flat_map do |reaction|
          @__reaction__ = reaction
          yield
        end
      end

      def on_stream_packet
        @__reaction__ = @__all_reaction__
        yield
      end

      def on_everytime(*methods, batch: false)
        @__reaction__ = @__all_reaction__
        subscribe_methods(methods, batch, @__reaction__, @__stream__, '*') unless methods.empty?
      end

      def on_actions(reactions, *methods, batch: false)
        reactions.flat_map do |reaction|
          on_action(reaction, *methods, batch: batch)
        end
      end

      def on_action(reaction, *methods, batch: false)
        @__reaction__ = reaction
        subscribe_methods(methods, batch, @__reaction__, @__stream__) unless methods.empty?
      end

      def type(type, *methods, batch: false)
        subscribe_methods(methods, batch, @__reaction__, @__stream__, type) unless methods.empty?
      end
      
      private

        def subscribe_methods(methods, batch, reaction, stream, type = '*')
          methods.map do |method_sym|
            id = self.name + '::' + method_sym.to_s
            block =  ->(msg) {
              method = method(method_sym)
              msg = Reactor.indif_access_recursive(msg)
              (method.arity == 1) ? method.call(msg) : method.call
            }
            if batch
              reaction.subscribe_batch(id, stream, type, &block)
            else
              reaction.subscribe(id, stream, type, &block)
            end
          end
        end
    end
  end
end
