
# Reactor

This gem works as a wrapper to envelope one or more streams of any type to work with them in a reactive way. This also abstracts the usage of this streams and unify the syntax so they can be used interchangeably.

## Installation

Add this line to your application's Gemfile:

```ruby  
gem 'reactor', '~> 1.0', path: '../../gems/reactor'
```  

And then execute:

    $ bundle install  

## Usage

For start, you should make some configurations. This configuration code should run once and setup everything for you at the beginning. In a rails application an initializer file is a good place for this.

### Configuration
This is a sample configuration. You can find runner of this on `lib/configuration.rb`;
```ruby
Reactor.configure do |config|
  config.logger = Logger.new(STDOUT, level: :info)
  config.run_consumers = ENV['CONSUMER_CLUSTER'].present?
  config.group_id = 'Project'
  config.error_handling_opts = {
    retry_count_on_fail: 3,
    max_add_end_of_queue_count: 3,
    error_class: Reactor::ConsumerErrorHandler,
    producer_error_class: Reactor::ProducerErrorHandler,
    on_max_retry_reached: Reactor::Streams::Kafka::RetryFailOptions::ADD_END_OF_QUEUE
  }

  config.kafka_api = {
    url: ENV['KAFKA_API_URL'],
    create_message_uri: 'create-messages',
    project: ENV['KAFKA_API_PROJECT'],
    code: ENV['KAFKA_API_CODE']
  }

  config.ruby_kafka = {
    seed_brokers: ENV['KAFKA_SEED_BROKERS'].split(',').map(&:strip),
    client_id: 'my_application',
  }
end
```
Here `kafka_api` is our producer api's configuration. `project` and `code` params are required to connect to api. These can be set as environment variables in your local go project with .env file.

And `ruby_kafka` is the configuration of the [ruby-kafka](https://github.com/zendesk/ruby-kafka) gem. You can use any options documented here: https://www.rubydoc.info/gems/ruby-kafka/Kafka%2FClient:initialize. These parameters will be used to initialize `ruby-kafka` client instance.

Now you can setup your streams on the same initializer file;
```ruby
KAFKA_STREAM = Reactor::Streams::Kafka

module Streams
  APP = Reactor::Stream.new
  A_MODEL = KAFKA_STREAM.new('AModel')
  ANOTHER_MODEL = KAFKA_STREAM.new('AnotherModel')
end
```
Here `Streams::APP` is a basic stream which works sync. When something is published to this stream with `Streams::APP.publish(...)` it will react instantly.

On the other hand `KAFKA_STREAM` is a big complex beast which works async. It requires separate threads to listen topics for consuming. So if `run_consumers` parameter is set to `true`, when the rails app starts, it will create the consumer threads and run them. If set to false then the stream will work as a producer only. You can set the number of consumers with `consumer_count` parameter documented here: `lib/reactor/streams/kafka.rb`. For example: If you run 2 consumer for one stream and run the app twice you will have 4 consumers. 2 of them will run concurrent in each app and this 2 sets run parallel. 

`group_id` is the consumer group name. In best practice this should be set different for each different app / project. This is a container place for consumers to be created in. In a Kafka topic if consumer count exceeds the partition count excess consumers will not listen any partition. And only one consumer can listen one partition in a group. Look Kafka docs for details.

And lastly every topic is one stream for Reactor. So if you have multiple topics designed to work with an app you can setup them as separate streams like this;
```ruby
A_MODEL = KAFKA_STREAM.new('AModel')
ANOTHER_MODEL = KAFKA_STREAM.new('AnotherModel')
```

Now we can setup our reactions;
```ruby
module Reaction  
  module AModel
    CREATED = Reactor::Reaction.new(:created, APP_STREAM, KAFKA_STREAM)  
    UPDATED = Reactor::Reaction.new(:updated, APP_STREAM, KAFKA_STREAM)  
    DELETED = Reactor::Reaction.new(:deleted, APP_STREAM, KAFKA_STREAM)  
  end  
end
```
In reactive programming paradigm streams can be merged, and the output will be a new stream. A Reaction is a special type of stream which merges our standart stream. A reaction is also have a label to filter itself when time comes to consume. Now when you publish to `Reaction::AModel::CREATED` it will automatically publish to `APP_STREAM` and `KAFKA_STREAM` under the hood. Also you can control which streams should be published to with `streams:` argument in the `publish` method of reaction.

Finally we can call our consumers to the scope. As you know we didn't write them yet but for the sake of completeness of the initializer file I want to add them here.
```ruby
AppStream::AModelConsumer  
KafkaStream::AModelConsumer
```

### Consumer Classes
Every consumer class should work with only one stream. We will set this with the `use` method on consumer class;
```ruby
# projects/planner/app/consumers/app_stream/attendees_consumer.rb
module AppStream  
  class AModelConsumer < Reactor::Consumer  
    use Streams::APP    
  
    on_action Reaction::AModel::UPDATED, :mutation_hook
  
    class << self
      def mutation_hook(msg)  
        puts '------ APP TRIGGER -------'  
        pp msg  
        puts '--------------------------'  
      end  
    end 
  end
end
```
For the Kafka consumer syntax is also same. We will just use `KAFKA_STREAM` instead of `APP_STREAM`. As you can see in consumer class every method works on class level, not in instance level. These classes will never be initialized somewhere by reactor. They will only be used as static classes. So don't try to keep instance or class level state in hook methods that you defined. If you need state in your logic, you should probably need to write a service or something else for this and call it from the hook.

`on_action` is one of the hooks you can use for subscriptions.

These are all with their definitions;
`on_everytime` - Every publish in the stream will hit this.
`on_action` - Every publish to a reaction in used stream context will hit this.
`on_stream_packet` - Every publish in the stream with a defined packet type will hit this.
`on_action_packet` - Every publish to a reaction in used stream with a defined packet type will hit this.

### Packets
Packets are user defined data types. For example we can define a data type `:created` and use a hook to only hit when the data type is this. For example;
```ruby
module KafkaConsumer
  class Another_Model < Reactor::Consumer
    use Streams::ANOTHER_MODEL do
      on_action_packet Reaction::ANOTHER_MODEL::CREATED do
        type :created, :create_a_model
      end

      def create_a_model(ticket)
        # write logic here
      end
    end 
  end
end
```
For publishing typed data you can use `packet:` argument.
```ruby
Streams::APP.publish('some message', packet: :created)
```
For best practice we can encapsulate packet types in module constants / enums in initializer.
```ruby
module Packet  
  CREATED = :created
end
```

### Batch subscription / publish
For every hook there is a `batch:` argument which is false by default. If you set this to true, the hook method will receive batch of messages instead of one at a time. In `APP_STREAM` you can get batch messages when they will be published as batch. For single publishes you will get only one message in an array in the hook. This is because the sync nature of `APP_STREAM`.
```ruby
on_action(Reaction::AModel::UPDATED, :mutation_hook, batch: true)
# /OR/
on_action_packet Reaction::AModel::UPDATED do  
  type(:updated_v1, :mutation_hook, batch: true)
end

class << self
  def mutation_hook(messages)
	messages.each do |msg|  
      puts '------ APP TRIGGER -------'  
      pp msg  
      puts '--------------------------'
    end  
  end  
end 
```
In Kafka side a batch subscriber couldn't be set because the offset management logic of Kafka is a bit complex and for now I couldn't find a way of doing this without increasing complexity and making some side effects. Kafka will get messages one by one even if it is published in batches.

On the other hand for the publish part everything works smoothly on every stream type. Just use `.publish_batch([array_of_messages])` instead of `.publish`. You can use `packet:` on `publish_batch` also.

### Kafka offset commit and error handling
There is a bunch of options when creating a Kafka stream. These are documented in `lib/reactor/streams/kafka.rb`. Here I will go into some details of error handling and offset commit.

If a hook method generates an exception when consuming Reactor will try to run the same hook method again `retry_count_on_fail` times (this argument is 0 by default). After every try it will wait for a time to retry again. This time is first set by `initial_wait` (default: 1 sec)  argument. For each try this wait time is modified by `wait_incrementer` argument. This is a proc which you can set your custom one. Default is `->(wait) { wait + (wait / 2) }` . After that if it still fails, `on_max_retry_reached` strategy will be applied. You can do 5 different things here;
- `DROP_CONSUMER`: Raise the error and shut down consumer gracefully. Other partitions' consumers will continue to work.
- `RUN_BLOCK`: Send the fetched message with metadata coming from `ruby-kafka` and the error being raised to a proc which you defined in `error_proc` argument. Ex: `->(fetched_msg, err) { ... }`
- `RUN_CLASS`: Same thing with `RUN_BLOCK` but you should set `error_class` argument and class should have an initializer with message argument and an on_error method in it. Ex:
```ruby
class MyErrorHandler  
 def initialize(fetched_msg)  
    @fetched_msg = fetched_msg  
  end  
  
 def on_error(error)  
    print [JSON.parse(@fetched_msg.value)['content'], error.message]  
  end
end
```
- `IGNORE`: Bypass the error and continue to work.
- `ADD_END_OF_QUEUE`: Ignores the error and continue to call other subscribers but in the meantime Reactor will generate a new message from the existing one with information in which subscription it raised an error and add this to the end of the queue. When trying to consume this new message again it will only trigger the errored hook method, not all subscribers. If it fails again it will add the message queue again with the same rules. But this should have a limit for message and you can set this limit by `max_add_end_of_queue_count` argument (Default is 5). This approach have only one drawback; if a new deploy released, consumers will be generated from beginning. So the messages added end of queue by this strategy will lose their pointers to hook method. In this case or the `max_add_end_of_queue_count` limit is reached Reactor will redirect the error `error_proc` or `error_class` if one of them defined. If none of these defined, it will drop the consumer by raising the error.


## Examples

### Simple usage with class:
```ruby
# create a stream  
APP_STREAM = Reactor::Stream.new  
  
# create a  simple consumer for this stream  
class ConsumerClass < Reactor::Consumer  
  use APP_STREAM do
    on_everytime :trigger
    
    def trigger
      puts "I'm triggered"
    end
  end
end  
  
# trigger the stream  
APP_STREAM.publish  
```  

### Trigger with message and message types
```ruby
# create a stream  
APP_STREAM = Reactor::Stream.new  
  
# create a  simple consumer for this stream  
class ConsumerClass < Reactor::Consumer  
  use APP_STREAM do
    on_everytime :show_msg # Hello World, A string, 2

    on_stream_packet do
      type :string, :show_msg # A string  
      type :int, :show_msg # 2  
    end
    
    def show_msg(msg)
      puts msg
    end
  end
end
# trigger the stream  
APP_STREAM.publish('Hello World')  
APP_STREAM.publish('A string', :string)  
APP_STREAM.publish(2, :int)
```

### Wrapping streams into reactions
```ruby
# create a stream  
APP_STREAM = Reactor::Stream.new  
  
# create a reaction  
MODEL_CREATED = Reactor::Reaction.new(:model_created, APP_STREAM)  
  
# create a  simple consumer for this reaction  
class ConsumerClass < Reactor::Consumer  
  use APP_STREAM do
    on_action MODEL_CREATED, :show_msg # Hello world
    
    def show_msg(msg)
      puts msg
    end
  end
end  
  
# trigger the stream  
MODEL_CREATED.publish('Hello World')
```  

### Full example for single/batch insert/update/upsert
```ruby
# using modules for better readibility  
module ReactiveStream  
  APP = Reactor::Stream.new
end  
  
# create reactions  
module Reaction  
  MODEL_CREATED = Reactor::Reaction.new(:model_created, ReactiveStream::APP) 
  MODEL_UPDATED = Reactor::Reaction.new(:model_updated, ReactiveStream::APP)
end  
  
# using modules for better readibility  
# packet types  
module Packet  
  INT = :int
end
  
# create consumer for this reaction  
class ConsumerClass < Reactor::Consumer  
  use APP_STREAM do
    on_action_packet(Reaction::MODEL_CREATED) do
      type Packet::INT, :model_created
    end

    on_action_packet(Reaction::MODEL_UPDATED) do
      type Packet::INT, :model_updated
    end

    def model_created(int)
      # do smt with int
    end
    
    def model_updated(int)
      # do smt with int
    end
  end
end

# trigger the action  
MODEL_CREATED.publish(12, Packet::INT)  
MODEL_CREATED.publish_batch([13, 14, 15], Packet::INT)  
MODEL_UPDATED.publish(12, Packet::INT)  
MODEL_UPDATED.publish_batch([13, 14, 15], Packet::INT)  
```  

### Also reactions supports multiple streams
```ruby
module ReactiveStream  
 APP = Reactor::Stream.new
 KAFKA = Reactor::Stream.new
end  
  
module Reaction  
 MODEL_CREATED = Reactor::Reaction.new(:model_created, ReactiveStream::APP, ReactiveStream::KAFKA)
end  
  
# create consumer for sync jobs  
class AppConsumer < Reactor::Consumer  
 use ReactiveStream::APP do
   # ...
 end
end  
  
# create consumer for async jobs  
class KafkaConsumer < Reactor::Consumer  
 use ReactiveStream::KAFKA do
   # ...
 end
end  
  
# trigger the action  
MODEL_CREATED.publish  
```

## Development

@ToDo

## Contributing

@ToDo

## License

@ToDo