require 'sidekiq/middleware/chain'
require 'sidekiq/middleware/client/unique_jobs'

module Sidekiq
  class Client

    def self.default_middleware
      Middleware::Chain.new do |m|
      end
    end

    def self.registered_workers
      Sidekiq.redis { |x| x.smembers('workers') }
    end

    def self.registered_queues
      Sidekiq.redis { |x| x.smembers('queues') }
    end

    ##
    # The main method used to push a job to Redis.  Accepts a number of options:
    #
    #   queue - the named queue to use, default 'default'
    #   class - the worker class to call, required
    #   args - an array of simple arguments to the perform method, must be JSON-serializable
    #   retry - whether to retry this job if it fails, true or false, default true
    #   backtrace - whether to save any error backtrace, default false
    #
    # All options must be strings, not symbols.  NB: because we are serializing to JSON, all
    # symbols in 'args' will be converted to strings.
    #
    # Example:
    #   Sidekiq::Client.push('queue' => 'my_queue', 'class' => MyWorker, 'args' => ['foo', 1, :bat => 'bar'])
    #
    def self.push(item)   
      push_to_queue(item, false)
    end

    ##
    # Sibling of the push method, push_batch implies multiple messages should be delivered, in batch, to
    # the specified worker.  
    #   queue - the named queue to use, default 'default'
    #   class - the worker class to call, required
    #   args - an array of arrays of simple arguments to the perform method.  The arrays within the base array must be JSON serializable
    #   retry - whether to retry this job if it fails, true or false, default true
    #   backtrace - whether to save any error backtrace, default false
    #
    # All options must be strings, not symbols.  NB: because we are serializing to JSON, all
    # symbols in 'args' will be converted to strings.
    # Example:
    #   Sidekiq::Client.push_batch('queue' => 'my_queue', 'class' => MyWorker, 'args' => [['bar', 2, :bat => 'foo'], ['foo', 1, :bat => 'bar']])
    #
    def self.push_batch(item)
      #TODO: Actually support scheduled batches      
      raise(ArgumentError, "Batches cannot be scheduled at this time.") if item['at']
      self.push_to_queue(item, true)
    end

    # Redis compatibility helper.  Example usage:
    #
    #   Sidekiq::Client.enqueue(MyWorker, 'foo', 1, :bat => 'bar')
    #
    # Messages are enqueued to the 'default' queue.
    #
    def self.enqueue(klass, *args)
      klass.perform_async(*args)
    end

    private 
      def self.get_payload(item)
        payload = []
        args = item.delete('args')
        args.each do |arguments|
          payload << Sidekiq.dump_json(item.merge({'args' => arguments}))
        end
        payload
      end    

      #Push the message to redis
      def self.push_to_queue(item, batch)
        raise(ArgumentError, "Message must be a Hash of the form: { 'class' => SomeWorker, 'args' => ['bob', 1, :foo => 'bar'] }") unless item.is_a?(Hash)
        raise(ArgumentError, "Message must include a class and set of arguments: #{item.inspect}") if !item['class'] || !item['args']
        raise(ArgumentError, "Message must include a Sidekiq::Worker class, not class name: #{item['class'].ancestors.inspect}") if !item['class'].is_a?(Class) || !item['class'].respond_to?('get_sidekiq_options')
        worker_class = item['class']
        item['class'] = item['class'].to_s

        item = worker_class.get_sidekiq_options.merge(item)
        item['retry'] = !!item['retry']
        queue = item['queue']

        pushed = false
        payload = batch ? self.get_payload(item) : Sidekiq.dump_json(item)
        Sidekiq.client_middleware.invoke(worker_class, payload, queue) do        
          Sidekiq.sqs do |conn|
            if item['at']
              raise "We are not supporting scheduling at this time with SQS"
              #pushed = (conn.zadd('schedule', item['at'].to_s, payload) == 1)
            else
              queue = conn.queues.named('queue_test_one')
              unless batch
                result = queue.send_message(payload)
                pushed = !result.md5.nil?
              else
                results = []
                payload.each_slice(10) { |slice| results << queue.batch_send(slice)}
                #Just assume everything worked for now
                pushed = true
              end              
            end
          end
        end
        !! pushed
      end  
    def self.enqueue_batch(klass, *args)
      klass.perform_batch_async(*args)
    end
  end
end
