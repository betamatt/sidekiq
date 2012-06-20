require 'sidekiq'
require 'celluloid'

module Sidekiq
  ##
  # The Fetcher blocks on Redis, waiting for a message to process
  # from the queues.  It gets the message and hands it to the Manager
  # to assign to a ready Processor.
  class Fetcher
    include Celluloid
    include Sidekiq::Util

    TIMEOUT = 5
    BATCH = 10

    def initialize(mgr, queues)
      @mgr = mgr
      @queues = queues.map { |q| "queue:#{q}" }
      @unique_queues = @queues.uniq

      # TODO: support multiple queues
      @queue = Sidekiq.sqs { |sqs| sqs.queues.named(queues.first) }

      @buffer = []
    end

    # Fetching is straightforward: the Manager makes a fetch
    # request for each idle processor when Sidekiq starts and
    # then issues a new fetch request every time a Processor
    # finishes a message.
    #
    # Because we have to shut down cleanly, we can't block
    # forever and we can't loop forever.  Instead we reschedule
    # a new fetch if the current fetch turned up nothing.
    def fetch
      watchdog('Fetcher#fetch died') do
        return if Sidekiq::Fetcher.done?

        if fetch_required?  
          begin 
            # Simulate a blocking receive by continually rescheduling until data is received
            received = false          
            @queue.receive_message(:limit => BATCH) do |msg|
              received = true
              @buffer << msg
            end
            
            if received
              @mgr.assign!(@buffer.pop)
            else
              # pause before fetching again
              sleep(TIMEOUT)
              after(0) { fetch }
            end
          rescue => ex
            logger.error("Error fetching message: #{ex}")
            logger.error(ex.backtrace.first)
            sleep(TIMEOUT)
            after(0) { fetch }
          end
        else
          @mgr.assign!(@buffer.pop)
        end
      end
    end

    def fetch_required?
      @buffer.size < 2 * BATCH 
    end

    # Ugh.  Say hello to a bloody hack.
    # Can't find a clean way to get the fetcher to just stop processing
    # its mailbox when shutdown starts.
    def self.done!
      @done = true
    end

    def self.done?
      @done
    end

  end
end
