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

        # If there's a message already buffered, ask the manager to assign it
        # before fetching new jobs
        assigned = false
        if !@buffer.empty?
          assigned = true
          @mgr.assign!(@buffer.pop)
        end

        # Fetch messages. If none can be retrieved, recschedule another fetch unless a message was already
        # assigned to the processor.
        if fetch_required?  
          begin 
            received = false
            @queue.receive_message(:limit => BATCH) do |msg|
              received = true
              @buffer << msg
            end

            refetch if !received && !assigned
          rescue => ex
            logger.error("Error fetching message: #{ex}")
            logger.error(ex.backtrace.first)
            refetch if !assigned
          end
        end
      end
    end

    def refetch
      sleep(TIMEOUT)
      after(0) { fetch }
    end

    def fetch_required?
      @buffer.size < BATCH 
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
