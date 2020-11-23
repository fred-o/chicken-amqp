(module amqp *

  ;;; ## Description
  
  ;;; A low-level [Chicken Scheme](https://call-cc.org) client for the
  ;;; Advanced Message Queueing Protocol v0.9.1.

  ;;; ### Limitations

  ;;; * Only AMQP 0.9.1 is supported, and 1.0 support is unlikely to
  ;;; be added.
  ;;; * Only PLAIN authentication is supported.
  ;;; * No SSL support.

  ;;; ## Authors

  ;;; Fredrik Appelberg (fredrik@appelberg.me)

  ;;; ## Repository

  ;;; https://github.com/fred-o/chicken-amqp

  ;;; ## Requirements
  
  ;;; * [[srfi-18]]
  ;;; * [[mailbox]]
  ;;; * [[bitstring]]
  ;;; * [[uri-generic]]

  ;;; ## Examples

  ;;; ### Producer

  ;;; ```Scheme
  ;;; ;; Establish a connection and open a channel
  ;;; (define conn (amqp-connect "amqp://myuser:mypassword@myserver/myvhost"))
  ;;; (define chan (amqp-channel-open conn))
  ;;;
  ;;; ;; Declare a durable topic exchange
  ;;; (amqp-exchange-declare chan "my-exchange" "topic" durable: 1)
  ;;;
  ;;; ;; Publish a message with a plain text payload
  ;;; (amqp-publish-message chan "my-exchange" "my-routing-key" "hello, world" '((content-type "text/plain")))
  ;;; ```
  
  ;;; ### Consumer

  ;;; ```Scheme
  ;;; ;; Establish a connection and open a channel
  ;;; (define conn (amqp-connect "amqp://myuser:mypassword@myserver/myvhost"))
  ;;; (define chan (amqp-channel-open conn))
  ;;;
  ;;; ;; Declare the previously created exchange passively. Not strictly
  ;;; ;; neccessary, but good practice.
  ;;; (amqp-exchange-declare chan "my-exchange" passive: 1)
  ;;;
  ;;; ;; Declare an anymous queue. The server will assign a random name, so
  ;;; ;; we need to check the return data to get it. 
  ;;; (let ((q (alist-ref 'queue (amqp-declare-queue chan "" auto-delete: 1))))
  ;;;   ;; Bind the queue to our exchange
  ;;;   (amqp-queue-bind chan q "my-exchange" "#")
  ;;; 
  ;;;   ;; Tell the server that we want to start consuming messages
  ;;;   (amqp-basic-consume chan q)
  ;;;
  ;;;   ;; Loop forever, recieving messages and printing the payload
  ;;;   (let loop ()
  ;;;     (let ((msg (amqp-receive-message chan))
  ;;;       (print (blob->string (amqp-message-payload msg)))
  ;;;       (loop)))))
  ;;; ```

  ;;; ## API

  ;;; ### Concurrency

  ;;; Each AMQP connection starts two SRFI-18 threads; one for reading
  ;;; and dispatching incoming frames, and one for handling heartbeats.

  ;;; All operations use SRFI-18 mutexes to ensure thread safety, and
  ;;; it should be perfectly fine to have multiple threads accessing
  ;;; the same channel. The `mailbox` egg is used internally when data
  ;;; needs to be passed between threads.
  
  ;;; ### Conditions

  ;;; AMQP handles errors by simply closing down the offending channel
  ;;; and/or connection. When this happens an *amqp* condition with
  ;;; `reply-text` and `reply-code` properties is raised, and the
  ;;; channel/connection object becomes invalid.
  
  ;;; ### Connections

  ;;; Create a new AMQP connection and returns a connection object.
  ;;; The `url` parameter should have the format `amqp://[user][:password@][host]/[vhost]`
  (define (amqp-connect url) ())

  ;;; Close an AMQP connection. 
  (define (amqp-disconnect connection) ())

  ;;; Set `amqp-debug` to `#t` to enable detailed debug logging of
  ;;; sent and received frames.
  (define amqp-debug (make-parameter #f))

  ;;; ### Receiving messages

  ;;; Block until the next `amqp-message` can be read on the given CHANNEL.
  ;;; 
  ;;; The client will receive messages from any of the following AMQP methods:
  ;;; * `basic.deliver` (after `basic.consume` on the same channel)
  ;;; * `basic.return` (for messages sent with `mandatory: 1` or `immediate: 1` that could not be
  ;;; routed or delivered immediately by the server)
  ;;; * `basic.get-ok` (in response to `basic.get`)
  (define (amqp-receive-message channel)
	(with-locked channel
				 (let* [(mthd (expect-frame channel 60 '(50 60 71) accessor: channel-content-mbox))
						(hdrs (read-frame channel accessor: channel-content-mbox))
						(body-size (frame-body-size hdrs))
						(buf (string->bitstring ""))]
				   (let loop []
					 (when (< (bitstring-length buf) body-size)
					   (set! buf (bitstring-append! buf
													(frame-payload (read-frame channel accessor: channel-content-mbox))))
					   (loop)))
				   (make-amqp-message
					(frame-properties mthd)
					(frame-properties hdrs)
					(if (amqp-payload-conversion)
						((amqp-payload-conversion) buf)
						buf)))))
  
  ;;; Record that holds received messages. The `delivery` slot holds
  ;;; an alist of delivery information from the server. The
  ;;; `properties` slot is an alist of AMQP message properties.
  (define-record amqp-message delivery properties payload)

  ;;; The `ampq` egg uses `bitstring` to handle binary data internally,
  ;;; but in an attempt to avoid abstraction leakage message payloads
  ;;; returned by `amqp-receive-message` are converted to standard
  ;;; `blob`s. If you don't want this behaviour, set the parameter
  ;;; `amqp-payload-conversion` to either #f or a function that
  ;;; accepts a `bitstring` and returns the desired payload format.
  (define amqp-payload-conversion (make-parameter bitstring->blob))

  ;;; ### Publishing messages

  ;;; Publish a message. The PAYLOAD can be an `u8vector`, `string`,
  ;;; `vector` or `blob`. PROPERTIES should be an alist of AMQP
  ;;; message properties.
  (define (amqp-publish-message channel exchange routing-key payload properties #!key (mandatory 0) (immediate 0))
	(let [(pl (->bitstring payload))]
	  (with-locked channel
				   (let [(frame-max (alist-ref 'frame-max (connection-parameters (channel-connection channel))))]
					 (write-frame channel 1 (make-basic-publish exchange routing-key mandatory immediate))
					 (write-frame channel 2 (encode-headers-payload 60 0 (/ (bitstring-length pl) 8) properties))
					 (write-frame channel 3 pl)))))

  ;;; ### AMQP Commands

  ;;; The AMQP command functions map 1:1 to the commands specified by
  ;;; the AMQP specification.

  ;;; * The first argument to any command function except
  ;;; `amqp-channel-open` should be a `channel` object. For other
  ;;; arguments, consult the AMQP specification.
  ;;; * All synchronous command functions except `amqp-channel-open` return an alist of values sent
  ;;; by the server in the reply command. Consult the AMQP specification for details.
  ;;; * Asynchronous command functions (`amqp-basic-ack`, `amqp-basic-reject`,
  ;;; etc.) or synchronous functions called with `no-wait: 1` return `(void)`.
  ;;; * `amqp-channel-open` returns a new `channel` object.

  ;;; #### The Channel Class

  ;;; Returns a new `channel` object that is then passed as an
  ;;; argument to the other functions.
  (define (amqp-channel-open connection)
	(let ((channel (new-channel connection)))
	  (write-frame channel 1 (make-channel-open))
	  (expect-frame channel 20 11)
	  channel))
  
  (define (amqp-channel-flow channel active) ())
  (define (amqp-channel-close
           channel
           #!key
           (reply-code 0)
           (reply-text "")
           (method-id 0)
           (class-id 0))
    ())

  ;;; #### The Exchange Class
  (define (amqp-exchange-declare
           channel
           exchange
           type
           #!key
           (passive 0)
           (durable 0)
           (no-wait 0)
           (arguments '()))
    ())
  (define (amqp-exchange-delete
           channel
           exchange
           #!key
           (if-unused 0)
           (no-wait 0))
    ())

  ;;; #### The Queue Class
  (define (amqp-queue-declare
           channel
           queue
           #!key
           (passive 0)
           (durable 0)
           (exclusive 0)
           (auto-delete 0)
           (no-wait 0)
           (arguments '()))
    ())
  (define (amqp-queue-bind
           channel
           queue
           exchange
           routing-key
           #!key
           (no-wait 0)
           (arguments '()))
    ())
  (define (amqp-queue-purge channel queue #!key (no-wait 0)) ())
  (define (amqp-queue-delete
           channel
           queue
           #!key
           (if-unused 0)
           (if-empty 0)
           (no-wait 0))
    ())
  (define (amqp-queue-unbind
           channel
           queue
           exchange
           routing-key
           #!key
           (arguments '()))
    ())

  ;;; #### The Basic Class
  (define (amqp-basic-qos channel prefetch-size prefetch-count global) ())
  (define (amqp-basic-consume
           channel
           queue
           #!key
           (consumer-tag "")
           (no-local 0)
           (no-ack 0)
           (exclusive 0)
           (no-wait 0)
           (arguments '()))
    ())
  (define (amqp-basic-cancel channel consumer-tag #!key (no-wait 0)) ())
  (define (amqp-basic-get channel queue #!key (no-ack 0)) ())
  (define (amqp-basic-ack channel delivery-tag #!key (multiple 0)) ())
  (define (amqp-basic-reject channel delivery-tag #!key (requeue 0)) ())
  (define (amqp-basic-recover channel requeue) ())
  (define (amqp-basic-recover-async channel requeue) ())

  ;;; #### The Transaction Class
  (define (amqp-tx-select channel) ())
  (define (amqp-tx-commit channel) ())
  (define (amqp-tx-rollback channel) ())

  ;;; ## License

  ;;; BSD

  ;;; ## Version History

  ;;; * 0.9 - Preparing for an 1.0 release
  )
