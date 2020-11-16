(import test
		amqp
		(chicken blob)
		(chicken process-context))

(define c (amqp-connect (get-environment-variable "AMQP_URI")))
(define ch (amqp-channel-open c))

(test-group "amqp"
  (test "channel.flow" '((active . 1)) (amqp-channel-flow ch 1))
  
  (test "exchange.declare" '() (amqp-exchange-declare ch "test-exchange-1" "topic" durable: 1))

  (test "queue.declare" '((queue . "test-queue-1")
						  (message-count . 0)
						  (consumer-count . 0))
		(amqp-queue-declare ch "test-queue-1" auto-delete: 1 durable: 1))
  
  (test "queue.purge" '((message-count . 0)) (amqp-queue-purge ch "test-queue-1"))

  (test "queue.bind" (void) (amqp-queue-bind ch "test-queue-1" "test-exchange-1" "#" no-wait: 1))

  (test "basic.qos" '() (amqp-basic-qos ch 0 1 0))

  (test "publish" #t (amqp-publish-message ch "test-exchange-1" "ping"
											"hello, world"
											'((content-type . "text/plain"))))

    (test "queue.declare passive" '((queue . "test-queue-1")
									(message-count . 1)
									(consumer-count . 0))
		  (amqp-queue-declare ch "test-queue-1" passive: 1))

  
  (let ([ctag (alist-ref 'consumer-tag (amqp-basic-consume ch "test-queue-1" no-ack: 0))])
	(test-assert "basic.consume" (not (null? ctag)))

	(let ([msg (amqp-receive-message ch)])
	  (test "receive message-payload" "hello, world" (blob->string (amqp-message-payload msg)))
	  (test "receive message-properties" '((content-type . "text/plain")) (amqp-message-properties msg))
	  (test "basic.ack" #t (amqp-basic-ack ch (alist-ref 'delivery-tag (amqp-message-delivery msg)))))

	(amqp-publish-message ch "test-exchange-1" "ping"
						  "hello, world"
						  '((content-type . "text/plain")))
	(let ([msg (amqp-receive-message ch)])
	  (test "basic.reject" #t (amqp-basic-reject ch (alist-ref 'delivery-tag (amqp-message-delivery msg)))))
	
	(test "basic.cancel" (list (cons 'consumer-tag ctag)) (amqp-basic-cancel ch ctag)))

  (test "queue.delete" '((message-count . 0)) (amqp-queue-delete ch "test-queue-1" if-empty: 1))
  
  (test "exchange.delete" '() (amqp-exchange-delete ch "test-exchange-1"))
  (test "channel.close" '() (amqp-channel-close ch))

  (amqp-disconnect c))

(test-exit)

