;; -*- geiser-scheme-implementation: 'chicken; -*-

(module amqp-primitives *

  (import scheme (chicken base) (chicken syntax)
          bitstring
          uuid-v4
          amqp-core
          amqp-091)

  ;; AMQP primitives API

  (define-record amqp-message delivery properties payload)

  (define (amqp-receive-message channel)
    (let* [(mthd (expect-frame channel 60 60))
           (hdrs (read-frame channel))
           (body-size (frame-body-size hdrs))
           (buf (string->bitstring ""))]
      (let loop []
        (when (< (bitstring-length buf) body-size)
          (set! buf (bitstring-append! buf
                                       (frame-payload (read-frame channel))))
          (loop)))
      (make-amqp-message (frame-properties mthd) (frame-properties hdrs) (bitstring->u8vector buf))))

  (define (amqp-publish-message channel exchange routing-key payload properties #!key (mandatory 0) (immediate 0))
    (let [(frame-max (alist-ref 'frame-max (connection-parameters connection)))]
      (write-frame channel 1 (make-basic-publish exchange routing-key mandatory immediate))
      (write-frame channel 2 (encode-headers-payload 60 0 (/ (bitstring-length payload) 8) properties))
      (write-frame channel 3 payload)))

  (define (amqp-channel-open connection)
	(let ((channel (new-channel connection)))
	  (write-frame channel 1 (make-channel-open))
	  (expect-frame channel 20 11)
	  channel))

  (define (amqp-channel-close channel reply-code reply-text)
	(write-frame channel 1 (make-channel-close reply-code reply-text 20 40))
	(expect-frame channel 20 41))
  
  (define (amqp-exchange-declare channel exchange type #!key (passive 0) (durable 0) (no-wait 0))
	(write-frame channel 1 (make-exchange-declare exchange type passive durable no-wait '()))
	(unless (= no-wait 1) (expect-frame channel 40 11))
	exchange)

  (define (amqp-exchange-delete channel exchange #!key (if-unused 0) (no-wait 0))
	(write-frame channel 1 (make-exchange-delete exchange if-unused no-wait))
	(unless (= no-wait 1) (expect-frame channel 40 21)))

  (define (amqp-queue-declare channel queue #!key (passive 0) (durable 0) (exclusive 0) (auto-delete 0) (no-wait 0))
	(write-frame channel 1 (make-queue-declare queue passive durable exclusive auto-delete no-wait '()))
	(unless (= no-wait 1)
	  (let [(reply (expect-frame channel 50 11))]
		(alist-ref 'queue (frame-properties reply)))))

  (define (amqp-queue-bind channel queue exchange routing-key #!key (no-wait 0))
	(write-frame channel 1 (make-queue-bind queue exchange routing-key no-wait '()))
	(unless (= no-wait 1) (expect-frame channel 50 21)))

  (define (amqp-basic-qos channel prefetch-size prefetch-count global)
	(write-frame channel 1 (make-basic-qos prefetch-size prefetch-count global))
	(expect-frame channel 60 11))

  (define (amqp-basic-consume channel queue  #!key (no-local 0) (no-ack 0) (exclusive 0) (no-wait 0))
	(let [(tag (make-uuid-v4))]
      (write-frame channel 1 (make-basic-consume queue tag no-local no-ack exclusive no-wait '()))
      (unless (= no-wait 1) (expect-frame channel 60 21))
	  tag))

  (define (amqp-basic-ack channel delivery-tag #!key (multiple 0))
	(write-frame channel 1 (make-basic-ack delivery-tag multiple)))

  (define (amqp-basic-reject channel delivery-tag #!key (requeue 0))
	(write-frame channel 1 (make-basic-reject delivery-tag requeue))))
