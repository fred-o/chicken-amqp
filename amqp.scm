(module amqp *

(import scheme
        (chicken base)
        srfi-18
        amqp-core
        amqp-primitives)

(define amqp-connection (make-parameter #f))
(define amqp-channel (make-parameter #f))

(define serializers (make-parameter '()))
(define deserializers (make-parameter '()))

(define (register-serializer content-type fn)
  (serializers (cons (cons content-type fn) (serializers))))

(define-syntax with-connection
  (syntax-rules ()
    [(with-connection uri body ...)
     (parameterize [(amqp-connection (amqp-connect uri))]
       (with-channel
        body ...)
       (thread-join! (car (connection-threads (amqp-connection)))))]))

(define-syntax with-channel
  (syntax-rules ()
    [(with-channel body ...)
     (begin
       (unless (amqp-connection) (error "no connection"))
       (parameterize
           [(amqp-channel (channel-open (amqp-connection)))]
         body ...))]))

(define (queue name #!key (bound-to #f) (bound-with #f))
  (let [(q (queue-declare (amqp-channel) name auto-delete: (if (equal? "" name) 1 0)))]
    (when bound-to
      (queue-bind (amqp-channel) q bound-to (or bound-with "#")))
    q))

(define (consume queue fn)
  (with-channel
   (basic-qos (amqp-channel) 0 1 0)
   (basic-consume (amqp-channel) queue)
   (thread-start!
    (lambda ()
      (letrec [(loop (lambda ()
                       (let [(m (receive-message (amqp-channel)))]
                         (fn (message-payload m) (message-properties m))
                         (basic-ack (amqp-channel) (alist-ref 'delivery-tag (message-delivery m)))
                         (loop))))]
        (loop))))))

)
