(module amqp *

(import scheme
        (chicken base)
        srfi-18
        bitstring
        amqp-core
        amqp-primitives)

(define amqp-connection (make-parameter #f))
(define amqp-channel (make-parameter #f))

(define serializers (make-parameter '()))
(define deserializers (make-parameter '()))

(define (register-serializer! content-type fn)
  (serializers (cons (cons content-type fn) (serializers))))
(define (register-deserializer! content-type fn)
  (deserializers (cons (cons content-type fn) (deserializers))))

(define-syntax with-connection
  (syntax-rules ()
    [(with-connection uri body ...)
     (parameterize [(amqp-connection (amqp-connect uri))
                    (serializers (serializers))
                    (deserializers (deserializers))]
       (with-channel
        body ...))]))

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

(define (consume queue fn #!key (detach #f))
  (with-channel
   (basic-qos (amqp-channel) 0 1 0)
   (basic-consume (amqp-channel) queue)
   (letrec [(loop (lambda ()
                    (let* [(msg (receive-message (amqp-channel)))
                           (payload (bitstring->string (message-payload msg)))
                           (properties (message-properties msg))
                           (content-type (alist-ref 'content-type properties))
                           (deserialize (alist-ref content-type (deserializers) equal?))]
                      (fn (if deserialize
                              (deserialize payload)
                              payload)
                          properties)
                      (basic-ack (amqp-channel) (alist-ref 'delivery-tag (message-delivery msg)))
                      (loop))))]
     (if detach
         (thread-start! loop)
         (loop)))))

)
