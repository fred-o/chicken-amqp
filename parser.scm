(define (make-connection-start-ok client-properties mechanism response locale)
  (let ((encoded-client-properties (encode-table client-properties)))
    (bitconstruct
      (10 16)
      (11 16)
      ((/ (bitstring-length encoded-client-properties) 8) 32)
      (encoded-client-properties bitstring)
      ((string-length mechanism) 8)
      (mechanism bitstring)
      ((string-length response) 32)
      (response bitstring)
      ((string-length locale) 8)
      (locale bitstring))))

(define (make-connection-secure-ok response)
  (bitconstruct
    (10 16)
    (21 16)
    ((string-length response) 32)
    (response bitstring)))

(define (make-connection-tune-ok channel-max frame-max heartbeat)
  (bitconstruct
    (10 16)
    (31 16)
    (channel-max 16)
    (frame-max 32)
    (heartbeat 16)))

(define (make-connection-open virtual-host)
  (bitconstruct
    (10 16)
    (40 16)
    ((string-length virtual-host) 8)
    (virtual-host bitstring)
    (0 8)
    (0 7)
    (0 1)))

(define (make-connection-close reply-code reply-text class-id method-id)
  (bitconstruct
    (10 16)
    (50 16)
    (reply-code 16)
    ((string-length reply-text) 8)
    (reply-text bitstring)
    (class-id 16)
    (method-id 16)))

(define (make-connection-close-ok) (bitconstruct (10 16) (51 16)))

(define (make-channel-open) (bitconstruct (20 16) (10 16) (0 8)))

(define (make-channel-flow active)
  (bitconstruct (20 16) (20 16) (0 7) (active 1)))

(define (make-channel-flow-ok active)
  (bitconstruct (20 16) (21 16) (0 7) (active 1)))

(define (make-channel-close reply-code reply-text class-id method-id)
  (bitconstruct
    (20 16)
    (40 16)
    (reply-code 16)
    ((string-length reply-text) 8)
    (reply-text bitstring)
    (class-id 16)
    (method-id 16)))

(define (make-channel-close-ok) (bitconstruct (20 16) (41 16)))

(define (make-exchange-declare exchange type passive durable no-wait arguments)
  (let ((encoded-arguments (encode-table arguments)))
    (bitconstruct
      (40 16)
      (10 16)
      (0 16)
      ((string-length exchange) 8)
      (exchange bitstring)
      ((string-length type) 8)
      (type bitstring)
      (0 3)
      (no-wait 1)
      (0 1)
      (0 1)
      (durable 1)
      (passive 1)
      ((/ (bitstring-length encoded-arguments) 8) 32)
      (encoded-arguments bitstring))))

(define (make-exchange-delete exchange if-unused no-wait)
  (bitconstruct
    (40 16)
    (20 16)
    (0 16)
    ((string-length exchange) 8)
    (exchange bitstring)
    (0 6)
    (no-wait 1)
    (if-unused 1)))

(define (make-queue-declare
         queue
         passive
         durable
         exclusive
         auto-delete
         no-wait
         arguments)
  (let ((encoded-arguments (encode-table arguments)))
    (bitconstruct
      (50 16)
      (10 16)
      (0 16)
      ((string-length queue) 8)
      (queue bitstring)
      (0 3)
      (no-wait 1)
      (auto-delete 1)
      (exclusive 1)
      (durable 1)
      (passive 1)
      ((/ (bitstring-length encoded-arguments) 8) 32)
      (encoded-arguments bitstring))))

(define (make-queue-bind queue exchange routing-key no-wait arguments)
  (let ((encoded-arguments (encode-table arguments)))
    (bitconstruct
      (50 16)
      (20 16)
      (0 16)
      ((string-length queue) 8)
      (queue bitstring)
      ((string-length exchange) 8)
      (exchange bitstring)
      ((string-length routing-key) 8)
      (routing-key bitstring)
      (0 7)
      (no-wait 1)
      ((/ (bitstring-length encoded-arguments) 8) 32)
      (encoded-arguments bitstring))))

(define (make-queue-unbind queue exchange routing-key arguments)
  (let ((encoded-arguments (encode-table arguments)))
    (bitconstruct
      (50 16)
      (50 16)
      (0 16)
      ((string-length queue) 8)
      (queue bitstring)
      ((string-length exchange) 8)
      (exchange bitstring)
      ((string-length routing-key) 8)
      (routing-key bitstring)
      ((/ (bitstring-length encoded-arguments) 8) 32)
      (encoded-arguments bitstring))))

(define (make-queue-purge queue no-wait)
  (bitconstruct
    (50 16)
    (30 16)
    (0 16)
    ((string-length queue) 8)
    (queue bitstring)
    (0 7)
    (no-wait 1)))

(define (make-queue-delete queue if-unused if-empty no-wait)
  (bitconstruct
    (50 16)
    (40 16)
    (0 16)
    ((string-length queue) 8)
    (queue bitstring)
    (0 5)
    (no-wait 1)
    (if-empty 1)
    (if-unused 1)))

(define (make-basic-qos prefetch-size prefetch-count global)
  (bitconstruct
    (60 16)
    (10 16)
    (prefetch-size 32)
    (prefetch-count 16)
    (0 7)
    (global 1)))

(define (make-basic-consume
         queue
         consumer-tag
         no-local
         no-ack
         exclusive
         no-wait
         arguments)
  (let ((encoded-arguments (encode-table arguments)))
    (bitconstruct
      (60 16)
      (20 16)
      (0 16)
      ((string-length queue) 8)
      (queue bitstring)
      ((string-length consumer-tag) 8)
      (consumer-tag bitstring)
      (0 4)
      (no-wait 1)
      (exclusive 1)
      (no-ack 1)
      (no-local 1)
      ((/ (bitstring-length encoded-arguments) 8) 32)
      (encoded-arguments bitstring))))

(define (make-basic-cancel consumer-tag no-wait)
  (bitconstruct
    (60 16)
    (30 16)
    ((string-length consumer-tag) 8)
    (consumer-tag bitstring)
    (0 7)
    (no-wait 1)))

(define (make-basic-publish exchange routing-key mandatory immediate)
  (bitconstruct
    (60 16)
    (40 16)
    (0 16)
    ((string-length exchange) 8)
    (exchange bitstring)
    ((string-length routing-key) 8)
    (routing-key bitstring)
    (0 6)
    (immediate 1)
    (mandatory 1)))

(define (make-basic-get queue no-ack)
  (bitconstruct
    (60 16)
    (70 16)
    (0 16)
    ((string-length queue) 8)
    (queue bitstring)
    (0 7)
    (no-ack 1)))

(define (make-basic-ack delivery-tag multiple)
  (bitconstruct (60 16) (80 16) (delivery-tag 64) (0 7) (multiple 1)))

(define (make-basic-reject delivery-tag requeue)
  (bitconstruct (60 16) (90 16) (delivery-tag 64) (0 7) (requeue 1)))

(define (make-basic-recover-async requeue)
  (bitconstruct (60 16) (100 16) (0 7) (requeue 1)))

(define (make-basic-recover requeue)
  (bitconstruct (60 16) (110 16) (0 7) (requeue 1)))

(define (make-tx-select) (bitconstruct (90 16) (10 16)))

(define (make-tx-commit) (bitconstruct (90 16) (20 16)))

(define (make-tx-rollback) (bitconstruct (90 16) (30 16)))

(define (parse-connection-start bits)
  (bitmatch
    bits
    (((version-major 8)
      (version-minor 8)
      (server-properties-size 32)
      (server-properties (* server-properties-size 8) bitstring)
      (mechanisms-size 32)
      (mechanisms (* mechanisms-size 8) bitstring)
      (locales-size 32)
      (locales (* locales-size 8) bitstring))
     (list 10
           10
           (list (cons 'version-major version-major)
                 (cons 'version-minor version-minor)
                 (cons 'server-properties (parse-table server-properties))
                 (cons 'mechanisms (bitstring->string mechanisms))
                 (cons 'locales (bitstring->string locales)))))))

(define (parse-connection-secure bits)
  (bitmatch
    bits
    (((challenge-size 32) (challenge (* challenge-size 8) bitstring))
     (list 10 20 (list (cons 'challenge (bitstring->string challenge)))))))

(define (parse-connection-tune bits)
  (bitmatch
    bits
    (((channel-max 16) (frame-max 32) (heartbeat 16))
     (list 10
           30
           (list (cons 'channel-max channel-max)
                 (cons 'frame-max frame-max)
                 (cons 'heartbeat heartbeat))))))

(define (parse-connection-open-ok bits)
  (bitmatch
    bits
    (((reserved-1-size 8) (reserved-1 (* reserved-1-size 8) bitstring))
     (list 10 41 (list (cons 'reserved-1 (bitstring->string reserved-1)))))))

(define (parse-connection-close bits)
  (bitmatch
    bits
    (((reply-code 16)
      (reply-text-size 8)
      (reply-text (* reply-text-size 8) bitstring)
      (class-id 16)
      (method-id 16))
     (list 10
           50
           (list (cons 'reply-code reply-code)
                 (cons 'reply-text (bitstring->string reply-text))
                 (cons 'class-id class-id)
                 (cons 'method-id method-id))))))

(define (parse-connection-close-ok bits) (list 10 51 '()))

(define (parse-channel-open-ok bits)
  (bitmatch
    bits
    (((reserved-1-size 32) (reserved-1 (* reserved-1-size 8) bitstring))
     (list 20 11 (list (cons 'reserved-1 (bitstring->string reserved-1)))))))

(define (parse-channel-flow bits)
  (bitmatch
    bits
    (((0 7) (active 1)) (list 20 20 (list (cons 'active active))))))

(define (parse-channel-flow-ok bits)
  (bitmatch
    bits
    (((0 7) (active 1)) (list 20 21 (list (cons 'active active))))))

(define (parse-channel-close bits)
  (bitmatch
    bits
    (((reply-code 16)
      (reply-text-size 8)
      (reply-text (* reply-text-size 8) bitstring)
      (class-id 16)
      (method-id 16))
     (list 20
           40
           (list (cons 'reply-code reply-code)
                 (cons 'reply-text (bitstring->string reply-text))
                 (cons 'class-id class-id)
                 (cons 'method-id method-id))))))

(define (parse-channel-close-ok bits) (list 20 41 '()))

(define (parse-exchange-declare-ok bits) (list 40 11 '()))

(define (parse-exchange-delete-ok bits) (list 40 21 '()))

(define (parse-queue-declare-ok bits)
  (bitmatch
    bits
    (((queue-size 8)
      (queue (* queue-size 8) bitstring)
      (message-count 32)
      (consumer-count 32))
     (list 50
           11
           (list (cons 'queue (bitstring->string queue))
                 (cons 'message-count message-count)
                 (cons 'consumer-count consumer-count))))))

(define (parse-queue-bind-ok bits) (list 50 21 '()))

(define (parse-queue-unbind-ok bits) (list 50 51 '()))

(define (parse-queue-purge-ok bits)
  (bitmatch
    bits
    (((message-count 32))
     (list 50 31 (list (cons 'message-count message-count))))))

(define (parse-queue-delete-ok bits)
  (bitmatch
    bits
    (((message-count 32))
     (list 50 41 (list (cons 'message-count message-count))))))

(define (parse-basic-qos-ok bits) (list 60 11 '()))

(define (parse-basic-consume-ok bits)
  (bitmatch
    bits
    (((consumer-tag-size 8) (consumer-tag (* consumer-tag-size 8) bitstring))
     (list 60
           21
           (list (cons 'consumer-tag (bitstring->string consumer-tag)))))))

(define (parse-basic-cancel-ok bits)
  (bitmatch
    bits
    (((consumer-tag-size 8) (consumer-tag (* consumer-tag-size 8) bitstring))
     (list 60
           31
           (list (cons 'consumer-tag (bitstring->string consumer-tag)))))))

(define (parse-basic-return bits)
  (bitmatch
    bits
    (((reply-code 16)
      (reply-text-size 8)
      (reply-text (* reply-text-size 8) bitstring)
      (exchange-size 8)
      (exchange (* exchange-size 8) bitstring)
      (routing-key-size 8)
      (routing-key (* routing-key-size 8) bitstring))
     (list 60
           50
           (list (cons 'reply-code reply-code)
                 (cons 'reply-text (bitstring->string reply-text))
                 (cons 'exchange (bitstring->string exchange))
                 (cons 'routing-key (bitstring->string routing-key)))))))

(define (parse-basic-deliver bits)
  (bitmatch
    bits
    (((consumer-tag-size 8)
      (consumer-tag (* consumer-tag-size 8) bitstring)
      (delivery-tag 64)
      (0 7)
      (redelivered 1)
      (exchange-size 8)
      (exchange (* exchange-size 8) bitstring)
      (routing-key-size 8)
      (routing-key (* routing-key-size 8) bitstring))
     (list 60
           60
           (list (cons 'consumer-tag (bitstring->string consumer-tag))
                 (cons 'delivery-tag delivery-tag)
                 (cons 'redelivered redelivered)
                 (cons 'exchange (bitstring->string exchange))
                 (cons 'routing-key (bitstring->string routing-key)))))))

(define (parse-basic-get-ok bits)
  (bitmatch
    bits
    (((delivery-tag 64)
      (0 7)
      (redelivered 1)
      (exchange-size 8)
      (exchange (* exchange-size 8) bitstring)
      (routing-key-size 8)
      (routing-key (* routing-key-size 8) bitstring)
      (message-count 32))
     (list 60
           71
           (list (cons 'delivery-tag delivery-tag)
                 (cons 'redelivered redelivered)
                 (cons 'exchange (bitstring->string exchange))
                 (cons 'routing-key (bitstring->string routing-key))
                 (cons 'message-count message-count))))))

(define (parse-basic-get-empty bits)
  (bitmatch
    bits
    (((reserved-1-size 8) (reserved-1 (* reserved-1-size 8) bitstring))
     (list 60 72 (list (cons 'reserved-1 (bitstring->string reserved-1)))))))

(define (parse-basic-recover-ok bits) (list 60 111 '()))

(define (parse-tx-select-ok bits) (list 90 11 '()))

(define (parse-tx-commit-ok bits) (list 90 21 '()))

(define (parse-tx-rollback-ok bits) (list 90 31 '()))

(define (parse-method-payload bits)
  (bitmatch
    bits
    (((class-id 16) (method-id 16) (arguments bitstring))
     (cond ((eq? 10 class-id)
            (cond ((eq? 10 method-id) (parse-connection-start arguments))
                  ((eq? 20 method-id) (parse-connection-secure arguments))
                  ((eq? 30 method-id) (parse-connection-tune arguments))
                  ((eq? 41 method-id) (parse-connection-open-ok arguments))
                  ((eq? 50 method-id) (parse-connection-close arguments))
                  ((eq? 51 method-id) (parse-connection-close-ok arguments))))
           ((eq? 20 class-id)
            (cond ((eq? 11 method-id) (parse-channel-open-ok arguments))
                  ((eq? 20 method-id) (parse-channel-flow arguments))
                  ((eq? 21 method-id) (parse-channel-flow-ok arguments))
                  ((eq? 40 method-id) (parse-channel-close arguments))
                  ((eq? 41 method-id) (parse-channel-close-ok arguments))))
           ((eq? 40 class-id)
            (cond ((eq? 11 method-id) (parse-exchange-declare-ok arguments))
                  ((eq? 21 method-id) (parse-exchange-delete-ok arguments))))
           ((eq? 50 class-id)
            (cond ((eq? 11 method-id) (parse-queue-declare-ok arguments))
                  ((eq? 21 method-id) (parse-queue-bind-ok arguments))
                  ((eq? 51 method-id) (parse-queue-unbind-ok arguments))
                  ((eq? 31 method-id) (parse-queue-purge-ok arguments))
                  ((eq? 41 method-id) (parse-queue-delete-ok arguments))))
           ((eq? 60 class-id)
            (cond ((eq? 11 method-id) (parse-basic-qos-ok arguments))
                  ((eq? 21 method-id) (parse-basic-consume-ok arguments))
                  ((eq? 31 method-id) (parse-basic-cancel-ok arguments))
                  ((eq? 50 method-id) (parse-basic-return arguments))
                  ((eq? 60 method-id) (parse-basic-deliver arguments))
                  ((eq? 71 method-id) (parse-basic-get-ok arguments))
                  ((eq? 72 method-id) (parse-basic-get-empty arguments))
                  ((eq? 111 method-id) (parse-basic-recover-ok arguments))))
           ((eq? 90 class-id)
            (cond ((eq? 11 method-id) (parse-tx-select-ok arguments))
                  ((eq? 21 method-id) (parse-tx-commit-ok arguments))
                  ((eq? 31 method-id) (parse-tx-rollback-ok arguments))))))))
