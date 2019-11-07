(module amqp-primitives *

(import scheme (chicken base) (chicken syntax)
        mailbox
        bitstring
        uuid-v4
        amqp-core
        amqp091)

;; AMQP primitives API

(define-record message delivery properties payload)

(define (receive-message channel)
  (let* [(mthd (expect-frame channel 60 60 mailbox: 'content))
         (hdrs (receive-frame channel mailbox: 'content))
         (body-size (frame-body-size hdrs))
         (buf (string->bitstring ""))]
    (let loop []
      (when (< (bitstring-length buf) body-size)
        (set! buf (bitstring-append! buf
                                     (frame-payload (receive-frame channel mailbox: 'content))))
        (loop)))
    (make-message (frame-properties mthd) (frame-properties hdrs) buf)))

(define (publish-message channel exchange routing-key payload properties #!key (mandatory 0) (immediate 0))
  (let [(frame-max (alist-ref 'frame-max (connection-parameters (channel-connection channel))))]
    (send-frame channel 1 (make-basic-publish exchange routing-key mandatory immediate))
    (send-frame channel 2 (encode-headers-payload 60 0 (/ (bitstring-length payload) 8) properties))
    (send-frame channel 3 payload)))

(define (channel-open conn)
  (let* [(id (next-channel-id conn))
         (ch (make-channel id
                           ;; method messages go to regular mailbox
                           (dispatch-register! conn
                                               (lambda (type channel-id class-id method-id)
                                                 (and (= channel-id id)
                                                      (= type 1)
                                                      (not (and (= class-id 60)
                                                                (= method-id 60))))))
                           ;; content-bearing messages go to the content mailbox
                           (dispatch-register! conn
                                               (lambda (type channel-id class-id method-id)
                                                 (and (= channel-id id)
                                                      (or (= type 2)
                                                          (= type 3)
                                                          (and (= class-id 60)
                                                               (= method-id 60))))))
                           conn))]
    (send-frame ch 1 (make-channel-open))
    (expect-frame ch 20 11)
    ch))

(define (exchange-declare channel exchange type #!key (passive 0) (durable 0) (no-wait 0))
  (send-frame channel 1 (make-exchange-declare exchange type passive durable no-wait '()))
  (expect-frame channel 40 11)
  exchange)

(define (exchange-delete channel exchange #!key (if-unused 0) (no-wait 0))
  (send-frame channel 1 (make-exchange-delete exchange if-unused no-wait))
  (expect-frame channel 40 21))

(define (queue-declare channel queue #!key (passive 0) (durable 0) (exclusive 0) (auto-delete 0) (no-wait 0))
  (send-frame channel 1 (make-queue-declare queue passive durable exclusive auto-delete no-wait '()))
  (let [(reply (expect-frame channel 50 11))]
    (alist-ref 'queue (frame-properties reply))))

(define (queue-bind channel queue exchange routing-key #!key (no-wait 0))
  (send-frame channel 1 (make-queue-bind queue exchange routing-key no-wait '()))
  (expect-frame channel 50 21))

(define (basic-qos channel prefetch-size prefetch-count global)
  (send-frame channel 1 (make-basic-qos prefetch-size prefetch-count global))
  (expect-frame channel 60 11))

(define (basic-consume channel queue  #!key (no-local 0) (no-ack 0) (exclusive 0) (no-wait 0))
  (let [(tag (make-uuid))]
    (send-frame channel 1 (make-basic-consume queue tag no-local no-ack exclusive no-wait '()))
    (expect-frame channel 60 21)))

(define (basic-ack channel delivery-tag #!key (multiple 0))
  (send-frame channel 1 (make-basic-ack delivery-tag multiple)))

(define (basic-reject channel delivery-tag #!key (requeue 0))
  (send-frame channel 1 (make-basic-reject delivery-tag requeue)))
)
